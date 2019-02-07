/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search.facet;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.lucene.index.IndexReader.CacheKey;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.TermFacetCache;
import org.apache.solr.request.TermFacetCache.CacheUpdater;
import org.apache.solr.request.TermFacetCache.FacetCacheKey;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.facet.SlotAcc.SlotContext;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

/**
 * Facet processing based on field values. (not range nor by query)
 * @see FacetField
 */
abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc indexOrderAcc;
  int effectiveMincount;

  Map<String,AggValueSource> deferredAggs;  // null if none

  // TODO: push any of this down to base class?

  //
  // For sort="x desc", collectAcc would point to "x", and sortAcc would also point to "x".
  // collectAcc would be used to accumulate all buckets, and sortAcc would be used to sort those buckets.
  //
  SlotAcc collectAcc;  // Accumulator to collect across entire domain (in addition to the countAcc).  May be null.
  SlotAcc sortAcc;     // Accumulator to use for sorting *only* (i.e. not used for collection). May be an alias of countAcc, collectAcc, or indexOrderAcc
  SlotAcc[] otherAccs; // Accumulators that do not need to be calculated across all buckets.

  SpecialSlotAcc allBucketsAcc;  // this can internally refer to otherAccs and/or collectAcc. setNextReader should be called on otherAccs directly if they exist.

  FacetFieldProcessor(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq);
    this.sf = sf;
    this.effectiveMincount = (int)(fcontext.isShard() ? Math.min(1 , freq.mincount) : freq.mincount);
    final SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache = fcontext.searcher.getCache(TermFacetCache.NAME);
    if (facetCache == null) {
      cachingCountSlotAccFactory = null;
    } else {
      CacheKey topLevelKey = fcontext.searcher.getIndexReader().getReaderCacheHelper().getKey();
      cachingCountSlotAccFactory = new CachingCountSlotAccFactory(facetCache, topLevelKey, freq.field);
    }
  }

  private final IntObjectMap<List<ParallelCountAccStruct>> trackParallelCountAccs = new IntObjectScatterMap<>();
  private final Map<QueryResultKey, ParallelCountAccStruct> trackParallelCountAccsQRK = new HashMap<>();
  List<ParallelCountAccStruct> parallelCountAccs = new ArrayList<>();

  /**
   * Provides a hook for subclasses of FacetFieldProcessor to register custom implementations of CountSlotAcc.
   * Also allows caller of <code>getParallelCountAcc</code> control over whether to accept a particular cached
   * instance of CountSlotAcc.
   */
  protected static interface CountSlotAccFactory {
    ParallelCountAccStruct newInstance(QueryResultKey qKey, DocSet docs, boolean isBase, FacetContext fcontext, int numSlots);
  }

  protected static final CountSlotAccFactory DEFAULT_COUNT_ACC_FACTORY = new CountSlotAccFactory() {

    @Override
    public ParallelCountAccStruct newInstance(QueryResultKey qKey, DocSet docs, boolean isBase, FacetContext fcontext, int numSlots) {
      final CountSlotAcc count = new CountSlotArrAcc(fcontext, numSlots);
      return new ParallelCountAccStruct(qKey, docs, CacheState.DO_NOT_CACHE, null, isBase, count, new ReadOnlyCountSlotAccWrapper(fcontext, count), null);
    }
  };

  private final CountSlotAccFactory cachingCountSlotAccFactory;

  CountSlotAcc getParallelCountAcc(QueryResultKey qKey, DocSet docs, int numSlots) {
    return getParallelCountAcc(qKey, docs, numSlots, null);
  }

  CountSlotAcc getParallelCountAcc(QueryResultKey qKey, DocSet docs, int numSlots, CountSlotAccFactory factory) {
    return getParallelCountAcc(qKey, docs, false, numSlots, factory);
  }

  private CountSlotAcc getParallelCountAcc(QueryResultKey qKey, DocSet docs, boolean isBase, int numSlots, CountSlotAccFactory factory) {
    final int size = docs.size();
    final ParallelCountAccStruct qrkCandidate;
    if (qKey != null && (qrkCandidate = trackParallelCountAccsQRK.get(qKey)) != null) {
      return qrkCandidate.countAccEntry.roCountAcc;
    } 
    List<ParallelCountAccStruct> extantSameSize = trackParallelCountAccs.get(size);
    if (extantSameSize != null) {
      for (ParallelCountAccStruct candidate : extantSameSize) {
        if (docs.intersectionSize(candidate.docSet) == size) {
          if (qKey != null) {
            trackParallelCountAccsQRK.put(qKey, candidate);
          }
          return candidate.countAccEntry.roCountAcc;
        }
      }
    } else {
      extantSameSize = new ArrayList<>(4); // will likely be *very* small
      trackParallelCountAccs.put(size, extantSameSize);
    }
    if (factory == null) {
      if (cachingCountSlotAccFactory == null || size < (freq.countCacheDf == 0 ? TermFacetCache.DEFAULT_THRESHOLD : (freq.countCacheDf < 0 ? Integer.MAX_VALUE : freq.countCacheDf))) {
        factory = DEFAULT_COUNT_ACC_FACTORY;
      } else {
        factory = cachingCountSlotAccFactory;
      }
    }
    final ParallelCountAccStruct ret = factory.newInstance(qKey, docs, isBase, fcontext, numSlots);
    extantSameSize.add(ret);
    parallelCountAccs.add(ret);
    if (qKey != null) {
      trackParallelCountAccsQRK.put(qKey, ret);
    }
    return isBase ? ret.countAccEntry.countAcc : ret.countAccEntry.roCountAcc;
  }
  static enum CacheState { DO_NOT_CACHE, NOT_CACHED, PARTIALLY_CACHED, CACHED }

  static final class ParallelCountAccStruct {
    final QueryResultKey qKey;
    final DocSet docSet;
    final CacheState cacheState;
    final Map<CacheKey, SegmentCacheEntry> alreadyCached;
    final boolean isBase;
    final CountAccEntry countAccEntry;
    final CacheUpdater cacheUpdater;

    public ParallelCountAccStruct(QueryResultKey qKey, DocSet docSet, CacheState cacheState,
        Map<CacheKey, SegmentCacheEntry> alreadyCached, boolean isBase, CountSlotAcc countAcc,
        ReadOnlyCountSlotAcc roCountAcc, CacheUpdater cacheUpdater) {
      this.qKey = qKey;
      this.docSet = docSet;
      this.cacheState = cacheState;
      this.alreadyCached = alreadyCached;
      this.isBase = isBase;
      this.countAccEntry = new CountAccEntry(countAcc, roCountAcc);
      this.cacheUpdater = cacheUpdater;
    }
  }
  static final class CountAccEntry {
    final CountSlotAcc countAcc;
    final ReadOnlyCountSlotAcc roCountAcc;
    public CountAccEntry(CountSlotAcc countAcc, ReadOnlyCountSlotAcc roCountAcc) {
      this.countAcc = countAcc;
      this.roCountAcc = roCountAcc;
    }
  }
  static final class FilterCtStruct {
    final boolean isBase;
    final Filter filter;
    final CountSlotAcc countAcc;
    final CacheUpdater cacheUpdater;

    public FilterCtStruct(Filter filter, CountSlotAcc countAcc, CacheUpdater cacheUpdater, boolean isBase) {
      this.isBase = isBase;
      this.filter = filter;
      this.countAcc = countAcc;
      this.cacheUpdater = cacheUpdater;
    }
  }
  protected FilterCtStruct[] getParallelFilters(boolean maySkipBaseSetCollection) {
    final FilterCtStruct[] filters = new FilterCtStruct[parallelCountAccs.size()];
    int i = 0;
    ParallelCountAccStruct base = null;
    for (ParallelCountAccStruct parallel : parallelCountAccs) {
      if (parallel.isBase) {
        base = parallel;
      } else if (parallel.cacheState != CacheState.CACHED) {
        filters[i++] = new FilterCtStruct(parallel.docSet.getTopFilter(), parallel.countAccEntry.countAcc,
            parallel.cacheUpdater, parallel.isBase);
      }
    }
    if (base.cacheState != CacheState.CACHED || !maySkipBaseSetCollection) {
      filters[i++] = new FilterCtStruct(base.docSet.getTopFilter(), base.countAccEntry.countAcc,
          base.cacheUpdater, base.isBase);
    }
    if (i == 0) {
      return null;
    } else if (i == filters.length) {
      return filters;
    } else {
      return Arrays.copyOf(filters, i);
    }
  }
  protected ParallelCountAccStruct[] getParallelDocSets(boolean maySkipBaseSetCollection) {
    //TODO check facet cache
    final ParallelCountAccStruct[] ret = new ParallelCountAccStruct[parallelCountAccs.size()];
    int i = 0;
    ParallelCountAccStruct base = null;
    for (ParallelCountAccStruct parallel : parallelCountAccs) {
      if (parallel.isBase) {
        base = parallel;
      } else {
        ret[i++] = parallel;
      }
    }
    ret[i] = base;
    return ret;
  }

  private CountSlotAcc createBaseCountAcc(int slotCount) {
    QueryResultKey baseQKey = fcontext.baseFilters == null ? null : new QueryResultKey(null, Arrays.asList(fcontext.baseFilters), null, 0);
    return getParallelCountAcc(baseQKey, fcontext.base, true, slotCount, null);
  }

  /**
   * A marker interface for SlotAcc instances that are populated as a derivative of the main CountSlotAcc, and thus do
   * not need to be separately/independently collected.
   */
  interface ParallelAcc {}

  /**
   * This is used to create accs for second phase (or to create accs for all aggs) */
  @Override
  protected void createAccs(int docCount, int slotCount) throws IOException {
    if (accMap == null) {
      accMap = new LinkedHashMap<>();
    }

    // allow a custom count acc to be used
    if (countAcc == null) {
      countAcc = createBaseCountAcc(slotCount);
      countAcc.key = "count";
    }

    if (accs != null) {
      // reuse these accs, but reset them first and resize since size could be different
      for (SlotAcc acc : accs) {
        acc.reset();
        acc.resize(new SlotAcc.Resizer() {
          @Override
          public int getNewSize() {
            return slotCount;
          }

          @Override
          public int getNewSlot(int oldSlot) {
            return 0;
          }
        });
      }
      return;
    } else {
      accs = new SlotAcc[ freq.getFacetStats().size() ];
    }

    int accIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = null;
      if (slotCount == 1) {
        acc = accMap.get(entry.getKey());
        if (acc != null) {
          acc.reset();
        }
      }
      if (acc == null) {
        acc = entry.getValue().createSlotAcc(fcontext, docCount, slotCount);
        acc.key = entry.getKey();
        accMap.put(acc.key, acc);
      }
      accs[accIdx++] = acc;
    }
  }

  void createCollectAcc(int numDocs, int numSlots) throws IOException {
    accMap = new LinkedHashMap<>();

    // we always count...
    // allow a subclass to set a custom counter.
    if (countAcc == null) {
      countAcc = createBaseCountAcc(numSlots);
    }

    if ("count".equals(freq.sortVariable)) {
      sortAcc = countAcc;
      deferredAggs = freq.getFacetStats();
    } else if ("index".equals(freq.sortVariable)) {
      // allow subclass to set indexOrderAcc first
      if (indexOrderAcc == null) {
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
        indexOrderAcc = new SortSlotAcc(fcontext);
      }
      sortAcc = indexOrderAcc;
      deferredAggs = freq.getFacetStats();
    }

    // If we are going to return all buckets and if there are no subfacets (that would need a domain), then don't defer
    // any aggregation calculations to a second phase.  This way we can avoid calculating domains for each bucket, which
    // can be expensive.
    if (freq.limit == -1 && freq.subFacets.size() == 0) {
      accs = new SlotAcc[ freq.getFacetStats().size() ];
      int otherAccIdx = 0;
      for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
        AggValueSource agg = entry.getValue();
        SlotAcc acc = agg.createSlotAcc(fcontext, numDocs, numSlots);
        acc.key = entry.getKey();
        accMap.put(acc.key, acc);
        accs[otherAccIdx++] = acc;
      }
      if (accs.length == 1) {
        collectAcc = accs[0];
      } else {
        collectAcc = new MultiAcc(fcontext, accs);
      }

      if (sortAcc == null) {
        sortAcc = accMap.get(freq.sortVariable);
        assert sortAcc != null;
      }

      deferredAggs = null;
    }

    if (sortAcc == null) {
      AggValueSource sortAgg = freq.getFacetStats().get(freq.sortVariable);
      if (sortAgg != null) {
        collectAcc = sortAgg.createSlotAcc(fcontext, numDocs, numSlots);
        collectAcc.key = freq.sortVariable; // TODO: improve this
      }
      sortAcc = collectAcc;
      deferredAggs = new HashMap<>(freq.getFacetStats());
      deferredAggs.remove(freq.sortVariable);
    }

    if (deferredAggs == null || deferredAggs.size() == 0) {
      deferredAggs = null;
    }

    boolean needOtherAccs = freq.allBuckets;  // TODO: use for missing too...

    if (!needOtherAccs) {
      // we may need them later, but we don't want to create them now
      // otherwise we won't know if we need to call setNextReader on them.
      return;
    }

    // create the deferred aggs up front for use by allBuckets
    createOtherAccs(numDocs, 1);
  }

  private void createOtherAccs(int numDocs, int numSlots) throws IOException {
    if (otherAccs != null) {
      // reuse existing accumulators
      for (SlotAcc acc : otherAccs) {
        acc.reset();  // todo - make reset take numDocs and numSlots?
      }
      return;
    }

    int numDeferred = deferredAggs == null ? 0 : deferredAggs.size();
    if (numDeferred <= 0) return;

    otherAccs = new SlotAcc[ numDeferred ];

    int otherAccIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : deferredAggs.entrySet()) {
      AggValueSource agg = entry.getValue();
      SlotAcc acc = agg.createSlotAcc(fcontext, numDocs, numSlots);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);
      otherAccs[otherAccIdx++] = acc;
    }

    if (numDeferred == freq.getFacetStats().size()) {
      // accs and otherAccs are the same...
      accs = otherAccs;
    }
  }

  int collectFirstPhase(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    int num = -1;
    if (collectAcc != null) {
      num = collectAcc.collect(docs, slot, slotContext);
    }
    if (allBucketsAcc != null) {
      num = allBucketsAcc.collect(docs, slot, slotContext);
    }
    return num >= 0 ? num : docs.size();
  }

  void collectFirstPhase(int segDoc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    if (collectAcc != null) {
      collectAcc.collect(segDoc, slot, slotContext);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.collect(segDoc, slot, slotContext);
    }
  }

  /** Processes the collected data to finds the top slots, and composes it in the response NamedList. */
  SimpleOrderedMap<Object> findTopSlots(final int numSlots, final int slotCardinality, final int missingSlot,
                                        IntFunction<Comparable> bucketValFromSlotNumFunc,
                                        Function<Comparable, String> fieldQueryValFunc) throws IOException {
    int numBuckets = 0;

    final int off = fcontext.isShard() ? 0 : (int) freq.offset;

    long effectiveLimit = Integer.MAX_VALUE; // use max-int instead of max-long to avoid overflow
    if (freq.limit >= 0) {
      effectiveLimit = freq.limit;
      if (fcontext.isShard()) {
        if (freq.overrequest == -1) {
          // add over-request if this is a shard request and if we have a small offset (large offsets will already be gathering many more buckets than needed)
          if (freq.offset < 10) {
            effectiveLimit = (long) (effectiveLimit * 1.1 + 4); // default: add 10% plus 4 (to overrequest for very small limits)
          }
        } else {
          effectiveLimit += freq.overrequest;
        }
      }
    }


    final int sortMul = freq.sortDirection.getMultiplier();

    int maxTopVals = (int) (effectiveLimit >= 0 ? Math.min(freq.offset + effectiveLimit, Integer.MAX_VALUE - 1) : Integer.MAX_VALUE - 1);
    maxTopVals = Math.min(maxTopVals, slotCardinality);
    final SlotAcc sortAcc = this.sortAcc, indexOrderAcc = this.indexOrderAcc;
    final BiPredicate<Slot,Slot> orderPredicate;
    if (indexOrderAcc != null && indexOrderAcc != sortAcc) {
      orderPredicate = (a, b) -> {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? (indexOrderAcc.compare(a.slot, b.slot) > 0) : cmp < 0;
      };
    } else {
      orderPredicate = (a, b) -> {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      };
    }
    final PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxTopVals) {
      @Override
      protected boolean lessThan(Slot a, Slot b) { return orderPredicate.test(a, b); }
    };

    // note: We avoid object allocation by having a Slot and re-using the 'bottom'.
    Slot bottom = null;
    Slot scratchSlot = new Slot();
    boolean shardHasMoreBuckets = false;  // This shard has more buckets than were returned
    for (int slotNum = 0; slotNum < numSlots; slotNum++) {

      // screen out buckets not matching mincount
      if (effectiveMincount > 0) {
        int count = countAcc.getCount(slotNum);
        if (count  < effectiveMincount) {
          if (count > 0)
            numBuckets++;  // Still increment numBuckets as long as we have some count.  This is for consistency between distrib and non-distrib mode.
          continue;
        }
      }

      numBuckets++;

      if (bottom != null) {
        shardHasMoreBuckets = true;
        scratchSlot.slot = slotNum; // scratchSlot is only used to hold this slotNum for the following line
        if (orderPredicate.test(bottom, scratchSlot)) {
          bottom.slot = slotNum;
          bottom = queue.updateTop();
        }
      } else if (effectiveLimit > 0) {
        // queue not full
        Slot s = new Slot();
        s.slot = slotNum;
        queue.add(s);
        if (queue.size() >= maxTopVals) {
          bottom = queue.top();
        }
      }
    }

    assert queue.size() <= numBuckets;

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    if (freq.numBuckets) {
      if (!fcontext.isShard()) {
        res.add("numBuckets", numBuckets);
      } else {
        calculateNumBuckets(res);
      }
    }

    FacetDebugInfo fdebug = fcontext.getDebugInfo();
    if (fdebug != null) fdebug.putInfoItem("numBuckets", (long) numBuckets);

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      // countAcc.setValues(allBuckets, allBucketsSlot);
      allBuckets.add("count", allBucketsAcc.getSpecialCount());
      allBucketsAcc.setValues(allBuckets, -1); // -1 slotNum is unused for SpecialSlotAcc
      // allBuckets currently doesn't execute sub-facets (because it doesn't change the domain?)
      res.add("allBuckets", allBuckets);
    }

    SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
    if (freq.missing) {
      res.add("missing", missingBucket);
      // moved missing fillBucket after we fill facet since it will reset all the accumulators.
    }

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= maxTopVals;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    ArrayList<SimpleOrderedMap> bucketList = new ArrayList<>(collectCount);
    res.add("buckets", bucketList);

    boolean needFilter = deferredAggs != null || freq.getSubFacets().size() > 0;

    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
      Comparable val = bucketValFromSlotNumFunc.apply(slotNum);
      bucket.add("val", val);

      Query filter = needFilter ? makeBucketQuery(fieldQueryValFunc.apply(val)) : null;

      fillBucket(bucket, countAcc.getCount(slotNum), slotNum, null, filter);

      bucketList.add(bucket);
    }

    if (fcontext.isShard() && shardHasMoreBuckets) {
      // Currently, "more" is an internal implementation detail and only returned for distributed sub-requests
      res.add("more", true);
    }

    if (freq.missing) {
      // TODO: it would be more efficient to build up a missing DocSet if we need it here anyway.
      if (missingSlot < 0) {
        fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null, false, null);
      } else {
        Query filter = needFilter ? getFieldMissingQuery(fcontext.searcher, freq.field) : null;
        fillBucket(missingBucket, countAcc.getCount(missingSlot), missingSlot, null, filter);
      }
    }

    return res;
  }

  /**
   * Trivial helper method for building up a bucket query given the (Stringified) bucket value
   */
  protected Query makeBucketQuery(final String bucketValue) {
    // TODO: this isn't viable for things like text fields w/ analyzers that are non-idempotent (ie: stemmers)
    // TODO: but changing it to just use TermQuery isn't safe for things like numerics, dates, etc...
    return sf.getType().getFieldQuery(null, sf, bucketValue);
  }

  private void calculateNumBuckets(SimpleOrderedMap<Object> target) throws IOException {
    DocSet domain = fcontext.base;
    if (freq.prefix != null) {
      Query prefixFilter = sf.getType().getPrefixQuery(null, sf, freq.prefix);
      domain = fcontext.searcher.getDocSet(prefixFilter, domain);
    }

    HLLAgg agg = new HLLAgg(freq.field);
    SlotAcc acc = agg.createSlotAcc(fcontext, domain.size(), 1);
    acc.collect(domain, 0, null); // we know HLL doesn't care about the bucket query
    acc.key = "numBuckets";
    acc.setValues(target, 0);
  }

  private static class Slot {
    int slot;
  }

  private void fillBucket(SimpleOrderedMap<Object> target, int count, int slotNum, DocSet subDomain, Query filter) throws IOException {
    target.add("count", count);
    if (count <= 0 && !freq.processEmpty) return;

    if (collectAcc != null && slotNum >= 0) {
      collectAcc.setValues(target, slotNum);
    }

    createOtherAccs(-1, 1);

    if (otherAccs == null && freq.subFacets.isEmpty()) return;

    if (subDomain == null) {
      subDomain = fcontext.searcher.getDocSet(filter, fcontext.base);
    }

    // if no subFacets, we only need a DocSet
    // otherwise we need more?
    // TODO: save something generic like "slotNum" in the context and use that to implement things like filter exclusion if necessary?
    // Hmmm, but we need to look up some stuff anyway (for the label?)
    // have a method like "DocSet applyConstraint(facet context, DocSet parent)"
    // that's needed for domain changing things like joins anyway???

    if (otherAccs != null) {
      // do acc at a time (traversing domain each time) or do all accs for each doc?
      for (SlotAcc acc : otherAccs) {
        acc.reset(); // TODO: only needed if we previously used for allBuckets or missing
        acc.collect(subDomain, 0, slot -> { return new SlotContext(filter); });
        acc.setValues(target, 0);
      }
    }

    processSubs(target, filter, subDomain, false, null);
  }

  @Override
  protected void processStats(SimpleOrderedMap<Object> bucket, Query bucketQ, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    assert null != bucketQ;
    int collected = collect(docs, 0, slotNum -> { return new SlotContext(bucketQ); });

    // countAcc.incrementCount(0, collected);  // should we set the counton the acc instead of just passing it?

    assert collected == docCount;
    addStats(bucket, collected, 0);
  }

  // overrides but with different signature!
  private void addStats(SimpleOrderedMap<Object> target, int count, int slotNum) throws IOException {
    target.add("count", count);
    if (count > 0 || freq.processEmpty) {
      for (SlotAcc acc : accs) {
        acc.setValues(target, slotNum);
      }
    }
  }

  @Override
  void setNextReader(LeafReaderContext ctx) throws IOException {
    // base class calls this (for missing bucket...) ...  go over accs[] in that case
    super.setNextReader(ctx);
  }

  void setNextReaderFirstPhase(LeafReaderContext ctx) throws IOException {
    if (collectAcc != null) {
      collectAcc.setNextReader(ctx);
    }
    if (otherAccs != null) {
      for (SlotAcc acc : otherAccs) {
        acc.setNextReader(ctx);
      }
    }
  }

  static class MultiAcc extends SlotAcc {
    final SlotAcc[] subAccs;

    MultiAcc(FacetContext fcontext, SlotAcc[] subAccs) {
      super(fcontext);
      this.subAccs = subAccs;
    }

    @Override
    public void setNextReader(LeafReaderContext ctx) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.setNextReader(ctx);
      }
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.collect(doc, slot, slotContext);
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.reset();
      }
    }

    @Override
    public void resize(Resizer resizer) {
      for (SlotAcc acc : subAccs) {
        acc.resize(resizer);
      }
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.setValues(bucket, slotNum);
      }
    }
  }


  static class SpecialSlotAcc extends SlotAcc {
    SlotAcc collectAcc;
    SlotAcc[] otherAccs;
    int collectAccSlot;
    int otherAccsSlot;
    long count;

    SpecialSlotAcc(FacetContext fcontext, SlotAcc collectAcc, int collectAccSlot, SlotAcc[] otherAccs, int otherAccsSlot) {
      super(fcontext);
      this.collectAcc = collectAcc;
      this.collectAccSlot = collectAccSlot;
      this.otherAccs = otherAccs;
      this.otherAccsSlot = otherAccsSlot;
    }

    public int getCollectAccSlot() { return collectAccSlot; }
    public int getOtherAccSlot() { return otherAccsSlot; }

    long getSpecialCount() {
      return count;
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      assert slot != collectAccSlot || slot < 0;
      count++;
      if (collectAcc != null) {
        collectAcc.collect(doc, collectAccSlot, slotContext);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.collect(doc, otherAccsSlot, slotContext);
        }
      }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      // collectAcc and otherAccs will normally have setNextReader called directly on them.
      // This, however, will be used when collect(DocSet,slot) variant is used on this Acc.
      if (collectAcc != null) {
        collectAcc.setNextReader(readerContext);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.setNextReader(readerContext);
        }
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      if (collectAcc != null) {
        collectAcc.setValues(bucket, collectAccSlot);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.setValues(bucket, otherAccsSlot);
        }
      }
    }

    @Override
    public void reset() {
      // reset should be called on underlying accs
      // TODO: but in case something does need to be done here, should we require this method to be called but do nothing for now?
      throw new UnsupportedOperationException();
    }

    @Override
    public void resize(Resizer resizer) {
      // someone else will call resize on collectAcc directly
      if (collectAccSlot >= 0) {
        collectAccSlot = resizer.getNewSlot(collectAccSlot);
      }
    }
  }


  /*
   "qfacet":{"cat2":{"_l":["A"]}},
   "all":{"_s":[[
     "all",
     {"cat3":{"_l":["A"]}}]]},
   "cat1":{"_l":["A"]}}}
   */

  static <T> List<T> asList(Object list) {
    return list != null ? (List<T>)list : Collections.EMPTY_LIST;
  }

  protected SimpleOrderedMap<Object> refineFacets() throws IOException {
    boolean skipThisFacet = (fcontext.flags & SKIP_FACET) != 0;


    List leaves = asList(fcontext.facetInfo.get("_l"));        // We have not seen this bucket: do full faceting for this bucket, including all sub-facets
    List<List> skip = asList(fcontext.facetInfo.get("_s"));    // We have seen this bucket, so skip stats on it, and skip sub-facets except for the specified sub-facets that should calculate specified buckets.
    List<List> partial = asList(fcontext.facetInfo.get("_p")); // We have not seen this bucket, do full faceting for this bucket, and most sub-facets... but some sub-facets are partial and should only visit specified buckets.

    // For leaf refinements, we do full faceting for each leaf bucket.  Any sub-facets of these buckets will be fully evaluated.  Because of this, we should never
    // encounter leaf refinements that have sub-facets that return partial results.

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> bucketList = new ArrayList<>( leaves.size() + skip.size() + partial.size() );
    res.add("buckets", bucketList);

    // TODO: an alternate implementations can fill all accs at once
    createAccs(-1, 1);

    for (Object bucketVal : leaves) {
      bucketList.add( refineBucket(bucketVal, false, null) );
    }

    for (List bucketAndFacetInfo : skip) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, true, facetInfo ) );
    }

    // The only difference between skip and missing is the value of "skip" passed to refineBucket
    for (List bucketAndFacetInfo : partial) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, false, facetInfo ) );
    }

    if (freq.missing) {
      Map<String,Object> bucketFacetInfo = (Map<String,Object>)fcontext.facetInfo.get("missing");

      if (bucketFacetInfo != null || !skipThisFacet) {
        SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
        fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null, skipThisFacet, bucketFacetInfo);
        res.add("missing", missingBucket);
      }
    }

    if (freq.numBuckets && !skipThisFacet) {
      calculateNumBuckets(res);
    }

    // If there are just a couple of leaves, and if the domain is large, then
    // going by term is likely the most efficient?
    // If the domain is small, or if the number of leaves is large, then doing
    // the normal collection method may be best.

    return res;
  }

  private SimpleOrderedMap<Object> refineBucket(Object bucketVal, boolean skip, Map<String,Object> facetInfo) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
    FieldType ft = sf.getType();
    bucketVal = ft.toNativeType(bucketVal);  // refinement info passed in as JSON will cause int->long and float->double
    bucket.add("val", bucketVal);

    // fieldQuery currently relies on a string input of the value...
    String bucketStr = bucketVal instanceof Date ? ((Date)bucketVal).toInstant().toString() : bucketVal.toString();
    Query domainQ = ft.getFieldQuery(null, sf, bucketStr);

    fillBucket(bucket, domainQ, null, skip, facetInfo);

    return bucket;
  }

  private static class CachingCountSlotAccFactory implements CountSlotAccFactory {

    private final SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache;
    private final CacheKey topLevelKey;
    private final String field;

    public CachingCountSlotAccFactory(SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache,
        CacheKey topLevelKey, String field) {
      this.facetCache = facetCache;
      this.topLevelKey = topLevelKey;
      this.field = field;
    }

    @Override
    public ParallelCountAccStruct newInstance(QueryResultKey qKey, DocSet docs, boolean isBase, FacetContext fcontext, int numSlots) {
      FacetCacheKey facetCacheKey = new FacetCacheKey(qKey, field);
      final Map<CacheKey, SegmentCacheEntry> segmentCache = facetCache.get(facetCacheKey);
      final Map<CacheKey, SegmentCacheEntry> newSegmentCache;
      SegmentCacheEntry topLevelEntry;
      final CacheState cacheState;
      if (segmentCache == null) {
        // no cache presence; initialize.
        cacheState = CacheState.NOT_CACHED;
        newSegmentCache = new HashMap<>(fcontext.searcher.getIndexReader().leaves().size() + 1);
      } else if (segmentCache.containsKey(topLevelKey)) {
        topLevelEntry = segmentCache.get(topLevelKey);
        CachedCountSlotAcc acc = new CachedCountSlotAcc(fcontext, topLevelEntry.topLevelCounts);
        return new ParallelCountAccStruct(qKey, docs, CacheState.CACHED, null, isBase, acc,
            new ReadOnlyCountSlotAccWrapper(fcontext, acc), acc);
      } else {
        // defensive copy, since cache entries are shared across threads
        cacheState = CacheState.PARTIALLY_CACHED;
        newSegmentCache = new HashMap<>(fcontext.searcher.getIndexReader().leaves().size() + 1);
        newSegmentCache.putAll(segmentCache);
      }
      CacheUpdateCountSlotAcc acc = new CacheUpdateCountSlotAcc(fcontext, numSlots, newSegmentCache, topLevelKey, facetCache, facetCacheKey);
      return new ParallelCountAccStruct(qKey, docs, cacheState, segmentCache, isBase, acc,
          new ReadOnlyCountSlotAccWrapper(fcontext, acc), acc);
    }
  }
}
