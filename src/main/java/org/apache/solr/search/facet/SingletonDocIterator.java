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

import org.apache.solr.search.DocIterator;
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountGlobal;
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountPerSeg;

final class SingletonDocIterator extends ParallelDocIterator {

  private final DocIterator backing;
  private final boolean isBase;

  SingletonDocIterator(DocIterator backing, boolean isBase) {
    super(1);
    this.backing = backing;
    this.isBase = isBase;
  }

  @Override
  public boolean hasNext() {
    return backing.hasNext();
  }

  @Override
  public int nextDoc() {
    return backing.nextDoc();
  }

  @Override
  public boolean collectBase() {
    return isBase;
  }

  @Override
  public int registerCounts(SegCountGlobal segCounts) {
    return 0;
  }

  @Override
  public int registerCounts(SegCountPerSeg segCounts) {
    return 0;
  }

}
