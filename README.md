See [SOLR-13132](https://issues.apache.org/jira/browse/SOLR-13132)

## Deployment

Care must be taken in deployment to load classes from the solrplugins jar file _before_
corresponding classes from the stock Solr jar files. This is best accomplished using
Jetty's [`extraClasspath` method](https://www.eclipse.org/jetty/documentation/current/jetty-classloading.html#using-extra-classpath-method)
to affect the load order of jar files within Jetty, adding the following child element:
```xml
  <Set name="extraClasspath">${path_to}/facet-cache-solr-7.5.0.jar</Set>
```
to the Solr Jetty `WebAppContext` configuration. The relevant config file in the
standard Solr distribution is `server/contexts/solr-jetty-context.xml`. The path is
rooted at `/opt/solr/` in the stock Solr docker image.

## Facet cache configuration
To configure the facetCache, add the following cache config to your `solrconfig.xml`:
```
<cache name="termFacetCache"
       class="solr.search.LRUCache"
       size="200"
       initialSize="200"
       autowarmCount="200"
       regenerator="solr.request.TermFacetCacheRegenerator" />
```
