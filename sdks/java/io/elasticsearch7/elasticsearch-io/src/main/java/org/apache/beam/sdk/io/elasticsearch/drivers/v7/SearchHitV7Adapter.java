package org.apache.beam.sdk.io.elasticsearch.drivers.v7;

import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.search.SearchHit;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchHitAdapter;

public class SearchHitV7Adapter implements SearchHitAdapter {
  private final SearchHit searchHit;

  private SearchHitV7Adapter(SearchHit searchHit) {
    this.searchHit = searchHit;
  }

  public static SearchHitV7Adapter of(SearchHit searchHit) {
    return new SearchHitV7Adapter(searchHit);
  }
}
