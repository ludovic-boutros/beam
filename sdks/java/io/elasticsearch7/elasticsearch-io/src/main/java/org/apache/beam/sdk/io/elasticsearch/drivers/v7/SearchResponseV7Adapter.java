package org.apache.beam.sdk.io.elasticsearch.drivers.v7;

import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.search.SearchResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.search.SearchHit;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchHitAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchResponseAdapter;

import java.util.Arrays;
import java.util.ListIterator;
import java.util.stream.Collectors;

public class SearchResponseV7Adapter implements SearchResponseAdapter {
  private final SearchResponse searchResponse;

  private SearchResponseV7Adapter(SearchResponse searchResponse) {
    this.searchResponse = searchResponse;
  }

  public static SearchResponseV7Adapter of(SearchResponse searchResponse) {
    return new SearchResponseV7Adapter(searchResponse);
  }

  @Override
  public String getScrollId() {
    return searchResponse.getScrollId();
  }

  @Override
  public ListIterator<? extends SearchHitAdapter> iterator() {
    SearchHit[] hits = searchResponse.getHits().getHits();

    return Arrays.asList(hits).stream()
            .map(SearchHitV7Adapter::of)
            .collect(Collectors.toList())
            .listIterator();
  }
}
