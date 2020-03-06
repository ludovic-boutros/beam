package org.apache.beam.sdk.io.elasticsearch;

import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchHitAdapter;

import java.io.Serializable;

public class Document implements Serializable {
  private final SearchHitAdapter searchHitAdapter;

  private Document(SearchHitAdapter searchHitAdapter) {
    this.searchHitAdapter = searchHitAdapter;
  }

  public static Document of(SearchHitAdapter searchHitAdapter) {
    return new Document(searchHitAdapter);
  }
}
