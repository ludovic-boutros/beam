package org.apache.beam.sdk.io.elasticsearch;

import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.BulkItemResponseAdapter;

public class BulkItemReponse {
  private final BulkItemResponseAdapter bulkItemResponseAdapter;

  private BulkItemReponse(BulkItemResponseAdapter bulkItemResponseAdapter) {
    this.bulkItemResponseAdapter = bulkItemResponseAdapter;
  }

  public static BulkItemReponse of(BulkItemResponseAdapter bulkItemResponseAdapter) {
    return new BulkItemReponse(bulkItemResponseAdapter);
  }
}
