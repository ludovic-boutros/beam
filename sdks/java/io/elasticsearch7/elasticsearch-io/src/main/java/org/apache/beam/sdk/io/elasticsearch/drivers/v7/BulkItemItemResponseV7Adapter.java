package org.apache.beam.sdk.io.elasticsearch.drivers.v7;

import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkItemResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.rest.RestStatus;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.BulkItemResponseAdapter;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class BulkItemItemResponseV7Adapter implements BulkItemResponseAdapter {
  private final BulkItemResponse bulkItemResponse;

  private BulkItemItemResponseV7Adapter(BulkItemResponse bulkItemResponse) {
    this.bulkItemResponse = bulkItemResponse;
  }

  public static BulkItemItemResponseV7Adapter of(Object bulkItemResponse) {
    checkArgument(bulkItemResponse instanceof BulkItemResponse,
            "Cannot cast Elasticsearch response into BulkItemResponse. " +
                    "Found: " + bulkItemResponse.getClass().getName());

    return new BulkItemItemResponseV7Adapter((BulkItemResponse) bulkItemResponse);
  }

  @Override
  public boolean isFailed() {
    return bulkItemResponse.isFailed();
  }

  @Override
  public String getId() {
    return bulkItemResponse.getId();
  }

  @Override
  public RestStatus getFailureStatus() {
    return bulkItemResponse.getFailure().getStatus();
  }
}
