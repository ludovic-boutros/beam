package org.apache.beam.sdk.io.elasticsearch.drivers.adapters;

import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.rest.RestStatus;

import java.io.Serializable;

public interface BulkItemResponseAdapter extends Serializable {
  boolean isFailed();

  String getId();

  RestStatus getFailureStatus();
}
