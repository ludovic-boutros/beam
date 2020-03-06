package org.apache.beam.sdk.io.elasticsearch.drivers;

import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.BulkItemResponseAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.RestHighLevelClientAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.RestHighLevelClientAdapter.Configuration;
import org.apache.beam.sdk.io.elasticsearch.drivers.v7.BulkItemItemResponseV7Adapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.v7.RestHighLevelClientV7Adapter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class AdapterFactory {
  private static final Map<Integer, Function<Object, BulkItemResponseAdapter>> BULK_RESPONSE_SUPPLIERS = new HashMap<>();
  private static final Map<Integer, Function<RestHighLevelClientAdapter.Configuration, RestHighLevelClientAdapter>> REST_CLIENT_SUPPLIERS = new HashMap<>();

  static {
    BULK_RESPONSE_SUPPLIERS.put(7, BulkItemItemResponseV7Adapter::of);

    REST_CLIENT_SUPPLIERS.put(7, RestHighLevelClientV7Adapter::of);
  }

  public static BulkItemResponseAdapter bulkResponseAdapter(int version, Object bulkItemResponse) {
    Function<Object, BulkItemResponseAdapter> builder = BULK_RESPONSE_SUPPLIERS.get(version);
    checkArgument(builder != null, "Cannot find BulkResponseAdapter for version: " + version);

    return builder.apply(bulkItemResponse);
  }

  public static RestHighLevelClientAdapter restHighLevelClientAdapter(int version, Configuration configuration) {
    Function<Configuration, RestHighLevelClientAdapter> builder = REST_CLIENT_SUPPLIERS.get(version);
    checkArgument(configuration != null, "Cannot find RestHighLevelClientAdapter for version: " + version);

    return builder.apply(configuration);
  }
}
