package org.apache.beam.sdk.io.elasticsearch.drivers.v7;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.search.ClearScrollRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.search.SearchRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.search.SearchResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.search.SearchScrollRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.support.IndicesOptions;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.Request;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RequestOptions;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.Response;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestClient;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestClientBuilder;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestHighLevelClient;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.core.CountRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.core.CountResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.core.MainResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.index.query.QueryBuilders;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.search.slice.SliceBuilder;
import org.apache.beam.sdk.io.elasticsearch.WriteRequest;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.BulkItemResponseAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.RestHighLevelClientAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchResponseAdapter;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class RestHighLevelClientV7Adapter implements RestHighLevelClientAdapter {
  private final RestHighLevelClient client;

  private RestHighLevelClientV7Adapter(RestHighLevelClient client) {
    this.client = client;
  }

  public static RestHighLevelClientV7Adapter of(Configuration configuration) {
    RestClientBuilder restClientBuilder = RestClient.builder(configuration.hosts);

    restClientBuilder.setHttpClientConfigCallback(
            httpClientBuilder -> {
              if (configuration.credentialsProvider != null) {
                httpClientBuilder.setDefaultCredentialsProvider(configuration.credentialsProvider);
              }

              if (configuration.sslContext != null) {
                httpClientBuilder.setSSLContext(configuration.sslContext).setSSLStrategy(configuration.sessionStrategy);
              }

              return httpClientBuilder;
            });

    restClientBuilder.setRequestConfigCallback(
            requestConfigBuilder -> {
              if (configuration.connectionTimeout != null) {
                requestConfigBuilder.setConnectTimeout(configuration.connectionTimeout);
              }
              if (configuration.socketAndRetryTimeout != null) {
                requestConfigBuilder.setSocketTimeout(configuration.socketAndRetryTimeout);
              }
              return requestConfigBuilder;
            });

    return new RestHighLevelClientV7Adapter(new RestHighLevelClient(restClientBuilder));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public int backendVersion() throws IOException {
    MainResponse info = client.info(RequestOptions.DEFAULT);

    return Integer.parseInt(info.getVersion().getNumber().substring(0, 1));
  }

  @Override
  public long count(String queryString, String indexName) throws IOException {
    CountResponse response =
            client.count(
                    new CountRequest()
                            .indices(indexName)
                            .query(QueryBuilders.queryStringQuery(queryString)),
                    RequestOptions.DEFAULT);
    return response.getCount();
  }

  @Override
  public int numberOfShards(String indexName) throws IOException {
    GetSettingsRequest request =
            new GetSettingsRequest()
                    .indices(indexName)
                    .names(INDEX_NUMBER_OF_SHARDS)
                    .includeDefaults(true)
                    .indicesOptions(IndicesOptions.lenientExpandOpen());

    GetSettingsResponse settingsResponse =
            client.indices().getSettings(request, RequestOptions.DEFAULT);

    return Integer.parseInt(
            settingsResponse.getSetting(
                    indexName, INDEX_NUMBER_OF_SHARDS));
  }

  @Override
  public long getIndexByteSize(String indexName) throws IOException {
    Response response =
            client
                    .getLowLevelClient()
                    .performRequest(buildStatsRequest(indexName));
    return MAPPER
            .readValue(response.getEntity().getContent(), JsonNode.class)
            .path(INDICES)
            .path(indexName)
            .path(PRIMARIES)
            .path(STORE)
            .path(SIZE_IN_BYTES)
            .asLong();
  }

  @Override
  public SearchResponseAdapter search(String query, int batchSize, Integer sliceId, Integer numSlices, String scrollKeepalive) throws IOException {
    SearchSourceBuilder searchSourceBuilder =
            SearchSourceBuilder.searchSource()
                    .size(batchSize)
                    .query(QueryBuilders.queryStringQuery(query));

    // Do we use slices ?
    if (sliceId != null && numSlices != null && numSlices > 1) {
      searchSourceBuilder.slice(new SliceBuilder(sliceId, numSlices));
    }

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder).scroll(scrollKeepalive);

    SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

    return SearchResponseV7Adapter.of(response);
  }

  @Override
  public SearchResponseAdapter scroll(String scrollId, String scrollKeepalive) throws IOException {
    SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
    // don't forget to keep the scroll context
    searchScrollRequest.scroll(scrollKeepalive);

    SearchResponse response = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);

    return SearchResponseV7Adapter.of(response);
  }

  @Override
  public void clearScroll(String scrollId) throws IOException {
    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.setScrollIds(Collections.singletonList(scrollId));
    client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
  }

  @Override
  public List<BulkItemResponseAdapter> bulk(List<WriteRequest> request) throws IOException {
    BulkRequest bulkRequest = new BulkRequest();

    // TODO: map WriteRequest classes to Elasticsearch classes

    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

    if (bulkResponse.getItems() == null) {
      return Collections.emptyList();
    } else {
      return Arrays.stream(bulkResponse.getItems())
              .map(BulkItemItemResponseV7Adapter::of)
              .collect(Collectors.toList());
    }
  }

  @Override
  public int size(WriteRequest element) {
    // TODO: map WriteRequest classes to Elasticsearch classes
    return 0;
  }

  private Request buildStatsRequest(String index) {
    StringJoiner joiner = new StringJoiner("/", "/", "");

    return new Request(HttpMethod.GET.name(), joiner.add(index).add(STATS_STORE_API).toString());
  }
}
