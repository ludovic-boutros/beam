package org.apache.beam.sdk.io.elasticsearch.drivers.adapters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.repackaged.elasticsearch7_driver.org.apache.http.HttpHost;
import org.apache.beam.repackaged.elasticsearch7_driver.org.apache.http.client.CredentialsProvider;
import org.apache.beam.repackaged.elasticsearch7_driver.org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.beam.sdk.io.elasticsearch.WriteRequest;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.List;

public interface RestHighLevelClientAdapter extends AutoCloseable {
  String INDEX_NUMBER_OF_SHARDS = "index.number_of_shards";
  String MATCH_ALL_LUCENE_QUERY_STRING = "*:*";
  String INDICES = "indices";
  String PRIMARIES = "primaries";
  String STORE = "store";
  String SIZE_IN_BYTES = "size_in_bytes";

  String STATS_STORE_API = "_stats/store";
  ObjectMapper MAPPER = new ObjectMapper();

  class Configuration {
    public final HttpHost[] hosts;
    public final CredentialsProvider credentialsProvider;
    public final SSLContext sslContext;
    public final SSLIOSessionStrategy sessionStrategy;
    public final Integer connectionTimeout;

    public Configuration(HttpHost[] hosts,
                         CredentialsProvider credentialsProvider,
                         SSLContext sslContext,
                         SSLIOSessionStrategy sessionStrategy,
                         Integer connectionTimeout,
                         Integer socketAndRetryTimeout) {
      this.hosts = hosts;
      this.credentialsProvider = credentialsProvider;
      this.sslContext = sslContext;
      this.sessionStrategy = sessionStrategy;
      this.connectionTimeout = connectionTimeout;
      this.socketAndRetryTimeout = socketAndRetryTimeout;
    }

    public final Integer socketAndRetryTimeout;
  }

  int backendVersion() throws IOException;

  long count(String queryString, String indexName) throws IOException;

  int numberOfShards(String indexName) throws IOException;

  long getIndexByteSize(String index) throws IOException;

  SearchResponseAdapter search(String query,
                               int batchSize,
                               Integer sliceId,
                               Integer numSlices,
                               String scrollKeepalive) throws IOException;

  SearchResponseAdapter scroll(String scrollId, String scrollKeepalive) throws IOException;

  void clearScroll(String scrollId) throws IOException;

  List<BulkItemResponseAdapter> bulk(List<WriteRequest> request) throws IOException;

  int size(WriteRequest element);
}
