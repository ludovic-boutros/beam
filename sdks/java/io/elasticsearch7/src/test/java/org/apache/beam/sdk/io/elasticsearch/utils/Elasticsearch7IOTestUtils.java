/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.elasticsearch.utils;

import static org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer.ELASTICSEARCH_DEFAULT_USER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.io.elasticsearch.BulkItemResponseContainer;
import org.apache.beam.sdk.io.elasticsearch.WriteResult;
import org.apache.beam.sdk.io.elasticsearch.WriteableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class Elasticsearch7IOTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7IOTestUtils.class);

  public static final String ES_PASSWORD = "Be@MP0w3R";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  /** Elasticsearch Default HTTP port. */
  public static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

  private static final WriteableCoder<IndexRequest> REQUEST_CODER = WriteableCoder.of();
  public static final String ELASTICSEARCH_VERSION = System.getProperty("elasticsearchVersion");
  private static final String UPDATED = "updated";

  public static RestHighLevelClient buildRestHighLevelClient(
      ElasticsearchWithSSLContainer container)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException,
          KeyManagementException {
    RestClientBuilder restClientBuilder =
        RestClient.builder(
            new HttpHost(
                container.getContainerIpAddress(),
                container.getMappedPort(ELASTICSEARCH_DEFAULT_PORT),
                "https"));

    // configure credentials
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(ELASTICSEARCH_DEFAULT_USER, ES_PASSWORD));

    // configure SSL
    LOG.info("Using system env for SSL configuration:");
    LOG.info("javax.net.ssl.trustStore: {}", System.getProperty("javax.net.ssl.trustStore"));
    LOG.info(
        "javax.net.ssl.trustStorePassword: {}",
        System.getProperty("javax.net.ssl.trustStorePassword"));
    LOG.info("javax.net.ssl.keyStore: {}", System.getProperty("javax.net.ssl.keyStore"));
    LOG.info(
        "javax.net.ssl.keyStorePassword: {}", System.getProperty("javax.net.ssl.keyStorePassword"));

    KeyStore keyStore = KeyStore.getInstance("jks");
    try (InputStream is =
        new FileInputStream(new File(System.getProperty("javax.net.ssl.keyStore")))) {
      String keystorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
      keyStore.load(is, keystorePassword.toCharArray());
    }

    SSLContext sslContext =
        SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();

    SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);

    restClientBuilder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy);

          return httpClientBuilder;
        });

    return new RestHighLevelClient(restClientBuilder);
  }

  public static void checkHealth(@NotNull RestHighLevelClient restHighLevelClient)
      throws IOException {
    ClusterHealthResponse health =
        restHighLevelClient.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);

    if (health.isTimedOut()) {
      throw new IllegalStateException("Cannot get ES Cluster status. A timeout occurred");
    }

    if (ClusterHealthStatus.RED.equals(health.getStatus())) {
      throw new IllegalStateException("ES Cluster state is RED");
    }
  }

  private static void refresh(@NotNull RestHighLevelClient restHighLevelClient, String esIndexName)
      throws IOException {
    restHighLevelClient.indices().refresh(new RefreshRequest(esIndexName), RequestOptions.DEFAULT);
  }

  public static void deleteIndex(
      @NotNull RestHighLevelClient restHighLevelClient, String esIndexName) throws IOException {
    boolean exists =
        restHighLevelClient
            .indices()
            .exists(new GetIndexRequest(esIndexName), RequestOptions.DEFAULT);
    if (exists) {
      restHighLevelClient
          .indices()
          .delete(new DeleteIndexRequest(esIndexName), RequestOptions.DEFAULT);
    }
  }

  static void assertDocumentCount(
      RestHighLevelClient restHighLevelClient, String esIndexName, int count) throws IOException {
    // Make sure that everything is indexed
    refresh(restHighLevelClient, esIndexName);

    CountResponse response =
        restHighLevelClient.count(
            new CountRequest().indices(esIndexName).query(QueryBuilders.matchAllQuery()),
            RequestOptions.DEFAULT);

    assertThat(response.getCount()).isEqualTo(count);
  }

  public static void assertDocumentIsUpdated(
      RestHighLevelClient restHighLevelClient, String esIndexName, String id) throws IOException {
    // Make sure that everything is indexed
    refresh(restHighLevelClient, esIndexName);

    GetResponse response =
        restHighLevelClient.get(new GetRequest().index(esIndexName).id(id), RequestOptions.DEFAULT);

    boolean isUpdated = (boolean) response.getSource().getOrDefault(UPDATED, false);

    assertThat(isUpdated).isTrue();
  }

  static <T extends Writeable & DocWriteRequest<T>> void assertResultContains(
      WriteResult<T> result, int successfulDocumentCount, int failedDocumentCount) {
    assertOutput(
        "Failure output should contains: ", result.getFailedIndexing(), failedDocumentCount, true);
    assertOutput(
        "Success output should contains: ",
        result.getSuccessfulIndexing(),
        successfulDocumentCount,
        false);
  }

  private static <T extends Comparable<? super T>> boolean isSorted(
      final Iterable<? extends T> iterable) {
    T previous = null;
    for (final T current : iterable) { // iterable produces
      if (previous != null // comparator consumes
          && current.compareTo(previous) <= 0) {
        return false;
      }
      previous = current;
    }
    return true;
  }

  static <T extends Writeable & DocWriteRequest<T>> void assertBatchOrder(
      PCollection<KV<String, Iterable<BulkItemResponseContainer<T>>>> perBatchResponses) {
    PAssert.that("Batches must keep request order", perBatchResponses)
        .satisfies(
            (SerializableFunction<
                    Iterable<KV<String, Iterable<BulkItemResponseContainer<T>>>>, Void>)
                element -> {
                  element.forEach(
                      batch -> {
                        List<Integer> ids =
                            StreamSupport.stream(batch.getValue().spliterator(), false)
                                .map(BulkItemResponseContainer::getBulkItemResponse)
                                .map(BulkItemResponse::getId)
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        assertThat(isSorted(ids)).isTrue();
                      });

                  return null;
                });
  }

  static void assertBatchContainsMaxElementCount(
      PCollection<KV<String, Long>> perBatchResponses, int maxBatchSize, int batchCount) {
    PAssert.that(
            "There should be a maximum of " + maxBatchSize + " element per batch",
            perBatchResponses)
        .satisfies(
            (SerializableFunction<Iterable<KV<String, Long>>, Void>)
                element -> {
                  element.forEach(
                      batch -> assertThat(batch.getValue()).isLessThanOrEqualTo(maxBatchSize));
                  return null;
                });

    PAssert.that("There should be at least " + batchCount + " batches", perBatchResponses)
        .satisfies(
            (SerializableFunction<Iterable<KV<String, Long>>, Void>)
                element -> {
                  long count = Streams.stream((element)).count();

                  // We expect at least some batches. Usually it's greater than the expected count.
                  // So we test: expected x 4 >= actual >= expected
                  assertThat(count).isGreaterThanOrEqualTo(batchCount);
                  assertThat(count).isLessThanOrEqualTo(batchCount * 4);
                  return null;
                });
  }

  private static <T extends Writeable & DocWriteRequest<T>> void assertOutput(
      String message,
      PCollection<BulkItemResponseContainer<T>> output,
      int documentCount,
      boolean isFailed) {
    PAssert.that(message, output)
        .satisfies(
            (SerializableFunction<Iterable<BulkItemResponseContainer<T>>, Void>)
                element -> {
                  Long size =
                      Streams.stream(element)
                          .peek(
                              r ->
                                  assertThat(r.getBulkItemResponse().isFailed())
                                      .isEqualTo(isFailed))
                          .peek(
                              r ->
                                  assertThat(r.getBulkItemResponse().getId())
                                      .isEqualTo(r.getWriteRequest().id()))
                          .count();

                  assertThat(size).isEqualTo(documentCount);

                  return null;
                });
  }

  public static BulkItemResponse buildBulkItemResponse() {
    IndexResponse.Builder builder = new IndexResponse.Builder();
    builder.setForcedRefresh(false);
    builder.setId("myId");
    builder.setPrimaryTerm(0);
    builder.setSeqNo(0);
    builder.setShardId(ShardId.fromString("[dummy][0]"));
    builder.setShardInfo(new ReplicationResponse.ShardInfo(5, 5));
    builder.setType("_doc");
    builder.setResult(DocWriteResponse.Result.CREATED);
    builder.setVersion(0L);

    return new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, builder.build());
  }

  public static BulkItemResponse buildErrorBulkItemResponse() {
    BulkItemResponse.Failure failure =
        new BulkItemResponse.Failure(
            "dummy",
            "_doc",
            "myId",
            new IllegalStateException("Dummy error"),
            RestStatus.status(4, 5));

    return new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, failure);
  }

  public static IndexRequest buildIndexRequest() {
    IndexRequest indexRequest = new IndexRequest("dummy");

    indexRequest.id("0");
    indexRequest.source("{ \"id\" : \"0\" }", XContentType.JSON);
    indexRequest.versionType(VersionType.EXTERNAL);
    indexRequest.version(0);

    return indexRequest;
  }

  public static void injectDocuments(
      RestHighLevelClient restHighLevelClient, String esIndexName, int count) throws IOException {
    List<ESTestDocument> esTestDocuments = generateDocuments(count, ESTestDocument::of);

    BulkRequest request = new BulkRequest();

    esTestDocuments.stream()
        .map(document -> getIndexRequest(document, esIndexName, true))
        .forEach(request::add);

    if (!request.requests().isEmpty()) {
      restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }

    assertDocumentCount(restHighLevelClient, esIndexName, count);
  }

  public static void createIndex(
      RestHighLevelClient restHighLevelClient, String esIndexName, Integer shardCount)
      throws IOException {
    boolean exists =
        restHighLevelClient
            .indices()
            .exists(new GetIndexRequest(esIndexName), RequestOptions.DEFAULT);
    if (exists) {
      throw new IllegalStateException("Index '" + esIndexName + "' should not exists !");
    }

    restHighLevelClient
        .indices()
        .create(
            new CreateIndexRequest(esIndexName)
                .settings(Settings.builder().put("index.number_of_shards", shardCount).build()),
            RequestOptions.DEFAULT);
  }

  public static class ESTestDocument implements Serializable {
    public final String id;

    private ESTestDocument(String id) {
      this.id = id;
    }

    public String toJson() {
      try {
        return MAPPER.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(e);
      }
    }

    public static ESTestDocument of(int id) {
      return of(Integer.toString(id));
    }

    static ESTestDocument of(String id) {
      return new ESTestDocument(id);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ESTestDocument that = (ESTestDocument) o;
      return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  public static int computeDocumentAverageSize(int count, String esIndexName) {
    Double average =
        generateDocuments(count, ESTestDocument::of).stream()
            .map(document -> getIndexRequest(document, esIndexName, false))
            .collect(
                Collectors.averagingInt(
                    (IndexRequest element) -> {
                      try {
                        return REQUEST_CODER.getSize(element);
                      } catch (IOException e) {
                        throw new IllegalStateException(e);
                      }
                    }));

    return average.intValue();
  }

  public static <T> List<T> generateDocuments(int count, Function<Integer, T> generator) {
    return IntStream.range(0, count).boxed().map(generator).collect(Collectors.toList());
  }

  public static class StringToDeleteRequest
      extends PTransform<PCollection<String>, PCollection<DeleteRequest>> {
    private final String indexName;

    private StringToDeleteRequest(String indexName) {
      this.indexName = indexName;
    }

    public static StringToDeleteRequest of(String indexName) {
      return new StringToDeleteRequest(indexName);
    }

    @Override
    public PCollection<DeleteRequest> expand(PCollection<String> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<String, DeleteRequest>() {
                @Override
                public DeleteRequest apply(String id) {
                  return getDeleteRequest(id, indexName);
                }
              }));
    }
  }

  public static class StringToUpdateRequest
      extends PTransform<PCollection<String>, PCollection<UpdateRequest>> {
    private final String indexName;

    private StringToUpdateRequest(String indexName) {
      this.indexName = indexName;
    }

    public static StringToUpdateRequest of(String indexName) {
      return new StringToUpdateRequest(indexName);
    }

    @Override
    public PCollection<UpdateRequest> expand(PCollection<String> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<String, UpdateRequest>() {
                @Override
                public UpdateRequest apply(String id) {
                  return getUpdateRequest(id, indexName);
                }
              }));
    }
  }

  public static class JsonToESIndexRequest
      extends PTransform<PCollection<ESTestDocument>, PCollection<IndexRequest>> {
    private final String indexName;
    private final boolean useDocumentIdsAsIndexRequestIds;

    private JsonToESIndexRequest(String indexName, boolean useDocumentIdsAsIndexRequestIds) {
      this.indexName = indexName;
      this.useDocumentIdsAsIndexRequestIds = useDocumentIdsAsIndexRequestIds;
    }

    public static JsonToESIndexRequest of(
        String indexName, boolean useDocumentIdsAsIndexRequestIds) {
      return new JsonToESIndexRequest(indexName, useDocumentIdsAsIndexRequestIds);
    }

    public static JsonToESIndexRequest of(String indexName) {
      return JsonToESIndexRequest.of(indexName, false);
    }

    @Override
    public PCollection<IndexRequest> expand(PCollection<ESTestDocument> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<ESTestDocument, IndexRequest>() {
                @Override
                public IndexRequest apply(ESTestDocument document) {
                  return getIndexRequest(document, indexName, useDocumentIdsAsIndexRequestIds);
                }
              }));
    }
  }

  @NotNull
  public static IndexRequest getIndexRequest(
      ESTestDocument document, String indexName, boolean useDocumentIdsAsIndexRequestIds) {
    IndexRequest retValue = new IndexRequest();
    retValue.source(document.toJson(), XContentType.JSON);
    retValue.index(indexName);
    if (useDocumentIdsAsIndexRequestIds) {
      retValue.id(document.id);
    } else {
      // In order to retrieve the request, it needs to have an id.
      // If the id is not set, we just create one with the elasticsearch algorithm.
      retValue.id(UUIDs.base64UUID());
    }
    return retValue;
  }

  @NotNull
  private static DeleteRequest getDeleteRequest(String id, String indexName) {
    DeleteRequest retValue = new DeleteRequest();
    retValue.index(indexName);
    retValue.id(id);

    return retValue;
  }

  @NotNull
  private static UpdateRequest getUpdateRequest(String id, String indexName) {
    UpdateRequest retValue = new UpdateRequest();
    retValue.index(indexName);
    retValue.id(id);
    retValue.doc(UPDATED, true);

    return retValue;
  }

  public static class JsonToESVersionedIndexRequest
      extends PTransform<PCollection<ESTestDocument>, PCollection<IndexRequest>> {
    private final String indexName;
    private final long version;

    private JsonToESVersionedIndexRequest(String indexName, long version) {
      this.indexName = indexName;
      this.version = version;
    }

    public static JsonToESVersionedIndexRequest of(String indexName, long version) {
      return new JsonToESVersionedIndexRequest(indexName, version);
    }

    @Override
    public PCollection<IndexRequest> expand(PCollection<ESTestDocument> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<ESTestDocument, IndexRequest>() {
                @Override
                public IndexRequest apply(ESTestDocument document) {
                  IndexRequest retValue = new IndexRequest();
                  retValue.source(document.toJson(), XContentType.JSON);
                  retValue.index(indexName);
                  retValue.id(document.id);
                  retValue.version(version);
                  retValue.versionType(VersionType.EXTERNAL);

                  return retValue;
                }
              }));
    }
  }
}
