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
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class of an Elasticsearch 7 Beam IO.
 *
 * <p>TODO: add some documentation (see the README.md)
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class Elasticsearch7IO {
  private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7IO.class);
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final String DEFAULT_SCROLL_KEEPALIVE = "5m";
  private static final int DEFAULT_MAX_BATCH_SIZE = 1000;
  private static final int DEFAULT_MAX_BATCH_SIZE_BYTES = 5 * 1024 * 1024;

  public static Read.Builder read() {
    return Read.builder()
        .setScrollKeepalive(DEFAULT_SCROLL_KEEPALIVE)
        .setBatchSize(DEFAULT_BATCH_SIZE);
  }

  public static <T extends Writeable & DocWriteRequest<T>> Write.Builder<T> write() {
    Write.Builder<T> builder = Write.builder();

    return builder
        // advised default starting batch size in ES docs
        .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
        // advised default starting batch size in ES docs
        .setMaxBatchSizeBytes(DEFAULT_MAX_BATCH_SIZE_BYTES);
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SearchHit>> {

    public abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    public abstract ValueProvider<QueryBuilder> getQuery();

    public abstract String getScrollKeepalive();

    public abstract int getBatchSize();

    public static Builder builder() {
      return new AutoValue_Elasticsearch7IO_Read.Builder();
    }

    public static Builder withQuery(QueryBuilder query) {
      return read().setQuery(QueryBuilderValueProvider.of(query));
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      public abstract Builder setQuery(ValueProvider<QueryBuilder> query);

      public abstract Builder setScrollKeepalive(String scrollKeepalive);

      public abstract Builder setBatchSize(int batchSize);

      public abstract Read build();
    }

    @Override
    public PCollection<SearchHit> expand(PBegin input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  public static class BoundedElasticsearchSource extends BoundedSource<SearchHit> {
    private static final String INDEX_NUMBER_OF_SHARDS = "index.number_of_shards";
    private static final String STATS_STORE_API = "_stats/store";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INDICES = "indices";
    private static final String PRIMARIES = "primaries";
    private static final String STORE = "store";
    private static final String SIZE_IN_BYTES = "size_in_bytes";

    private final Read spec;
    @Nullable private final Integer numSlices;
    @Nullable private final Integer sliceId;
    @Nullable private Long estimatedByteSize;

    private ElasticsearchIndiceInfo stats;

    // constructor used in split()
    private BoundedElasticsearchSource(
        Read spec,
        @Nullable Integer numSlices,
        @Nullable Integer sliceId,
        @Nullable Long estimatedByteSize) {
      this.spec = spec;
      this.numSlices = numSlices;
      this.estimatedByteSize = estimatedByteSize;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<SearchHit>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      int nbBundles;

      // First initialize some stats
      fillStats();

      long indexSize = getEstimatedSizeBytes(options);
      if (desiredBundleSizeBytes > 0) {
        float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
        int indexShardCount = stats.getShardNumber();

        // We should not use more bundles than the index shard count.
        // See complexity and potential memory explosion issues here:
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#sliced-scroll
        nbBundles = Math.min(indexShardCount, (int) Math.ceil(nbBundlesFloat));
      } else {
        nbBundles = 1;
      }
      // split the index into nbBundles chunks of desiredBundleSizeBytes by creating
      // nbBundles sources each reading a slice of the index
      // (see
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-scroll)
      // the slice API allows to split the ES shards
      // to have bundles closer to desiredBundleSizeBytes
      for (int i = 0; i < nbBundles; i++) {
        long estimatedByteSizeForBundle = indexSize / nbBundles;
        sources.add(new BoundedElasticsearchSource(spec, nbBundles, i, estimatedByteSizeForBundle));
      }

      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (estimatedByteSize != null) {
        return estimatedByteSize;
      }

      fillStats();

      LOG.debug("estimate source byte size: total index size " + stats.getIndexByteSize());
      LOG.debug("estimate source byte size: total document count " + stats.getIndexDocumentCount());
      LOG.debug("estimate source byte size: query document count " + stats.getQueryDocumentCount());

      if (stats.getIndexDocumentCount()
          == 0) { // The min size is 1, because DirectRunner does not like 0
        estimatedByteSize = 1L;
        return estimatedByteSize;
      }

      if (stats.getQueryDocumentCount()
          == 0) { // The min size is 1, because DirectRunner does not like 0
        estimatedByteSize = 1L;
        return estimatedByteSize;
      }

      estimatedByteSize =
          (stats.getIndexByteSize() / stats.getIndexDocumentCount())
              * stats.getQueryDocumentCount();
      return estimatedByteSize;
    }

    private static long getQueryCount(
        @Nonnull RestHighLevelClient restHighLevelClient,
        @Nonnull String indexName,
        @Nonnull QueryBuilder query)
        throws IOException {
      CountResponse response =
          restHighLevelClient.count(
              new CountRequest().indices(indexName).query(query), RequestOptions.DEFAULT);

      return response.getCount();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("numSlices", numSlices));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
    }

    @Override
    public BoundedReader<SearchHit> createReader(PipelineOptions options) {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<SearchHit> getOutputCoder() {
      return WriteableCoder.of();
    }

    /**
     * Retrieve indice statistics in order to compute bundle count.
     *
     * <p>- total document count
     *
     * <p>- query document count
     *
     * <p>- index size in byte
     *
     * <p>- index shard number
     *
     * @throws IOException in case of Elasticsearch querying issues
     */
    private void fillStats() throws IOException {
      if (stats != null) {
        // already filled up
        return;
      }

      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      ValueProvider<QueryBuilder> query = spec.getQuery();

      try (RestHighLevelClient restHighLevelClient = connectionConfiguration.createClient()) {
        // Retrieve index shard number
        GetSettingsRequest request =
            new GetSettingsRequest()
                .indices(connectionConfiguration.getIndex())
                .names(INDEX_NUMBER_OF_SHARDS)
                .includeDefaults(true)
                .indicesOptions(IndicesOptions.lenientExpandOpen());

        GetSettingsResponse settingsResponse =
            restHighLevelClient.indices().getSettings(request, RequestOptions.DEFAULT);
        int numberOfShards =
            Integer.parseInt(
                settingsResponse.getSetting(
                    connectionConfiguration.getIndex(), INDEX_NUMBER_OF_SHARDS));

        // Retrieve total index document count
        CountResponse countResponse =
            restHighLevelClient.count(
                new CountRequest()
                    .indices(connectionConfiguration.getIndex())
                    .query(QueryBuilders.matchAllQuery()),
                RequestOptions.DEFAULT);

        // Retrieve query document count
        long queryCount;

        if (query == null || query.get() == null) {
          queryCount = countResponse.getCount();
        } else {
          if (connectionConfiguration.getIndex() == null) {
            throw new IllegalArgumentException(
                "Index must be configured in connection configuration for reading operations.");
          }
          queryCount =
              getQueryCount(restHighLevelClient, connectionConfiguration.getIndex(), query.get());
        }

        // Retrieve index size
        long indexByteSize = getIndexByteSize(connectionConfiguration, restHighLevelClient);

        stats =
            ElasticsearchIndiceInfo.builder()
                .setShardNumber(numberOfShards)
                .setIndexDocumentCount(countResponse.getCount())
                .setQueryDocumentCount(queryCount)
                .setIndexByteSize(indexByteSize)
                .build();
      }
    }

    private long getIndexByteSize(
        ConnectionConfiguration connectionConfiguration, RestHighLevelClient restHighLevelClient)
        throws IOException {
      Response response =
          restHighLevelClient
              .getLowLevelClient()
              .performRequest(buildStatsRequest(connectionConfiguration.getIndex()));
      return MAPPER
          .readValue(response.getEntity().getContent(), JsonNode.class)
          .path(INDICES)
          .path(connectionConfiguration.getIndex())
          .path(PRIMARIES)
          .path(STORE)
          .path(SIZE_IN_BYTES)
          .asLong();
    }

    private Request buildStatsRequest(String index) {
      StringJoiner joiner = new StringJoiner("/", "/", "");

      return new Request(HttpMethod.GET.name(), joiner.add(index).add(STATS_STORE_API).toString());
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<SearchHit> {

    private final BoundedElasticsearchSource source;

    private RestHighLevelClient restClient;
    private SearchHit current;
    private String scrollId;
    private ListIterator<SearchHit> batchIterator;

    private BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient();

      QueryBuilder query = source.spec.getQuery() != null ? source.spec.getQuery().get() : null;
      if (query == null) {
        query = QueryBuilders.matchAllQuery();
      }

      SearchSourceBuilder searchSourceBuilder =
          SearchSourceBuilder.searchSource().size(source.spec.getBatchSize()).query(query);

      // Do we use slices ?
      if (source.sliceId != null && source.numSlices != null && source.numSlices > 1) {
        searchSourceBuilder.slice(new SliceBuilder(source.sliceId, source.numSlices));
      }

      SearchRequest searchRequest = new SearchRequest();
      searchRequest.source(searchSourceBuilder).scroll(source.spec.getScrollKeepalive());

      SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
      setScrollId(response);

      return readNextBatchAndReturnFirstDocument(response);
    }

    @Override
    public boolean advance() throws IOException {
      if (batchIterator.hasNext()) {
        current = batchIterator.next();

        return true;
      } else {
        if (scrollId == null) {
          return false;
        } else {
          SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
          // don't forget to keep the scroll context
          searchScrollRequest.scroll(source.spec.getScrollKeepalive());

          SearchResponse response = restClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
          setScrollId(response);

          return readNextBatchAndReturnFirstDocument(response);
        }
      }
    }

    private void setScrollId(SearchResponse response) {
      scrollId = response.getScrollId();
    }

    private boolean readNextBatchAndReturnFirstDocument(SearchResponse searchResult) {
      // stop if no more data
      SearchHit[] hits = searchResult.getHits().getHits();
      if (hits.length == 0) {
        current = null;
        batchIterator = null;

        return false;
      }

      batchIterator = Arrays.asList(hits).listIterator();
      current = batchIterator.next();

      return true;
    }

    @Override
    public SearchHit getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }

      return current;
    }

    @Override
    public void close() throws IOException {
      // remove the scroll (best effort)
      try {
        if (scrollId != null) {
          ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
          clearScrollRequest.setScrollIds(Collections.singletonList(scrollId));
          restClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        }
      } catch (Exception ex) {
        LOG.warn("It seems that the scroll object has been already freed: ", ex);
      } finally {
        if (restClient != null) {
          restClient.close();
        }
      }
    }

    @Override
    public BoundedSource<SearchHit> getCurrentSource() {
      return source;
    }
  }

  /**
   * A {@link PTransform} writing data to Elasticsearch.
   *
   * <p>It takes {@link DocWriteRequest} objects as input and returns a {@link WriteResult} in order
   * to manage errors.
   */
  @AutoValue
  public abstract static class Write<T extends Writeable & DocWriteRequest<T>>
      extends PTransform<PCollection<T>, WriteResult<T>> {
    private static final String FAILED_INDEXING_TAG_ID = "failedIndexing";
    private static final String MAIN_INDEXING_TAG_ID = "successIndexing";

    public abstract ConnectionConfiguration getConnectionConfiguration();

    public abstract int getMaxBatchSize();

    public abstract int getMaxBatchSizeBytes();

    @Nullable
    public abstract RetryConfiguration getRetryConfiguration();

    public static <T extends Writeable & DocWriteRequest<T>> Builder<T> builder() {
      return new AutoValue_Elasticsearch7IO_Write.Builder<>();
    }

    @AutoValue.Builder
    public abstract static class Builder<T extends Writeable & DocWriteRequest<T>> {
      public abstract Builder<T> setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      public abstract Builder<T> setMaxBatchSize(int maxBatchSize);

      public abstract Builder<T> setMaxBatchSizeBytes(int maxBatchSizeBytes);

      public abstract Builder<T> setRetryConfiguration(RetryConfiguration retryConfiguration);

      public abstract Write<T> build();
    }

    @Override
    public WriteResult<T> expand(PCollection<T> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");

      TupleTag<BulkItemResponseContainer<T>> failedIndexingTag =
          new TupleTag<BulkItemResponseContainer<T>>(FAILED_INDEXING_TAG_ID) {};
      TupleTag<BulkItemResponseContainer<T>> successfulIndexingTag =
          new TupleTag<BulkItemResponseContainer<T>>(MAIN_INDEXING_TAG_ID) {};

      PCollectionTuple tuple =
          input.apply(
              ParDo.of(new WriteFn<>(this, successfulIndexingTag, failedIndexingTag))
                  .withOutputTags(successfulIndexingTag, TupleTagList.of(failedIndexingTag)));

      return WriteResult.in(
          input.getPipeline(),
          failedIndexingTag,
          tuple.get(failedIndexingTag).setCoder(BulkItemResponseContainerCoder.of()),
          successfulIndexingTag,
          tuple.get(successfulIndexingTag).setCoder(BulkItemResponseContainerCoder.of()));
    }

    static class WriteFn<T extends Writeable & DocWriteRequest<T>>
        extends DoFn<T, BulkItemResponseContainer<T>> {
      private final Write<T> spec;
      private final TupleTag<BulkItemResponseContainer<T>> successfulIndexingTag;
      private final TupleTag<BulkItemResponseContainer<T>> failedIndexingTag;
      private transient RestHighLevelClient restClient;

      private HashMap<String, SerializableValueInSingleWindow<T>> batch;

      private int currentBatchSizeBytes;
      private String currentBatchId;
      private transient FluentBackoff retryBackoff;
      private final WriteableCoder<T> requestCoder = WriteableCoder.of();

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);

      static final String RETRY_ATTEMPT_LOG = "Error writing to Elasticsearch. Retry attempt[{}}].";

      static final String RETRY_FAILED_LOG =
          "Error writing to ES after {} attempt(s). No more attempts allowed.";

      WriteFn(
          Write<T> spec,
          TupleTag<BulkItemResponseContainer<T>> successfulIndexingTag,
          TupleTag<BulkItemResponseContainer<T>> failedIndexingTag) {
        this.spec = spec;
        this.successfulIndexingTag = successfulIndexingTag;
        this.failedIndexingTag = failedIndexingTag;
      }

      @Setup
      @SuppressWarnings("unused")
      public void setup() throws IOException {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
        checkBackendVersion(connectionConfiguration);
        restClient = connectionConfiguration.createClient();

        if (spec.getRetryConfiguration() != null) {
          retryBackoff =
              FluentBackoff.DEFAULT
                  .withInitialBackoff(RETRY_INITIAL_BACKOFF)
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
      }

      @StartBundle
      @SuppressWarnings("unused")
      public void startBundle(StartBundleContext context) {
        batch = new HashMap<>(spec.getMaxBatchSize());
        currentBatchSizeBytes = 0;
        currentBatchId = generateId();
      }

      @ProcessElement
      @SuppressWarnings("unused")
      public void processElement(
          @Element T element,
          @Timestamp Instant timestamp,
          BoundedWindow window,
          PaneInfo pane,
          MultiOutputReceiver receiver)
          throws Exception {
        checkArgument(
            element.id() != null,
            "Index request's ids cannot be null ! UUIDs.base64UUID() can be used in order to generate one.");

        batch.put(
            element.id(), SerializableValueInSingleWindow.of(element, timestamp, window, pane));
        currentBatchSizeBytes += requestCoder.getSize(element);

        // Test max batch size (document count or size)
        if (batch.size() >= spec.getMaxBatchSize()
            || currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
          flushBatch(
              (valueInSingleWindow, bulkItemResponse) ->
                  receiver
                      .get(successfulIndexingTag)
                      .outputWithTimestamp(
                          buildBulkItemResponseContainerWith(bulkItemResponse),
                          valueInSingleWindow.getTimestamp()),
              (valueInSingleWindow, bulkItemResponse) ->
                  receiver
                      .get(failedIndexingTag)
                      .outputWithTimestamp(
                          buildBulkItemResponseContainerWith(bulkItemResponse),
                          valueInSingleWindow.getTimestamp()));
        }
      }

      @FinishBundle
      @SuppressWarnings("unused")
      public void finishBundle(FinishBundleContext context) throws IOException {
        flushBatch(
            (valueInSingleWindow, bulkItemResponse) ->
                context.output(
                    successfulIndexingTag,
                    buildBulkItemResponseContainerWith(bulkItemResponse),
                    valueInSingleWindow.getTimestamp(),
                    valueInSingleWindow.getWindow()),
            (valueInSingleWindow, bulkItemResponse) ->
                context.output(
                    failedIndexingTag,
                    buildBulkItemResponseContainerWith(bulkItemResponse),
                    valueInSingleWindow.getTimestamp(),
                    valueInSingleWindow.getWindow()));
      }

      private BulkItemResponseContainer<T> buildBulkItemResponseContainerWith(
          BulkItemResponse bulkItemResponse) {
        BulkItemResponseContainer.Builder<T> builder = BulkItemResponseContainer.builder();

        return builder
            .setBulkItemResponse(bulkItemResponse)
            .setWriteRequest(batch.get(bulkItemResponse.getId()).getValue())
            .setBatchId(currentBatchId)
            .build();
      }

      @Teardown
      @SuppressWarnings("unused")
      public void closeClient() throws IOException {
        if (restClient != null) {
          restClient.close();
        }
      }

      private void flushBatch(
          BiConsumer<ValueInSingleWindow<T>, BulkItemResponse> successfulOutput,
          BiConsumer<ValueInSingleWindow<T>, BulkItemResponse> failureOutput)
          throws IOException {
        if (batch.isEmpty()) {
          return;
        }

        BulkRequest request = buildBulkRequest();

        BulkResponse bulkResponse = restClient.bulk(request, RequestOptions.DEFAULT);

        if (bulkResponse.getItems() != null) {
          Arrays.stream(bulkResponse.getItems())
              .forEach(
                  bulkItemResponse -> {
                    // Do we have an error ?
                    if (bulkItemResponse.isFailed()) {
                      handleRetry(
                          successfulOutput,
                          failureOutput,
                          batch.get(bulkItemResponse.getId()),
                          bulkItemResponse);
                    } else {
                      successfulOutput.accept(
                          batch.get(bulkItemResponse.getId()), bulkItemResponse);
                    }
                  });
        }

        // Re-init for the next batch
        batch.clear();
        currentBatchSizeBytes = 0;
        currentBatchId = generateId();
      }

      private void handleRetry(
          BiConsumer<ValueInSingleWindow<T>, BulkItemResponse> successfulOutput,
          BiConsumer<ValueInSingleWindow<T>, BulkItemResponse> failureOutput,
          ValueInSingleWindow<T> indexRequestValueInSingleWindow,
          BulkItemResponse bulkItemResponse) {
        RetryConfiguration retryConfiguration = spec.getRetryConfiguration();
        if (retryConfiguration != null
            && retryConfiguration
                .getRetryPredicate()
                .test(bulkItemResponse.getFailure().getStatus())) {
          Sleeper sleeper = Sleeper.DEFAULT;
          BackOff backoff = retryBackoff.backoff();
          int attempt = 0;

          BulkItemResponse retriedBulkItemResponse = bulkItemResponse;

          // while retry policy exists
          try {
            DocWriteRequest<?> writeRequest = indexRequestValueInSingleWindow.getValue();

            if (writeRequest == null) {
              LOG.error("Cannot retrieve Elasticsearch WriteRequest. No retry will be done.");
            } else {
              while (retriedBulkItemResponse.isFailed() && BackOffUtils.next(sleeper, backoff)) {
                LOG.warn(RETRY_ATTEMPT_LOG, ++attempt);

                BulkRequest retriedRequest = new BulkRequest();
                retriedRequest.add(writeRequest);

                BulkResponse retriedBulkResponse =
                    restClient.bulk(retriedRequest, RequestOptions.DEFAULT);

                // We know that we only have one response
                retriedBulkItemResponse = retriedBulkResponse.getItems()[0];
              }
            }
          } catch (InterruptedException e) {
            LOG.error("Retry interrupted", e);
          } catch (IOException e) {
            LOG.error("Exception during retry", e);
          }

          if (retriedBulkItemResponse.isFailed()) {
            LOG.error(RETRY_FAILED_LOG, attempt);
            failureOutput.accept(indexRequestValueInSingleWindow, retriedBulkItemResponse);
          } else {
            successfulOutput.accept(indexRequestValueInSingleWindow, retriedBulkItemResponse);
          }
        }
      }

      private BulkRequest buildBulkRequest() {
        BulkRequest bulkRequest = new BulkRequest();

        batch.values().stream()
            .map(ValueInSingleWindow::getValue)
            .filter(Objects::nonNull)
            .forEach(bulkRequest::add);

        return bulkRequest;
      }
    }
  }

  /**
   * This IO is (currently) only compliant with Elasticsearch 7.x.
   *
   * @param connectionConfiguration given Elasticsearch connection configuration
   */
  private static void checkBackendVersion(ConnectionConfiguration connectionConfiguration) {
    try (RestHighLevelClient restHighLevelClient = connectionConfiguration.createClient()) {
      MainResponse info = restHighLevelClient.info(RequestOptions.DEFAULT);

      int backendVersion = Integer.parseInt(info.getVersion().getNumber().substring(0, 1));
      checkArgument(
          (backendVersion == 7),
          "The Elasticsearch version to connect to is %s.x. "
              + "This version of the Elasticsearch7IO is only compatible with "
              + "Elasticsearch v7.x",
          backendVersion);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }

  private static String generateId() {
    return UUIDs.base64UUID();
  }
}
