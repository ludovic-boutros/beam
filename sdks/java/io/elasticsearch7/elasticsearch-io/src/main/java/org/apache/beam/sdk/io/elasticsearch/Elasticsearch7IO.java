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

import com.google.auto.value.AutoValue;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.DocWriteRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkItemResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.bulk.BulkResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RequestOptions;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestHighLevelClient;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.core.MainResponse;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.UUIDs;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.elasticsearch.drivers.AdapterFactory;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.BulkItemResponseAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.RestHighLevelClientAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchHitAdapter;
import org.apache.beam.sdk.io.elasticsearch.drivers.adapters.SearchResponseAdapter;
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.io.elasticsearch.drivers.adapters.RestHighLevelClientAdapter.MATCH_ALL_LUCENE_QUERY_STRING;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

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

  public static Write.Builder write() {
    Write.Builder builder = Write.builder();

    return builder
            // advised default starting batch size in ES docs
            .setMaxBatchSize(DEFAULT_MAX_BATCH_SIZE)
            // advised default starting batch size in ES docs
            .setMaxBatchSizeBytes(DEFAULT_MAX_BATCH_SIZE_BYTES);
  }

  /**
   * A {@link PTransform} reading data from Elasticsearch.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Document>> {

    public abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    public abstract String getLuceneQueryString();

    public abstract String getScrollKeepalive();

    public abstract int getBatchSize();

    public static Builder builder() {
      return new AutoValue_Elasticsearch7IO_Read.Builder();
    }

    public static Builder withLuceneQueryString(String query) {
      return read().setLuceneQueryString(query);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConnectionConfiguration(
              ConnectionConfiguration connectionConfiguration);

      public abstract Builder setLuceneQueryString(String query);

      public abstract Builder setScrollKeepalive(String scrollKeepalive);

      public abstract Builder setBatchSize(int batchSize);

      public abstract Read build();
    }

    @Override
    public PCollection<Document> expand(PBegin input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      return input.apply(
              org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("luceneQueryString", getLuceneQueryString()));
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /**
   * A {@link BoundedSource} reading from Elasticsearch.
   */
  public static class BoundedElasticsearchSource extends BoundedSource<Document> {
    private final Read spec;
    @Nullable
    private final Integer numSlices;
    @Nullable
    private final Integer sliceId;
    @Nullable
    private Long estimatedByteSize;

    private ElasticsearchIndiceInfo stats;

    private int backendVersion;

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
    public List<? extends BoundedSource<Document>> split(
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
            @Nonnull RestHighLevelClientAdapter restHighLevelClient,
            @Nonnull String indexName,
            @Nonnull String queryString)
            throws IOException {
      return restHighLevelClient.count(queryString, indexName);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("numSlices", numSlices));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
    }

    @Override
    public BoundedReader<Document> createReader(PipelineOptions options) {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    // TODO
    public Coder<Document> getOutputCoder() {
      return SerializableCoder.of(Document.class);
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
      backendVersion = checkBackendVersion(connectionConfiguration);

      String query = spec.getLuceneQueryString();

      try (RestHighLevelClientAdapter restHighLevelClient = connectionConfiguration.createClient(backendVersion)) {
        int numberOfShards = restHighLevelClient.numberOfShards(connectionConfiguration.getIndex());

        // Retrieve query document count
        long totalCount = restHighLevelClient.count(MATCH_ALL_LUCENE_QUERY_STRING, connectionConfiguration.getIndex());
        long queryCount;

        if (query == null) {
          queryCount = totalCount;
        } else {
          queryCount = restHighLevelClient.count(query, connectionConfiguration.getIndex());
        }

        // Retrieve index size
        long indexByteSize = restHighLevelClient.getIndexByteSize(connectionConfiguration.getIndex());

        stats =
                ElasticsearchIndiceInfo.builder()
                        .setShardNumber(numberOfShards)
                        .setIndexDocumentCount(totalCount)
                        .setQueryDocumentCount(queryCount)
                        .setIndexByteSize(indexByteSize)
                        .build();
      } catch (Exception e) {
        // TODO
      }
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<Document> {

    private final BoundedElasticsearchSource source;

    private RestHighLevelClientAdapter restClient;
    private SearchHitAdapter current;
    private String scrollId;
    private ListIterator<? extends SearchHitAdapter> batchIterator;

    private BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient(source.backendVersion);
      String query = source.spec.getLuceneQueryString();

      if (query == null) {
        query = MATCH_ALL_LUCENE_QUERY_STRING;
      }

      SearchResponseAdapter response = restClient.search(query,
              source.spec.getBatchSize(),
              source.sliceId,
              source.numSlices,
              source.spec.getScrollKeepalive());

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
          SearchResponseAdapter response = restClient.scroll(scrollId, source.spec.getScrollKeepalive());
          setScrollId(response);

          return readNextBatchAndReturnFirstDocument(response);
        }
      }
    }

    private void setScrollId(SearchResponseAdapter response) {
      scrollId = response.getScrollId();
    }

    private boolean readNextBatchAndReturnFirstDocument(SearchResponseAdapter searchResult) {
      // stop if no more data
      batchIterator = searchResult.iterator();
      if (!batchIterator.hasNext()) {
        current = null;
        batchIterator = null;

        return false;
      }

      current = batchIterator.next();

      return true;
    }

    @Override
    public Document getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }

      return Document.of(current);
    }

    @Override
    public void close() throws IOException {
      // remove the scroll (best effort)
      try {
        if (scrollId != null) {
          restClient.clearScroll(scrollId);
        }
      } catch (Exception ex) {
        LOG.warn("It seems that the scroll object has been already freed: ", ex);
      } finally {
        if (restClient != null) {
          try {
            restClient.close();
          } catch (Exception e) {
            LOG.error("Cannot close Elasticsearch client", e);
          }
        }
      }
    }

    @Override
    public BoundedSource<Document> getCurrentSource() {
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
  public abstract static class Write
          extends PTransform<PCollection<WriteRequest>, WriteResult> {
    private static final String FAILED_INDEXING_TAG_ID = "failedIndexing";
    private static final String MAIN_INDEXING_TAG_ID = "successIndexing";

    public abstract ConnectionConfiguration getConnectionConfiguration();

    public abstract int getMaxBatchSize();

    public abstract int getMaxBatchSizeBytes();

    @Nullable
    public abstract RetryConfiguration getRetryConfiguration();

    public static Builder builder() {
      return new AutoValue_Elasticsearch7IO_Write.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConnectionConfiguration(
              ConnectionConfiguration connectionConfiguration);

      public abstract Builder setMaxBatchSize(int maxBatchSize);

      public abstract Builder setMaxBatchSizeBytes(int maxBatchSizeBytes);

      public abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      public abstract Write build();
    }

    @Override
    public WriteResult expand(PCollection<WriteRequest> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");

      TupleTag<BulkItemResponseContainer> failedIndexingTag =
              new TupleTag<BulkItemResponseContainer>(FAILED_INDEXING_TAG_ID) {
              };
      TupleTag<BulkItemResponseContainer> successfulIndexingTag =
              new TupleTag<BulkItemResponseContainer>(MAIN_INDEXING_TAG_ID) {
              };

      PCollectionTuple tuple =
              input.apply(
                      ParDo.of(new WriteFn(this, successfulIndexingTag, failedIndexingTag))
                              .withOutputTags(successfulIndexingTag, TupleTagList.of(failedIndexingTag)));

      return WriteResult.in(
              input.getPipeline(),
              failedIndexingTag,
              tuple.get(failedIndexingTag).setCoder(BulkItemResponseContainerCoder.of()),
              successfulIndexingTag,
              tuple.get(successfulIndexingTag).setCoder(BulkItemResponseContainerCoder.of()));
    }

    static class WriteFn
            extends DoFn<WriteRequest, BulkItemResponseContainer> {
      private final Write spec;
      private final TupleTag<BulkItemResponseContainer> successfulIndexingTag;
      private final TupleTag<BulkItemResponseContainer> failedIndexingTag;
      private transient RestHighLevelClientAdapter restClient;
      private int backendVersion;

      private LinkedHashMap<String, SerializableValueInSingleWindow> batch;

      private int currentBatchSizeBytes;
      private String currentBatchId;
      private transient FluentBackoff retryBackoff;

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);

      static final String RETRY_ATTEMPT_LOG = "Error writing to Elasticsearch. Retry attempt[{}}].";

      static final String RETRY_FAILED_LOG =
              "Error writing to ES after {} attempt(s). No more attempts allowed.";

      WriteFn(
              Write spec,
              TupleTag<BulkItemResponseContainer> successfulIndexingTag,
              TupleTag<BulkItemResponseContainer> failedIndexingTag) {
        this.spec = spec;
        this.successfulIndexingTag = successfulIndexingTag;
        this.failedIndexingTag = failedIndexingTag;
      }

      @Setup
      @SuppressWarnings("unused")
      public void setup() throws IOException {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
        backendVersion = checkBackendVersion(connectionConfiguration);
        restClient = connectionConfiguration.createClient(backendVersion);

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
        batch = new LinkedHashMap<>(spec.getMaxBatchSize());
        currentBatchSizeBytes = 0;
        currentBatchId = generateId();
      }

      @ProcessElement
      @SuppressWarnings("unused")
      public void processElement(
              @Element WriteRequest element,
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
        currentBatchSizeBytes += restClient.size(element);

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

      private BulkItemResponseContainer buildBulkItemResponseContainerWith(
              BulkItemResponseAdapter bulkItemResponse) {
        return BulkItemResponseContainer.builder()
                .setBulkResponse(BulkItemReponse.of(AdapterFactory.bulkResponseAdapter(backendVersion, bulkItemResponse)))
                .setWriteRequest(batch.get(bulkItemResponse.getId()).getValue())
                .setBatchId(currentBatchId)
                .build();
      }

      @Teardown
      @SuppressWarnings("unused")
      public void closeClient() throws IOException {
        if (restClient != null) {
          try {
            restClient.close();
          } catch (Exception e) {
            LOG.error("Cannot close Elasticsearch client", e);
          }
        }
      }

      private void flushBatch(
              BiConsumer<ValueInSingleWindow<WriteRequest>, BulkItemResponseAdapter> successfulOutput,
              BiConsumer<ValueInSingleWindow<WriteRequest>, BulkItemResponseAdapter> failureOutput)
              throws IOException {
        if (batch.isEmpty()) {
          return;
        }

        List<WriteRequest> request = buildBulkRequest();
        List<BulkItemResponseAdapter> response = restClient.bulk(request);

        response.stream()
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


        // Re-init for the next batch
        batch.clear();
        currentBatchSizeBytes = 0;
        currentBatchId = generateId();
      }

      private void handleRetry(
              BiConsumer<ValueInSingleWindow<WriteRequest>, BulkItemResponseAdapter> successfulOutput,
              BiConsumer<ValueInSingleWindow<WriteRequest>, BulkItemResponseAdapter> failureOutput,
              ValueInSingleWindow<WriteRequest> indexRequestValueInSingleWindow,
              BulkItemResponseAdapter bulkItemResponse) {
        RetryConfiguration retryConfiguration = spec.getRetryConfiguration();
        if (retryConfiguration != null
                && retryConfiguration
                .getRetryPredicate()
                .test(bulkItemResponse.getFailureStatus())) {
          Sleeper sleeper = Sleeper.DEFAULT;
          BackOff backoff = retryBackoff.backoff();
          int attempt = 0;

          BulkItemResponseAdapter retriedBulkItemResponse = bulkItemResponse;

          // while retry policy exists
          try {
            WriteRequest writeRequest = indexRequestValueInSingleWindow.getValue();

            if (writeRequest == null) {
              LOG.error("Cannot retrieve Elasticsearch WriteRequest. No retry will be done.");
            } else {
              while (retriedBulkItemResponse.isFailed() && BackOffUtils.next(sleeper, backoff)) {
                LOG.warn(RETRY_ATTEMPT_LOG, ++attempt);

                List<BulkItemResponseAdapter> retriedBulkResponse =
                        restClient.bulk(Collections.singletonList(writeRequest));

                // We know that we only have one response
                retriedBulkItemResponse = retriedBulkResponse.get(0);
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

      private List<WriteRequest> buildBulkRequest() {
        return batch.values().stream()
                .map(ValueInSingleWindow::getValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
      }
    }
  }

  /**
   * TODO: change the comment
   * This IO is (currently) only compliant with Elasticsearch 7.x.
   *
   * @param connectionConfiguration given Elasticsearch connection configuration
   */
  private static int checkBackendVersion(ConnectionConfiguration connectionConfiguration) {
    // Try to get the version with ES 7 implementation
    try (RestHighLevelClientAdapter restHighLevelClient = connectionConfiguration.createClient(7)) {
      int backendVersion = restHighLevelClient.backendVersion();

      checkArgument(
              (backendVersion == 7),
              "The Elasticsearch version to connect to is %s.x. "
                      + "This version of the Elasticsearch7IO is only compatible with "
                      + "Elasticsearch v7.x",
              backendVersion);

      return backendVersion;
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot get Elasticsearch version", e);
    }
  }

  private static String generateId() {
    return UUIDs.base64UUID();
  }
}
