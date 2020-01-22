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

import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.assertBatchContainsMaxElementCount;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.assertResultContains;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.generateDocuments;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.BulkItemResponseContainer;
import org.apache.beam.sdk.io.elasticsearch.BulkItemResponseContainerCoder;
import org.apache.beam.sdk.io.elasticsearch.Elasticsearch7IO;
import org.apache.beam.sdk.io.elasticsearch.WriteResult;
import org.apache.beam.sdk.io.elasticsearch.WriteableCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Writing Test runner class.
 *
 * @param <T> the Elasticsearch Request type
 * @param <V> the processed document type
 */
public class Elasticsearch7IOWritingTestRunner<T extends Writeable & DocWriteRequest<T>, V> {
  private final TestPipeline pipeline;

  private final PTransform<PCollection<V>, PCollection<T>> writeRequest;

  private final int documentCount;

  private final int documentAverageSize;

  private final int expectedSuccessDocumentCount;

  private final int expectedFailedDocumentCount;

  private final int expectedFinalESDocumentCount;

  private final Elasticsearch7IO.Write<T> write;

  private final RestHighLevelClient restHighLevelClient;

  private final String esIndexName;

  private final Function<Integer, V> generator;

  public Function<Integer, V> getGenerator() {
    return generator;
  }

  public TestPipeline getPipeline() {
    return pipeline;
  }

  public PTransform<PCollection<V>, PCollection<T>> getWriteRequest() {
    return writeRequest;
  }

  public int getDocumentCount() {
    return documentCount;
  }

  public int getDocumentAverageSize() {
    return documentAverageSize;
  }

  public int getExpectedSuccessDocumentCount() {
    return expectedSuccessDocumentCount;
  }

  public int getExpectedFailedDocumentCount() {
    return expectedFailedDocumentCount;
  }

  public int getExpectedFinalESDocumentCount() {
    return expectedFinalESDocumentCount;
  }

  public Elasticsearch7IO.Write<T> getWrite() {
    return write;
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return restHighLevelClient;
  }

  public String getEsIndexName() {
    return esIndexName;
  }

  public Elasticsearch7IOWritingTestRunner(
      TestPipeline pipeline,
      PTransform<PCollection<V>, PCollection<T>> writeRequest,
      int documentCount,
      int documentAverageSize,
      int expectedSuccessDocumentCount,
      int expectedFailedDocumentCount,
      int expectedFinalESDocumentCount,
      Elasticsearch7IO.Write<T> write,
      RestHighLevelClient restHighLevelClient,
      String esIndexName,
      Function<Integer, V> generator) {
    this.pipeline = pipeline;
    this.writeRequest = writeRequest;
    this.documentCount = documentCount;
    this.documentAverageSize = documentAverageSize;
    this.expectedSuccessDocumentCount = expectedSuccessDocumentCount;
    this.expectedFailedDocumentCount = expectedFailedDocumentCount;
    this.expectedFinalESDocumentCount = expectedFinalESDocumentCount;
    this.write = write;
    this.restHighLevelClient = restHighLevelClient;
    this.esIndexName = esIndexName;
    this.generator = generator;
  }

  public static <T extends Writeable & DocWriteRequest<T>, V> Builder<T, V> builder() {
    return new Builder<>();
  }

  public static final class Builder<T extends Writeable & DocWriteRequest<T>, V> {
    private TestPipeline pipeline;
    private PTransform<PCollection<V>, PCollection<T>> writeRequest;
    private int documentCount;
    private int documentAverageSize;
    private int expectedSuccessDocumentCount;
    private int expectedFailedDocumentCount;
    private int expectedFinalESDocumentCount;
    private Elasticsearch7IO.Write<T> write;
    private RestHighLevelClient restHighLevelClient;
    private String esIndexName;
    private Function<Integer, V> generator;

    private Builder() {}

    public Builder<T, V> withPipeline(TestPipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    public Builder<T, V> withWriteRequest(PTransform<PCollection<V>, PCollection<T>> writeRequest) {
      this.writeRequest = writeRequest;
      return this;
    }

    public Builder<T, V> withDocumentCount(int documentCount) {
      this.documentCount = documentCount;
      return this;
    }

    public Builder<T, V> withDocumentAverageSize(int documentAverageSize) {
      this.documentAverageSize = documentAverageSize;
      return this;
    }

    public Builder<T, V> assertSuccessDocumentCount(int expectedSuccessDocumentCount) {
      this.expectedSuccessDocumentCount = expectedSuccessDocumentCount;
      return this;
    }

    public Builder<T, V> assertFailedDocumentCount(int expectedFailedDocumentCount) {
      this.expectedFailedDocumentCount = expectedFailedDocumentCount;
      return this;
    }

    public Builder<T, V> assertFinalESDocumentCount(int expectedFinalESDocumentCount) {
      this.expectedFinalESDocumentCount = expectedFinalESDocumentCount;
      return this;
    }

    public Builder<T, V> withWrite(Elasticsearch7IO.Write<T> write) {
      this.write = write;
      return this;
    }

    public Builder<T, V> withRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
      this.restHighLevelClient = restHighLevelClient;
      return this;
    }

    public Builder<T, V> withEsIndexName(String esIndexName) {
      this.esIndexName = esIndexName;
      return this;
    }

    public Builder<T, V> withGenerator(Function<Integer, V> generator) {
      this.generator = generator;
      return this;
    }

    public Elasticsearch7IOWritingTestRunner<T, V> build() {
      return new Elasticsearch7IOWritingTestRunner<>(
          pipeline,
          writeRequest,
          documentCount,
          documentAverageSize,
          expectedSuccessDocumentCount,
          expectedFailedDocumentCount,
          expectedFinalESDocumentCount,
          write,
          restHighLevelClient,
          esIndexName,
          generator);
    }
  }

  public void run() throws IOException {
    TypeDescriptor<BulkItemResponseContainer<T>> typeDescriptor =
        new TypeDescriptor<BulkItemResponseContainer<T>>() {};

    List<V> input = generateDocuments(getDocumentCount(), getGenerator());

    PCollection<V> inputDocuments =
        getPipeline().apply("Generate input documents", Create.of(input));
    PCollection<T> writeRequests =
        inputDocuments
            .apply("Map to WriteRequest", getWriteRequest())
            .setCoder(WriteableCoder.of());
    WriteResult<T> writeResult = writeRequests.apply("Send to Elasticsearch", getWrite());

    // Test result content
    assertResultContains(
        writeResult, getExpectedSuccessDocumentCount(), getExpectedFailedDocumentCount());

    // Test batching (Elasticsearch bulk requests)
    PCollectionList<BulkItemResponseContainer<T>> responseLists =
        PCollectionList.of(writeResult.getSuccessfulIndexing())
            .and(writeResult.getFailedIndexing());
    PCollection<BulkItemResponseContainer<T>> flattenResponses =
        responseLists.apply(Flatten.pCollections());
    PCollection<KV<String, Long>> perBatchResponses =
        flattenResponses
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptor.of(String.class), typeDescriptor))
                    .via((BulkItemResponseContainer<T> item) -> KV.of(item.getBatchId(), item)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BulkItemResponseContainerCoder.of()))
            .apply(Count.perKey());

    assertBatchContainsMaxElementCount(
        perBatchResponses, getWrite().getMaxBatchSize(), getExpectedBatchCount());

    // Run the pipeline
    getPipeline().run().waitUntilFinish();

    // Test the Elasticsearch index
    Elasticsearch7IOTestUtils.assertDocumentCount(
        getRestHighLevelClient(), getEsIndexName(), getExpectedFinalESDocumentCount());
  }

  private int getExpectedBatchCount() {
    int remainder = getDocumentCount() % getWrite().getMaxBatchSize() > 0 ? 1 : 0;
    return Math.max(
        (getDocumentCount() / getWrite().getMaxBatchSize() + remainder),
        ((getDocumentCount() * getDocumentAverageSize()) / getWrite().getMaxBatchSizeBytes()));
  }
}
