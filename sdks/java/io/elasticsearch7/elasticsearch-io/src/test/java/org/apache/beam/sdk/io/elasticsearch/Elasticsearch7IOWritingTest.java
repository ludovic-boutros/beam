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

import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.ELASTICSEARCH_DEFAULT_PORT;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.ELASTICSEARCH_VERSION;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.ESTestDocument;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.ES_PASSWORD;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.JsonToESIndexRequest;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.JsonToESVersionedIndexRequest;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.StringToDeleteRequest;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.StringToUpdateRequest;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.assertDocumentIsUpdated;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.buildRestHighLevelClient;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.checkHealth;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.computeDocumentAverageSize;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.deleteIndex;
import static org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer.ELASTICSEARCH_DEFAULT_USER;
import static org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer.TRUST_AND_KEY_STORE;
import static org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer.TRUST_AND_KEY_STORE_PASSWORD;

import java.io.IOException;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.index.IndexRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.update.UpdateRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestHighLevelClient;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOWritingTestRunner;
import org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link Elasticsearch7IO} writing functions. */
@RunWith(JUnit4.class)
public class Elasticsearch7IOWritingTest implements Serializable {

  private static final int COUNT = 100;
  // just a prime number
  private static final int MAX_BATCH_SIZE = 7;

  private static RestHighLevelClient restHighLevelClient;
  private static ElasticsearchWithSSLContainer container;
  private ConnectionConfiguration connectionConfiguration;
  private static final String ES_INDEX_NAME = "write_test_index";
  private static final int DOCUMENT_AVERAGE_SIZE = computeDocumentAverageSize(COUNT, ES_INDEX_NAME);
  private static final int MAX_BATCH_SIZE_BYTE = MAX_BATCH_SIZE * DOCUMENT_AVERAGE_SIZE;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient TestPipeline secondPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass()
      throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException,
          CertificateException {
    container =
        new ElasticsearchWithSSLContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:" + ELASTICSEARCH_VERSION, ES_PASSWORD);
    container.start();

    restHighLevelClient = buildRestHighLevelClient(container);

    checkHealth(restHighLevelClient);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    restHighLevelClient.close();
    container.stop();
  }

  @Before
  public void setUp() throws IOException {
    connectionConfiguration =
        ConnectionConfiguration.builder()
            .setAddresses(
                Collections.singletonList(
                    "https://"
                        + container.getContainerIpAddress()
                        + ":"
                        + container.getMappedPort(ELASTICSEARCH_DEFAULT_PORT)))
            .setUsername(ELASTICSEARCH_DEFAULT_USER)
            .setPassword(ES_PASSWORD)
            .setKeystorePath(TRUST_AND_KEY_STORE)
            .setKeystorePassword(TRUST_AND_KEY_STORE_PASSWORD)
            .build();

    deleteIndex(restHighLevelClient, ES_INDEX_NAME);
  }

  @After
  public void tearDown() {}

  @Test
  public void writingTest() throws IOException {
    Elasticsearch7IO.Write.Builder<IndexRequest> builder = Elasticsearch7IO.write();

    generateDefaultTestRunnerBuilder(JsonToESIndexRequest.of(ES_INDEX_NAME, true))
        .withWrite(
            builder
                .setConnectionConfiguration(connectionConfiguration)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .build())
        .withDocumentCount(COUNT)
        .assertFailedDocumentCount(0)
        .assertSuccessDocumentCount(COUNT)
        .assertFinalESDocumentCount(COUNT)
        .assertOrdered(true)
        .build()
        .run();
  }

  @Test
  public void writingWithMaxBatchSizeByteTest() throws IOException {
    Elasticsearch7IO.Write.Builder<IndexRequest> builder = Elasticsearch7IO.write();

    generateDefaultTestRunnerBuilder(JsonToESIndexRequest.of(ES_INDEX_NAME))
        .withWrite(
            builder
                .setConnectionConfiguration(connectionConfiguration)
                .setMaxBatchSize(COUNT)
                .setMaxBatchSizeBytes(MAX_BATCH_SIZE_BYTE)
                .build())
        .withDocumentCount(COUNT)
        .assertFailedDocumentCount(0)
        .assertSuccessDocumentCount(COUNT)
        .assertFinalESDocumentCount(COUNT)
        .build()
        .run();
  }

  @Test
  public void writingRetryTest() throws IOException {
    RetryConfiguration retryConfiguration =
        RetryConfiguration.builder()
            .setRetryPredicate(RetryConfiguration.defaultRetryPredicate(409))
            .setMaxAttempts(5)
            .setMaxDuration(Duration.millis(5000))
            .build();

    Elasticsearch7IO.Write.Builder<IndexRequest> writeBuilder = Elasticsearch7IO.write();

    generateDefaultTestRunnerBuilder(JsonToESVersionedIndexRequest.of(ES_INDEX_NAME, 1L))
        .withWrite(
            writeBuilder
                .setRetryConfiguration(RetryConfiguration.DEFAULT)
                .setConnectionConfiguration(connectionConfiguration)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .build())
        .withDocumentCount(1)
        .assertFailedDocumentCount(0)
        .assertSuccessDocumentCount(1)
        .assertFinalESDocumentCount(1)
        .build()
        .run();

    // Each document indexing should throw an exception (version conflict)
    writeBuilder = Elasticsearch7IO.write();

    generateDefaultTestRunnerBuilder(
            secondPipeline, JsonToESVersionedIndexRequest.of(ES_INDEX_NAME, 1L))
        .withWrite(
            writeBuilder
                .setConnectionConfiguration(connectionConfiguration)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .setRetryConfiguration(retryConfiguration)
                .build())
        .withDocumentCount(1)
        .assertFailedDocumentCount(1)
        .assertSuccessDocumentCount(0)
        .assertFinalESDocumentCount(1)
        .build()
        .run();
  }

  @Test
  public void deleteTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, COUNT);

    Elasticsearch7IO.Write.Builder<DeleteRequest> writeBuilder = Elasticsearch7IO.write();

    Elasticsearch7IOWritingTestRunner.Builder<DeleteRequest, String> builder =
        Elasticsearch7IOWritingTestRunner.builder();

    builder
        .withEsIndexName(ES_INDEX_NAME)
        .withRestHighLevelClient(restHighLevelClient)
        .withWriteRequest(StringToDeleteRequest.of(ES_INDEX_NAME))
        .withDocumentAverageSize(DOCUMENT_AVERAGE_SIZE)
        .withGenerator(Integer::toUnsignedString)
        .withPipeline(pipeline)
        .withWrite(
            writeBuilder
                .setRetryConfiguration(RetryConfiguration.DEFAULT)
                .setConnectionConfiguration(connectionConfiguration)
                .setMaxBatchSize(MAX_BATCH_SIZE)
                .build())
        // deletion count
        .withDocumentCount(COUNT)
        .assertFailedDocumentCount(0)
        .assertSuccessDocumentCount(100)
        .assertFinalESDocumentCount(0)
        .build()
        .run();
  }

  @Test
  public void updateTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, 1);

    Elasticsearch7IO.Write.Builder<UpdateRequest> writeBuilder = Elasticsearch7IO.write();

    Elasticsearch7IOWritingTestRunner.Builder<UpdateRequest, String> builder =
        Elasticsearch7IOWritingTestRunner.builder();

    builder
        .withEsIndexName(ES_INDEX_NAME)
        .withRestHighLevelClient(restHighLevelClient)
        .withWriteRequest(StringToUpdateRequest.of(ES_INDEX_NAME))
        .withDocumentAverageSize(DOCUMENT_AVERAGE_SIZE)
        .withGenerator(Integer::toUnsignedString)
        .withPipeline(pipeline)
        .withWrite(
            writeBuilder
                .setRetryConfiguration(RetryConfiguration.DEFAULT)
                .setConnectionConfiguration(connectionConfiguration)
                .build())
        // update count
        .withDocumentCount(1)
        .assertFailedDocumentCount(0)
        .assertSuccessDocumentCount(1)
        .assertFinalESDocumentCount(1)
        .build()
        .run();

    assertDocumentIsUpdated(restHighLevelClient, ES_INDEX_NAME, "0");
  }

  private Elasticsearch7IOWritingTestRunner.Builder<IndexRequest, ESTestDocument>
      generateDefaultTestRunnerBuilder(
          TestPipeline pipeline,
          PTransform<PCollection<ESTestDocument>, PCollection<IndexRequest>>
              jsonToIndexResquestTransform) {
    Elasticsearch7IOWritingTestRunner.Builder<IndexRequest, ESTestDocument> builder =
        Elasticsearch7IOWritingTestRunner.builder();

    return builder
        .withEsIndexName(ES_INDEX_NAME)
        .withRestHighLevelClient(restHighLevelClient)
        .withWriteRequest(jsonToIndexResquestTransform)
        .withDocumentAverageSize(DOCUMENT_AVERAGE_SIZE)
        .withGenerator(ESTestDocument::of)
        .withPipeline(pipeline);
  }

  private Elasticsearch7IOWritingTestRunner.Builder<IndexRequest, ESTestDocument>
      generateDefaultTestRunnerBuilder(
          PTransform<PCollection<ESTestDocument>, PCollection<IndexRequest>>
              jsonToIndexResquestTransform) {
    return generateDefaultTestRunnerBuilder(pipeline, jsonToIndexResquestTransform);
  }
}
