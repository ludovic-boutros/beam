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
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.ES_PASSWORD;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.buildRestHighLevelClient;
import static org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils.checkHealth;
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
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.client.RestHighLevelClient;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.index.query.QueryBuilders;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOReadingTestRunner;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils;
import org.apache.beam.sdk.io.elasticsearch.utils.ElasticsearchWithSSLContainer;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link Elasticsearch7IO} reading functions. */
@RunWith(JUnit4.class)
public class Elasticsearch7IOReadingTest implements Serializable {
  private static final int COUNT = 100;
  private static final int BATCH_SIZE = 7;

  private static RestHighLevelClient restHighLevelClient;
  private static ElasticsearchWithSSLContainer container;
  private ConnectionConfiguration connectionConfiguration;
  private static final String ES_INDEX_NAME = "read_test_index";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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
    if (restHighLevelClient != null) {
      restHighLevelClient.close();
    }
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
            .setIndex(ES_INDEX_NAME)
            .setUsername(ELASTICSEARCH_DEFAULT_USER)
            .setPassword(ES_PASSWORD)
            .setKeystorePath(TRUST_AND_KEY_STORE)
            .setKeystorePassword(TRUST_AND_KEY_STORE_PASSWORD)
            .build();

    deleteIndex(restHighLevelClient, ES_INDEX_NAME);
  }

  @Test
  public void readingTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, COUNT);

    Elasticsearch7IOReadingTestRunner.builder()
        .withPipeline(pipeline)
        .withRead(
            Elasticsearch7IO.read()
                .setConnectionConfiguration(connectionConfiguration)
                .setBatchSize(BATCH_SIZE)
                .build())
        .assertDocumentCount(COUNT)
        .build()
        .run();
  }

  @Test
  public void readingMultiShardTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 5);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, COUNT);

    Elasticsearch7IOReadingTestRunner.builder()
        .withPipeline(pipeline)
        .withRead(
            Elasticsearch7IO.read()
                .setConnectionConfiguration(connectionConfiguration)
                .setBatchSize(BATCH_SIZE)
                .build())
        .assertDocumentCount(COUNT)
        .build()
        .run();
  }

  @Test
  public void readingEmptyIndexTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, 0);

    Elasticsearch7IOReadingTestRunner.builder()
        .withPipeline(pipeline)
        .withRead(
            Elasticsearch7IO.read()
                .setConnectionConfiguration(connectionConfiguration)
                .setBatchSize(BATCH_SIZE)
                .build())
        .assertDocumentCount(0)
        .build()
        .run();
  }

  @Test
  public void readingWithQueryTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, 100);

    Elasticsearch7IOReadingTestRunner.builder()
        .withPipeline(pipeline)
        .withRead(
            Elasticsearch7IO.Read.withQuery(QueryBuilders.idsQuery().addIds("0", "1", "2"))
                .setConnectionConfiguration(connectionConfiguration)
                .setBatchSize(1)
                .build())
        .assertDocumentCount(3)
        .build()
        .run();
  }

  @Test
  public void readingWithNullQueryTest() throws IOException {
    Elasticsearch7IOTestUtils.createIndex(restHighLevelClient, ES_INDEX_NAME, 1);
    Elasticsearch7IOTestUtils.injectDocuments(restHighLevelClient, ES_INDEX_NAME, 100);

    Elasticsearch7IOReadingTestRunner.builder()
        .withPipeline(pipeline)
        .withRead(
            Elasticsearch7IO.Read.withQuery(null)
                .setConnectionConfiguration(connectionConfiguration)
                .setBatchSize(1)
                .build())
        .assertDocumentCount(100)
        .build()
        .run();
  }
}
