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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.index.IndexRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Simple index request coder test. The goal of this test is not to test the Elasticsearch own code.
 * We just want to make sure that at a higher level, it works.
 */
@RunWith(JUnit4.class)
public class WriteableCoderTest extends CoderTestBase<IndexRequest> {
  @Override
  protected Coder<IndexRequest> provideCoder() {
    return WriteableCoder.of();
  }

  @Override
  protected IndexRequest buildElement() {
    return Elasticsearch7IOTestUtils.buildIndexRequest();
  }

  @Override
  protected void checkCodeDecodeAsserts(IndexRequest decodedElement) {
    assertThat(decodedElement.toString()).isEqualTo(buildElement().toString());
  }

  @Override
  protected IndexRequest buildElementWithError() {
    return null;
  }

  @Override
  protected void checkCodeDecodeWithErrorAsserts(IndexRequest decodedElement) {}

  @Test
  public void testSize() throws IOException {
    WriteableCoder<IndexRequest> coder = WriteableCoder.of();
    IndexRequest indexRequest = Elasticsearch7IOTestUtils.buildIndexRequest();
    int size = coder.getSize(indexRequest);

    assertThat(size).isEqualTo(160);
  }
}
