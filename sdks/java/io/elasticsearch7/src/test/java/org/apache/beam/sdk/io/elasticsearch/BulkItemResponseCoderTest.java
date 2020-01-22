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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Simple bulk item response coder test. The goal of this test is not to test the Elasticsearch own
 * code. We just want to make sure that at a higher level, it works
 */
@RunWith(JUnit4.class)
public class BulkItemResponseCoderTest extends CoderTestBase<BulkItemResponse> {
  @Override
  protected Coder<BulkItemResponse> provideCoder() {
    return BulkItemResponseCoder.of();
  }

  @Override
  protected BulkItemResponse buildElement() {
    return Elasticsearch7IOTestUtils.buildBulkItemResponse();
  }

  @Override
  protected BulkItemResponse buildElementWithError() {
    return Elasticsearch7IOTestUtils.buildErrorBulkItemResponse();
  }

  @Override
  protected void checkCodeDecodeWithErrorAsserts(BulkItemResponse decodedElement) {
    assertThat(decodedElement.getFailure()).isNotNull();
    assertThat(decodedElement.getId()).isEqualTo("myId");
    assertThat(decodedElement.getFailure().getCause()).isInstanceOf(IllegalStateException.class);
    assertThat(decodedElement.getFailure().getMessage())
        .isEqualTo("java.lang.IllegalStateException: Dummy error");
  }

  @Override
  protected void checkCodeDecodeAsserts(BulkItemResponse decodedElement) {
    assertThat(decodedElement.getFailure()).isNull();
    assertThat(decodedElement.getId()).isEqualTo("myId");
    assertThat(decodedElement.getResponse().getResult()).isEqualTo(DocWriteResponse.Result.CREATED);
  }
}
