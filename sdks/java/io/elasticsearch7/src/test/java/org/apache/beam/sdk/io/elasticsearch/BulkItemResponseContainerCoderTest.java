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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOTestUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Inner coders are already tested. Just test limits. */
@RunWith(JUnit4.class)
public class BulkItemResponseContainerCoderTest {
  private final Coder<BulkItemResponseContainer<IndexRequest>> coder =
      BulkItemResponseContainerCoder.of();

  @Test
  public void nullResponseArgumentsTest() {
    IndexRequest request = Elasticsearch7IOTestUtils.buildIndexRequest();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    OutputStreamStreamOutput output = new OutputStreamStreamOutput(bos);

    BulkItemResponseContainer.Builder<IndexRequest> builder = BulkItemResponseContainer.builder();
    assertThatThrownBy(
            () ->
                coder.encode(
                    builder.setBulkItemResponse(null).setWriteRequest(request).build(), output))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void nullRequestArgumentsTest() {
    BulkItemResponse response = Elasticsearch7IOTestUtils.buildBulkItemResponse();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    OutputStreamStreamOutput output = new OutputStreamStreamOutput(bos);

    BulkItemResponseContainer.Builder<IndexRequest> builder = BulkItemResponseContainer.builder();
    assertThatThrownBy(
            () ->
                coder.encode(
                    builder.setBulkItemResponse(response).setWriteRequest(null).build(), output))
        .isInstanceOf(NullPointerException.class);
  }
}
