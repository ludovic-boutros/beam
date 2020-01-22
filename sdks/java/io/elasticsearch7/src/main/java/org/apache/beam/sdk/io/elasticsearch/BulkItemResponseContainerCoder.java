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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * A {@link BulkItemResponseContainer} Coder which uses the Elasticsearch {@link
 * org.elasticsearch.common.io.stream.Writeable} interface in order to serialize Elasticsearch
 * objects objects.
 */
public class BulkItemResponseContainerCoder<T extends Writeable & DocWriteRequest<T>>
    extends Coder<BulkItemResponseContainer<T>> {
  private final BulkItemResponseCoder bulkItemResponseCoder = BulkItemResponseCoder.of();
  private final WriteableCoder<T> writeableCoder = WriteableCoder.of();
  private final StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();

  private BulkItemResponseContainerCoder() {}

  public static <T extends Writeable & DocWriteRequest<T>> BulkItemResponseContainerCoder<T> of() {
    return new BulkItemResponseContainerCoder<>();
  }

  @Override
  public void encode(BulkItemResponseContainer<T> value, OutputStream outStream)
      throws IOException {
    stringUtf8Coder.encode(value.getBatchId(), outStream);
    bulkItemResponseCoder.encode(value.getBulkItemResponse(), outStream);
    writeableCoder.encode(value.getWriteRequest(), outStream);
  }

  @Override
  public BulkItemResponseContainer<T> decode(InputStream inStream) throws IOException {
    BulkItemResponseContainer.Builder<T> builder = BulkItemResponseContainer.builder();

    return builder
        .setBatchId(stringUtf8Coder.decode(inStream))
        .setBulkItemResponse(bulkItemResponseCoder.decode(inStream))
        .setWriteRequest(writeableCoder.decode(inStream))
        .build();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() {}
}
