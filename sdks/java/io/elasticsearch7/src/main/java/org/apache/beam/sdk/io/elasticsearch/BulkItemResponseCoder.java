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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

/**
 * A {@link BulkItemResponse} Coder which uses the Elasticsearch {@link
 * org.elasticsearch.common.io.stream.Writeable} interface in order to serialize the objects.
 */
public class BulkItemResponseCoder extends Coder<BulkItemResponse> {
  private BulkItemResponseCoder() {}

  public static BulkItemResponseCoder of() {
    return new BulkItemResponseCoder();
  }

  @Override
  public void encode(BulkItemResponse value, OutputStream outStream) throws IOException {
    OutputStreamStreamOutput streamOutput = new OutputStreamStreamOutput(outStream);
    // We need to serialize the full bulk response because BulkItemResponse constructor from input
    // stream is private
    BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[] {value}, 0);
    bulkResponse.writeTo(streamOutput);
  }

  @Override
  public BulkItemResponse decode(InputStream inStream) throws IOException {
    InputStreamStreamInput streamInput = new InputStreamStreamInput(inStream);
    BulkResponse bulkResponse = new BulkResponse(streamInput);
    return bulkResponse.getItems()[0];
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() {}
}
