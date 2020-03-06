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

import java.util.Map;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.action.DocWriteRequest;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.Writeable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Main {@link org.apache.beam.sdk.io.elasticsearch.Elasticsearch7IO.Write} operation result. It
 * contains two outputs for successful and failed indexing.
 *
 * <p>An example of usage can be found in the tests.
 *
 * <p>It returns two {@link PCollection} of {@link BulkItemResponseContainer}.
 *
 * <p>You can check the unit test class:
 * org.apache.beam.sdk.io.elasticsearch.utils.Elasticsearch7IOWritingTestRunner#run()
 */
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<BulkItemResponseContainer> failedIndexingTag;
  private final PCollection<BulkItemResponseContainer> failedIndexing;
  private final TupleTag<BulkItemResponseContainer> successfulIndexingTag;
  private final PCollection<BulkItemResponseContainer> successfulIndexing;

  private WriteResult(
      Pipeline pipeline,
      TupleTag<BulkItemResponseContainer> failedIndexingTag,
      PCollection<BulkItemResponseContainer> failedIndexing,
      TupleTag<BulkItemResponseContainer> successfulIndexingTag,
      PCollection<BulkItemResponseContainer> successfulIndexing) {
    this.pipeline = pipeline;
    this.failedIndexingTag = failedIndexingTag;
    this.failedIndexing = failedIndexing;
    this.successfulIndexingTag = successfulIndexingTag;
    this.successfulIndexing = successfulIndexing;
  }

  static WriteResult in(
      Pipeline pipeline,
      TupleTag<BulkItemResponseContainer> failedIndexingTag,
      PCollection<BulkItemResponseContainer> failedIndexing,
      TupleTag<BulkItemResponseContainer> successfulIndexingTag,
      PCollection<BulkItemResponseContainer> successfulIndexing) {
    return new WriteResult(
        pipeline, failedIndexingTag, failedIndexing, successfulIndexingTag, successfulIndexing);
  }

  public PCollection<BulkItemResponseContainer> getFailedIndexing() {
    return failedIndexing;
  }

  public PCollection<BulkItemResponseContainer> getSuccessfulIndexing() {
    return successfulIndexing;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        failedIndexingTag, failedIndexing, successfulIndexingTag, successfulIndexing);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}