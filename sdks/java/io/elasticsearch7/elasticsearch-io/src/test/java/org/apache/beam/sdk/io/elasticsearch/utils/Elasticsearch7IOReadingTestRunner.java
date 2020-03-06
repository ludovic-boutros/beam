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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.search.SearchHit;
import org.apache.beam.sdk.io.elasticsearch.Elasticsearch7IO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;

public class Elasticsearch7IOReadingTestRunner implements Serializable {

  private final transient TestPipeline pipeline;
  private final Elasticsearch7IO.Read read;
  private final int expectedDocumentCount;

  private Elasticsearch7IOReadingTestRunner(
      TestPipeline pipeline, Elasticsearch7IO.Read read, int expectedDocumentCount) {
    this.pipeline = pipeline;
    this.read = read;
    this.expectedDocumentCount = expectedDocumentCount;
  }

  private TestPipeline getPipeline() {
    return pipeline;
  }

  private Elasticsearch7IO.Read getRead() {
    return read;
  }

  private int getExpectedDocumentCount() {
    return expectedDocumentCount;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private transient TestPipeline pipeline;
    private Elasticsearch7IO.Read read;
    private int documentCount;

    private Builder() {}

    public Builder withPipeline(TestPipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    public Builder withRead(Elasticsearch7IO.Read read) {
      this.read = read;
      return this;
    }

    public Builder assertDocumentCount(int documentCount) {
      this.documentCount = documentCount;
      return this;
    }

    public Elasticsearch7IOReadingTestRunner build() {
      return new Elasticsearch7IOReadingTestRunner(pipeline, read, documentCount);
    }
  }

  public void run() {
    PCollection<SearchHit> documents =
        getPipeline().apply("Read documents from Elasticsearch", getRead());

    PAssert.that(
            "Document list should contain " + getExpectedDocumentCount() + " documents", documents)
        .satisfies(
            (SerializableFunction<Iterable<SearchHit>, Void>)
                element -> {
                  Set<String> ids =
                      Streams.stream(element).map(SearchHit::getId).collect(Collectors.toSet());

                  assertThat(ids.size()).isEqualTo(getExpectedDocumentCount());

                  return null;
                });

    // Run the pipeline
    getPipeline().run().waitUntilFinish();
  }
}
