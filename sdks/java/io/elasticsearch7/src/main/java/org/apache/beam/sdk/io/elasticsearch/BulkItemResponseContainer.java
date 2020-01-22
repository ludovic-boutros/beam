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

import com.google.auto.value.AutoValue;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * An Elasticsearch response high level container which contains: - the original index request ; -
 * the Elasticsearch response ; - the batch id.
 *
 * <p>It is mainly used in case of indexing errors in order to do some additional retry or other
 * business process.
 */
@AutoValue
public abstract class BulkItemResponseContainer<T extends Writeable & DocWriteRequest<T>> {
  public abstract String getBatchId();

  public abstract T getWriteRequest();

  public abstract BulkItemResponse getBulkItemResponse();

  public static <T extends Writeable & DocWriteRequest<T>> Builder<T> builder() {
    return new AutoValue_BulkItemResponseContainer.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<T extends Writeable & DocWriteRequest<T>> {
    public abstract Builder<T> setBatchId(String withId);

    public abstract Builder<T> setWriteRequest(T withIndexRequest);

    public abstract Builder<T> setBulkItemResponse(BulkItemResponse withBulkItemResponse);

    public abstract BulkItemResponseContainer<T> build();
  }
}
