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
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * ValueProvider for {@link QueryBuilder}. It implements the serialization functions of QueryBuilder
 * objects using the {@link org.elasticsearch.common.io.stream.Writeable} interface.
 */
class QueryBuilderValueProvider implements ValueProvider<QueryBuilder> {
  @Nullable private QueryBuilder value;

  private QueryBuilderValueProvider(@Nullable QueryBuilder value) {
    this.value = value;
  }

  static QueryBuilderValueProvider of(QueryBuilder queryBuilder) {
    return new QueryBuilderValueProvider(queryBuilder);
  }

  @Override
  public QueryBuilder get() {
    return value;
  }

  @Override
  public boolean isAccessible() {
    return true;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    OutputStreamStreamOutput streamOutput = new OutputStreamStreamOutput(out);
    streamOutput.writeBoolean(value != null);
    if (value != null) {
      streamOutput.writeString(value.getClass().getName());
      value.writeTo(streamOutput);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
          InvocationTargetException, InstantiationException {
    InputStreamStreamInput streamInput = new InputStreamStreamInput(in);

    if (streamInput.readBoolean()) {
      String className = streamInput.readString();
      value =
          (QueryBuilder)
              Class.forName(className).getConstructor(StreamInput.class).newInstance(streamInput);
    } else {
      value = null;
    }
  }
}
