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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.StreamInput;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.Writeable;
import org.apache.beam.sdk.coders.Coder;

/**
 * A {@link
 * org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.Writeable}
 * object Coder.
 */
public class WriteableCoder<T extends Writeable> extends Coder<T> {

  private WriteableCoder() {}

  public static <T extends Writeable> WriteableCoder<T> of() {
    return new WriteableCoder<>();
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    OutputStreamStreamOutput streamOutput = new OutputStreamStreamOutput(outStream);
    streamOutput.writeString(value.getClass().getName());
    value.writeTo(streamOutput);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T decode(InputStream inStream) throws IOException {
    InputStreamStreamInput streamInput = new InputStreamStreamInput(inStream);
    String className = streamInput.readString();

    try {
      return (T)
          Class.forName(className).getConstructor(StreamInput.class).newInstance(streamInput);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() {}

  /**
   * Flush the request into a byte array in order to retrieve its size. TODO: Isn't it a bit
   * overkill ?
   *
   * @param element the request
   * @return the byte size of the request.
   */
  public int getSize(T element) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    encode(element, outputStream);

    return outputStream.size();
  }
}
