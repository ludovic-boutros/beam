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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;

/**
 * A simple {@link Coder} test base class.
 *
 * @param <T> The type of elements that the tested coder is able to encode/decode.
 */
public abstract class CoderTestBase<T> {
  private final Coder<T> coder = provideCoder();

  protected abstract Coder<T> provideCoder();

  protected abstract T buildElement();

  protected abstract void checkCodeDecodeAsserts(T decodedElement);

  protected abstract T buildElementWithError();

  protected abstract void checkCodeDecodeWithErrorAsserts(T decodedElement);

  @Test
  public void codeDecodeTest() throws IOException {
    T element = buildElement();

    if (element != null) {
      T decodedElement = encodeDecodeElement(element);

      checkCodeDecodeAsserts(decodedElement);
    }
  }

  @Test
  public void codeDecodeWithErrorTest() throws IOException {
    T element = buildElementWithError();

    if (element != null) {
      T decodedElement = encodeDecodeElement(element);

      checkCodeDecodeWithErrorAsserts(decodedElement);
    }
  }

  private T encodeDecodeElement(T element) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    OutputStreamStreamOutput output = new OutputStreamStreamOutput(bos);

    coder.encode(element, output);

    InputStreamStreamInput input =
        new InputStreamStreamInput(new ByteArrayInputStream(bos.toByteArray()));

    return coder.decode(input);
  }
}
