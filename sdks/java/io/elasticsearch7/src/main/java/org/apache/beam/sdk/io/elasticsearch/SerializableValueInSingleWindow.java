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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.Writeable;
import org.joda.time.Instant;

/**
 * This is a serializable version of {@link ValueInSingleWindow} for {@link Writeable} objects.
 *
 * <p>It uses specific {@link BoundedWindow} {@link Coder} mechanisms in order to serialize the
 * bounded window.
 *
 * @param <T> the type of the value
 */
public class SerializableValueInSingleWindow<T extends Writeable & DocWriteRequest<T>>
    extends ValueInSingleWindow<T> implements Serializable {

  private ValueInSingleWindow<T> window;

  private SerializableValueInSingleWindow(ValueInSingleWindow<T> window) {
    this.window = window;
  }

  public static <T extends Writeable & DocWriteRequest<T>> SerializableValueInSingleWindow<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return of(ValueInSingleWindow.of(value, timestamp, window, paneInfo));
  }

  public static <T extends Writeable & DocWriteRequest<T>> SerializableValueInSingleWindow<T> of(
      ValueInSingleWindow<T> delegate) {
    return new SerializableValueInSingleWindow<>(delegate);
  }

  @Nullable
  @Override
  public T getValue() {
    return window.getValue();
  }

  @Override
  public Instant getTimestamp() {
    return window.getTimestamp();
  }

  @Override
  public BoundedWindow getWindow() {
    return window.getWindow();
  }

  @Override
  public PaneInfo getPane() {
    return window.getPane();
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    getCoder().encode(window, oos);
  }

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    window = getCoder().decode(ois);
  }

  private ValueInSingleWindow.Coder<T> getCoder() {
    org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder =
        getWindowCoder(getWindow());
    org.apache.beam.sdk.coders.Coder<T> valueCoder = WriteableCoder.of();

    return Coder.of(valueCoder, windowCoder);
  }

  /**
   * Test window type and return the {@link Coder}.
   *
   * <p>Two options actually exist: {@link GlobalWindow} or {@link IntervalWindow} coders.
   *
   * @param window the window to be encoded
   * @return the {@link Coder}
   */
  private static org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> getWindowCoder(
      BoundedWindow window) {
    if (window instanceof GlobalWindow) {
      return GlobalWindow.Coder.INSTANCE;
    } else {
      return IntervalWindow.getCoder();
    }
  }
}
