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
import java.util.Objects;
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

  private T value;
  private Instant timestamp;
  private BoundedWindow window;
  private PaneInfo pane;

  private SerializableValueInSingleWindow(
      T value, Instant timestamp, BoundedWindow window, PaneInfo pane) {
    this.value = value;
    this.timestamp = timestamp;
    this.window = window;
    this.pane = pane;
  }

  public static <T extends Writeable & DocWriteRequest<T>> SerializableValueInSingleWindow<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return new SerializableValueInSingleWindow<>(value, timestamp, window, paneInfo);
  }

  @Nullable
  @Override
  public T getValue() {
    return value;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public BoundedWindow getWindow() {
    return window;
  }

  @Override
  public PaneInfo getPane() {
    return pane;
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    Class<? extends BoundedWindow> windowClass = window.getClass();
    oos.writeObject(windowClass);
    getCoder(windowClass).encode(this, oos);
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    Class<? extends BoundedWindow> windowClass = (Class<? extends BoundedWindow>) ois.readObject();
    ValueInSingleWindow<T> decoded = getCoder(windowClass).decode(ois);

    this.value = decoded.getValue();
    this.timestamp = decoded.getTimestamp();
    this.window = decoded.getWindow();
    this.pane = decoded.getPane();
  }

  private ValueInSingleWindow.Coder<T> getCoder(Class<? extends BoundedWindow> theClass) {
    org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> windowCoder =
        getWindowCoder(theClass);
    org.apache.beam.sdk.coders.Coder<T> valueCoder = WriteableCoder.of();

    return Coder.of(valueCoder, windowCoder);
  }

  /**
   * Test window type and return the {@link Coder}.
   *
   * <p>Two options actually exist: {@link GlobalWindow} or {@link IntervalWindow} coders.
   *
   * @param theType the window type to be encoded
   * @return the {@link Coder}
   */
  private static org.apache.beam.sdk.coders.Coder<? extends BoundedWindow> getWindowCoder(
      Class<? extends BoundedWindow> theType) {
    if (theType.equals(GlobalWindow.class)) {
      return GlobalWindow.Coder.INSTANCE;
    } else if (theType.equals(IntervalWindow.class)) {
      return IntervalWindow.getCoder();
    } else {
      throw new UnsupportedOperationException("Unknown window type.");
    }
  }

  /**
   * Elasticsearch requests do not define hashCode and equals functions. We use their string
   * representation for now in order to compare them.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializableValueInSingleWindow<?> that = (SerializableValueInSingleWindow<?>) o;
    return Objects.equals(value.toString(), that.value.toString())
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(window, that.window)
        && Objects.equals(pane, that.pane);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value.toString(), timestamp, window, pane);
  }
}
