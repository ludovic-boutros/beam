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

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.beam.repackaged.elasticsearch7_driver.org.elasticsearch.common.io.stream.Writeable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

/**
 * This is a serializable version of {@link ValueInSingleWindow} for {@link Writeable} objects.
 *
 * <p>It uses specific {@link BoundedWindow} {@link Coder} mechanisms in order to serialize the
 * bounded window.
 *
 */
public class SerializableValueInSingleWindow
    extends ValueInSingleWindow<WriteRequest> implements Serializable {

  private WriteRequest value;
  private Instant timestamp;
  private BoundedWindow window;
  private PaneInfo pane;

  private SerializableValueInSingleWindow(
      WriteRequest value, Instant timestamp, BoundedWindow window, PaneInfo pane) {
    this.value = value;
    this.timestamp = timestamp;
    this.window = window;
    this.pane = pane;
  }

  public static SerializableValueInSingleWindow of(
      WriteRequest value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return new SerializableValueInSingleWindow(value, timestamp, window, paneInfo);
  }

  @Nullable
  @Override
  public WriteRequest getValue() {
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
    SerializableValueInSingleWindow that = (SerializableValueInSingleWindow) o;
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
