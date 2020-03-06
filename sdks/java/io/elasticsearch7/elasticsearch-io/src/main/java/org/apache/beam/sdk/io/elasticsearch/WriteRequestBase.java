package org.apache.beam.sdk.io.elasticsearch;

public abstract class WriteRequestBase implements WriteRequest {
  private final String id;

  protected WriteRequestBase(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }
}
