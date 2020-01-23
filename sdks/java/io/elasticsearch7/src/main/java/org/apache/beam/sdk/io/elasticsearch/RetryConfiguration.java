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
import java.io.Serializable;
import java.util.function.Predicate;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.RestStatus;
import org.joda.time.Duration;

/**
 * A POJO encapsulating a configuration for retry behavior when issuing requests to ES. A retry will
 * be attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for 429
 * TOO_MANY_REQUESTS error.
 */
@AutoValue
public abstract class RetryConfiguration implements Serializable {
  private static final int DEFAULT_MAX_ATTEMPTS = 5;
  private static final Duration DEFAULT_MAX_DURATION = Duration.millis(10000);

  public static final RetryConfiguration DEFAULT = withDefaults();

  public static RetryPredicate defaultRetryPredicate(int code) {
    return new DefaultRetryPredicate(code);
  }

  public static RetryPredicate defaultRetryPredicate() {
    return new DefaultRetryPredicate();
  }

  public abstract int getMaxAttempts();

  public abstract Duration getMaxDuration();

  public abstract RetryPredicate getRetryPredicate();

  public static Builder builder() {
    return new AutoValue_RetryConfiguration.Builder();
  }

  public static RetryConfiguration withDefaults() {
    return builder()
        .setMaxAttempts(DEFAULT_MAX_ATTEMPTS)
        .setMaxDuration(DEFAULT_MAX_DURATION)
        .setRetryPredicate(defaultRetryPredicate())
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

    public abstract RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

    public abstract RetryConfiguration.Builder setRetryPredicate(RetryPredicate retryPredicate);

    public abstract RetryConfiguration build();
  }

  /**
   * An interface used to control if we retry the Elasticsearch call when a {@link Response} is
   * obtained. If {@link RetryPredicate#test(Object)} returns true, {@link Elasticsearch7IO.Write}
   * tries to resend the requests to the Elasticsearch server if the {@link RetryConfiguration}
   * permits it.
   */
  @FunctionalInterface
  interface RetryPredicate extends Predicate<RestStatus>, Serializable {}

  /**
   * This is the default predicate used to test if a failed ES operation should be retried. A retry
   * will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for
   * TOO_MANY_REQUESTS(429) error.
   */
  static class DefaultRetryPredicate implements RetryPredicate {

    private final int errorCode;

    DefaultRetryPredicate(int code) {
      this.errorCode = code;
    }

    DefaultRetryPredicate() {
      this(429);
    }

    /** Returns true if the response has the error code for any mutation. */
    private static boolean errorCodePresent(RestStatus status, int errorCode) {
      return status.getStatus() == errorCode;
    }

    @Override
    public boolean test(RestStatus status) {
      return errorCodePresent(status, errorCode);
    }
  }
}
