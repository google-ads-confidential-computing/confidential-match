/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cm.util;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import java.io.IOException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A retry strategy for the Apache HTTP client. Uses an exponential backoff when retrying. */
public final class ExponentialBackoffRetryStrategy extends DefaultHttpRequestRetryStrategy {

  private static final Logger logger =
      LoggerFactory.getLogger(ExponentialBackoffRetryStrategy.class);

  // Maximum number of retry attempts allowed
  private final TimeValue baseRetryDelay;
  // Multiply base retry delay by this amount for each attempt
  private final int retryDelayMultiplier;

  /**
   * Constructs a new instance.
   *
   * @param maxRetries the maximum number of retries allowed
   * @param baseRetryDelay the duration of the delay before the first retry attempt
   * @param retryDelayMultiplier the factor by which the retry delay is multiplied each attempt
   */
  public ExponentialBackoffRetryStrategy(
      int maxRetries, TimeValue baseRetryDelay, int retryDelayMultiplier) {
    // retry all exceptions, retry HTTP status codes listed below
    super(
        maxRetries,
        baseRetryDelay,
        // nonRetriableIOExceptionClasses
        ImmutableList.of(
            UnknownHostException.class, NoRouteToHostException.class, SSLException.class),
        // retriableCodes
        ImmutableList.of(
            HttpStatus.SC_REQUEST_TIMEOUT, // HTTP Status Code: 408
            HttpStatus.SC_TOO_MANY_REQUESTS, // HTTP Status Code: 429
            HttpStatus.SC_SERVICE_UNAVAILABLE, // HTTP Status Code: 503
            HttpStatus.SC_GATEWAY_TIMEOUT)); // HTTP Status Code: 504
    this.retryDelayMultiplier = retryDelayMultiplier;
    this.baseRetryDelay = baseRetryDelay;
  }

  /** Calculates the delay between retry attempts. */
  @SuppressWarnings("UnstableApiUsage") // LongMath::saturatedPow, LongMath::saturatedMultiply
  @Override
  public TimeValue getRetryInterval(HttpResponse response, int execCount, HttpContext context) {
    logger.info(
        "Current HTTP retry count: {}. Retrying unsuccessful HTTP response: {}-{} ",
        execCount,
        response.getCode(),
        response.getReasonPhrase());

    // Default method will return the header value or the base retry delay
    long baseBackoffMillis = super.getRetryInterval(response, execCount, context).toMilliseconds();
    return calculateBackoff(baseBackoffMillis, execCount);
  }

  @Override
  public TimeValue getRetryInterval(
      HttpRequest request, IOException exception, int execCount, HttpContext context) {
    logger.info(
        "Current HTTP retry count: {}. Retrying HTTP request with exception: {} . ",
        execCount,
        exception.getMessage());
    // Default method will return the base retry delay
    long baseBackoffMillis =
        super.getRetryInterval(request, exception, execCount, context).toMilliseconds();
    return calculateBackoff(baseBackoffMillis, execCount);
  }

  /** Determines if a request is safe to retry */
  @Override
  protected boolean handleAsIdempotent(HttpRequest request) {
    return true;
  }

  private TimeValue calculateBackoff(long baseBackoffMillis, int execCount) {
    // Base backoff delay, multiplied each time the request is retried
    long multiplier = LongMath.saturatedPow(retryDelayMultiplier, execCount);
    long regularBackoffMillis =
        LongMath.saturatedMultiply(baseRetryDelay.toMilliseconds(), multiplier);
    // Prefer the longer interval between the header value and the default delay and add jitter
    return TimeValue.ofMilliseconds(
        Double.valueOf(Math.random() * Math.max(regularBackoffMillis, baseBackoffMillis))
            .longValue());
  }
}
