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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.hc.client5.http.async.methods.SimpleHttpRequest.create;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.util.TimeValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ExponentialBackoffRetryStrategyTest {

  @Test
  public void getRetryInterval_responseCodeWithZeroAttemptsValue_returnsInitialValue() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var response = SimpleHttpResponse.create(HttpStatus.SC_TOO_MANY_REQUESTS);
    var context = new HttpClientContext();

    long delayMillis = strategy.getRetryInterval(response, 0, context).toMilliseconds();

    assertThat(delayMillis).isAtLeast(0);
    assertThat(delayMillis).isAtMost(initialDelay.toMilliseconds());
  }

  @Test
  public void getRetryInterval_responseCodeWithPositiveAttemptsValue_ReturnsExponentialValue() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var response = SimpleHttpResponse.create(HttpStatus.SC_TOO_MANY_REQUESTS);
    var context = new HttpClientContext();

    long delayMillis = strategy.getRetryInterval(response, 3, context).toMilliseconds();

    assertThat(delayMillis).isAtLeast(0);
    assertThat(delayMillis).isAtMost(initialDelay.toMilliseconds() * 8);
  }

  @Test
  public void
      getRetryInterval_esponseCodeWithNegativeAttemptsValue_throwsIllegalArumentException() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var response = SimpleHttpResponse.create(HttpStatus.SC_TOO_MANY_REQUESTS);
    var context = new HttpClientContext();

    assertThrows(
        IllegalArgumentException.class, () -> strategy.getRetryInterval(response, -3, context));
  }

  @Test
  public void getRetryInterval_exceptionWithZeroAttemptsValue_returnsInitialValue() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var request = create("POST", "testUri");
    var context = new HttpClientContext();

    long delayMillis =
        strategy.getRetryInterval(request, new IOException(), 0, context).toMilliseconds();

    assertThat(delayMillis).isAtLeast(0);
    assertThat(delayMillis).isAtMost(initialDelay.toMilliseconds());
  }

  @Test
  public void getRetryInterval_exceptionWithPositiveAttemptsValue_ReturnsExponentialValue() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var request = create("POST", "testUri");
    var context = new HttpClientContext();

    long delayMillis =
        strategy.getRetryInterval(request, new IOException(), 3, context).toMilliseconds();

    assertThat(delayMillis).isAtLeast(0);
    assertThat(delayMillis).isAtMost(initialDelay.toMilliseconds() * 8);
  }

  @Test
  public void getRetryInterval_exceptionWithNegativeAttemptsValue_throwsIllegalArumentException() {
    var initialDelay = TimeValue.ofMilliseconds(500);
    var strategy = new ExponentialBackoffRetryStrategy(9, initialDelay, 2);
    var request = create("POST", "testUri");
    var context = new HttpClientContext();

    assertThrows(
        IllegalArgumentException.class,
        () -> strategy.getRetryInterval(request, new IOException(), -3, context));
  }
}
