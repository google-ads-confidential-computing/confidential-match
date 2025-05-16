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

package com.google.cm.mrp.clients.lookupserviceclient.gcp;

import static com.google.cm.util.gcp.AuthUtils.getDefaultCredentials;
import static com.google.cm.util.gcp.AuthUtils.getEmail;
import static com.google.cm.util.gcp.AuthUtils.getIdToken;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClientModule;
import com.google.cm.util.ExponentialBackoffRetryStrategy;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

/** Guice module that provides a {@link LookupServiceShardClient} for use with GCP. */
public final class LookupServiceShardClientGcpModule extends LookupServiceShardClientModule {

  /** Header key for the service account. */
  private static final String CLAIMED_IDENTITY_HEADER = "x-gscp-claimed-identity";

  /** Header key for an ID token with authorization endpoint as the audience. */
  private static final String AUTH_TOKEN_HEADER = "x-auth-token";

  private static final Timeout REQUEST_TIMEOUT = Timeout.ofMinutes(1);
  private static final Timeout CONNECTION_TIMEOUT = Timeout.ofSeconds(15);
  private static final TimeValue BASE_RETRY_DELAY = TimeValue.ofMilliseconds(500);
  private static final int RETRY_DELAY_MULTIPLIER = 2; // double retry interval every attempt
  private static final long CACHE_EXPIRATION_MINUTES = 30L;

  private final int maxRequestRetries; // retry for 1min+ to compensate for busy lookup servers
  private final Supplier<String> emailSupplier;
  private final Supplier<String> idTokenSupplier;

  /** Creates a new instance. */
  public LookupServiceShardClientGcpModule(String lookupServiceAudience, int maxRequestRetries) {
    this.maxRequestRetries = maxRequestRetries;
    emailSupplier =
        Suppliers.synchronizedSupplier(
            Suppliers.memoizeWithExpiration(
                () -> {
                  try {
                    return getEmail(getDefaultCredentials());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                },
                CACHE_EXPIRATION_MINUTES,
                MINUTES));
    idTokenSupplier =
        Suppliers.synchronizedSupplier(
            Suppliers.memoizeWithExpiration(
                () -> {
                  try {
                    return getIdToken(lookupServiceAudience, getDefaultCredentials());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                },
                CACHE_EXPIRATION_MINUTES,
                MINUTES));
  }

  /** Returns an interceptor that sets the headers for x-auth-token and x-gscp-claimed-identity. */
  private HttpRequestInterceptor lookupServiceAuthHeadersInterceptor() {
    return (httpRequest, entityDetails, httpContext) -> {
      httpRequest.setHeader(AUTH_TOKEN_HEADER, idTokenSupplier.get());
      httpRequest.setHeader(CLAIMED_IDENTITY_HEADER, emailSupplier.get());
    };
  }

  /** {@inheritDoc} */
  @Override
  protected CloseableHttpAsyncClient getCloseableHttpAsyncClient() {
    CloseableHttpAsyncClient httpClient =
        HttpAsyncClients.customHttp2()
            .setRetryStrategy(
                new ExponentialBackoffRetryStrategy(
                    maxRequestRetries, BASE_RETRY_DELAY, RETRY_DELAY_MULTIPLIER))
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectionRequestTimeout(CONNECTION_TIMEOUT) // Internal connection request
                    .setResponseTimeout(REQUEST_TIMEOUT) // Waiting for server response
                    .build())
            .setDefaultConnectionConfig(
                ConnectionConfig.custom()
                    .setConnectTimeout(CONNECTION_TIMEOUT)
                    .build()) // Establishing connection
            .setIOReactorConfig(IOReactorConfig.custom().setSoTimeout(REQUEST_TIMEOUT).build())
            .addRequestInterceptorLast(lookupServiceAuthHeadersInterceptor())
            .setTlsStrategy(ClientTlsStrategyBuilder.create().setTlsVersions(TLS.V_1_3).build())
            .build();

    httpClient.start();

    return httpClient;
  }
}
