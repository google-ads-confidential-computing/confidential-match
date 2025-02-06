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

package com.google.cm.mrp.clients.attestation;

import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenDefaultAudience;
import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenHttpClient;
import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenSignatures;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

/** Guice module that provides dependencies for AttestationTokenService */
public final class AttestationTokenModule extends AbstractModule {

  private static final int MAX_RETRIES = 10;
  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
  private static final Timeout REQUEST_TIMEOUT = Timeout.ofMinutes(1);

  private static final String CONFIDENTIAL_SPACE_SOCKET_ADDRESS =
      "/run/container_launcher/teeserver.sock";
  private final String defaultAudience;
  private final List<String> awsSignaturesList;

  /** Constructs a new instance. */
  public AttestationTokenModule(String defaultAudience, List<String> awsSignaturesList) {
    this.defaultAudience = defaultAudience;
    this.awsSignaturesList = awsSignaturesList;
  }

  @Provides
  @Singleton
  @AttestationTokenDefaultAudience
  String getDefaultAudience() {
    return defaultAudience;
  }

  @Provides
  @Singleton
  @AttestationTokenSignatures
  List<String> getAwsSignaturesList() {
    return awsSignaturesList;
  }

  /** Build Apache HTTP client that can connect to a UNIX domain socket. */
  @Provides
  @Singleton
  @AttestationTokenHttpClient
  CloseableHttpClient buildUnixDomainHttpClient() {
    // Must create custom connection manager and use unix domain library
    BasicHttpClientConnectionManager connManager =
        new BasicHttpClientConnectionManager(
            (unused) ->
                new ConnectionSocketFactory() {
                  @Override
                  public Socket createSocket(HttpContext httpContext) throws IOException {
                    return AFUNIXSocket.newInstance();
                  }

                  @Override
                  public Socket connectSocket(
                      TimeValue timeValue,
                      Socket socket,
                      HttpHost httpHost,
                      InetSocketAddress inetSocketAddress,
                      InetSocketAddress inetSocketAddress1,
                      HttpContext httpContext)
                      throws IOException {
                    File socketFile = Path.of(CONFIDENTIAL_SPACE_SOCKET_ADDRESS).toFile();
                    socket.connect(
                        new AFUNIXSocketAddress(socketFile), (int) timeValue.getDuration());
                    return socket;
                  }
                });

    connManager.setConnectionConfig(
        ConnectionConfig.custom()
            .setConnectTimeout(REQUEST_TIMEOUT) // Establishing connection
            .build());

    return HttpClients.custom()
        .setConnectionManager(connManager)
        .setRetryStrategy(
            new DefaultHttpRequestRetryStrategy(MAX_RETRIES, TimeValue.of(RETRY_INTERVAL)))
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectionRequestTimeout(REQUEST_TIMEOUT) // Internal connection request
                .setResponseTimeout(REQUEST_TIMEOUT) // Waiting for server response.build());
                .build())
        .build();
  }
}
