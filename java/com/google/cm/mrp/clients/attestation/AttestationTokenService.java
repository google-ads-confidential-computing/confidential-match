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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenDefaultAudience;
import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenHttpClient;
import com.google.cm.mrp.clients.attestation.Annotations.AttestationTokenSignatures;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.AttestationTokenServiceException;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.UncheckedAttestationTokenServiceException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.scp.shared.mapper.GuavaObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service to fetch Confidential Space tokens to be used as OIDC tokens for attestation */
public final class AttestationTokenService {

  private static final Logger logger = LoggerFactory.getLogger(AttestationTokenService.class);
  private static final ObjectMapper mapper = new GuavaObjectMapper();
  private static final String EMPTY_AUDIENCE = "";
  private static final int CACHE_EXPIRATION_BUFFER_SEC = 5;
  private static final int MAX_CACHE_SIZE = 10;
  private static final long CACHE_TTL_SEC = 3600;
  protected static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();

  private static final String LOCALHOST_PATH = "http://localhost/v1/token";
  private static final String TOKEN_TYPE = "PKI";
  private static final String EXPIRATION_NAME = "exp";

  private final CloseableHttpClient httpClient;
  private final List<String> signatures;
  private final String defaultAudience;

  private final LoadingCache<String, AwsToken> tokenCache =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHE_SIZE)
          .expireAfterWrite(CACHE_TTL_SEC, TimeUnit.SECONDS)
          .concurrencyLevel(CONCURRENCY_LEVEL)
          .build(getCacheLoader());

  @Inject
  public AttestationTokenService(
      @AttestationTokenHttpClient CloseableHttpClient httpClient,
      @AttestationTokenDefaultAudience String defaultAudience,
      @AttestationTokenSignatures List<String> signatures) {
    this.httpClient = httpClient;
    this.defaultAudience = defaultAudience;
    this.signatures = signatures;
  }

  /** Gets a cached token using default audience. */
  public String getCachedToken() throws AttestationTokenServiceException {
    return getCachedTokenWithAudience(EMPTY_AUDIENCE);
  }

  /** Gets a cached token using the given audience. */
  public String getCachedTokenWithAudience(String audience)
      throws AttestationTokenServiceException {
    if (audience.isBlank()) {
      audience = defaultAudience;
    }
    try {
      AwsToken awsToken = tokenCache.get(audience);
      if (Instant.now()
          .isAfter(Instant.ofEpochSecond(awsToken.getExpiration() - CACHE_EXPIRATION_BUFFER_SEC))) {
        tokenCache.invalidate(audience);
        awsToken = tokenCache.get(audience);
      }
      return awsToken.getToken();
    } catch (UncheckedAttestationTokenServiceException
        | ExecutionException
        | UncheckedExecutionException e) {
      String msg = "Could not get token from cache.";
      logger.error(msg);
      throw new AttestationTokenServiceException(msg, e);
    }
  }

  private AwsToken sendAndGetHttpRequest(String audience) throws AttestationTokenServiceException {
    var postRequest = new HttpPost(URI.create(LOCALHOST_PATH));
    postRequest.setEntity(new StringEntity(getTokenJson(audience), ContentType.APPLICATION_JSON));
    try {
      return httpClient.execute(
          postRequest,
          httpResponse -> {
            var responseBody = EntityUtils.toString(httpResponse.getEntity());
            int errorCode = httpResponse.getCode();
            if (errorCode != 200) {
              String msg =
                  String.format(
                      "Received errorCode from AttestationTokenHttpClient: %s, %s",
                      errorCode, httpResponse.getEntity());
              logger.error(msg);
              throw new UncheckedAttestationTokenServiceException(msg);
            } else {
              JsonNode responseJson = mapper.readTree(responseBody);
              long exp = responseJson.get(EXPIRATION_NAME).asLong();
              return new AwsToken(responseBody, exp);
            }
          });
    } catch (IOException e) {
      String msg = "Could not get attestation token from confidential space service.";
      logger.error(msg);
      throw new UncheckedAttestationTokenServiceException(msg, e);
    }
  }

  private String getTokenJson(String audience) throws AttestationTokenServiceException {
    try {
      var request =
          ConfidentialSpaceTokenRequest.forAudienceTokenTypeAndKeyIds(
              audience, TOKEN_TYPE, signatures);
      return mapper.writeValueAsString(request);
    } catch (JsonProcessingException e) {
      String msg = "Could not convert ConfidentialSpaceTokenRequest to JSON.";
      logger.error(msg);
      throw new AttestationTokenServiceException(msg, e);
    }
  }

  private CacheLoader<String, AwsToken> getCacheLoader() {
    return new CacheLoader<>() {
      @SuppressWarnings("NullableProblems")
      @Override
      public AwsToken load(String audience) throws AttestationTokenServiceException {
        return sendAndGetHttpRequest(audience);
      }
    };
  }

  private static class AwsToken {

    private final String token;
    private final long expiration;

    AwsToken(String token, long expiration) {
      this.token = token;
      this.expiration = expiration;
    }

    String getToken() {
      return token;
    }

    long getExpiration() {
      return expiration;
    }
  }
}
