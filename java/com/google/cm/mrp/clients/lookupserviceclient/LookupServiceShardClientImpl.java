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

package com.google.cm.mrp.clients.lookupserviceclient;

import static com.google.cm.mrp.Constants.CustomLogLevel.DETAIL;
import static com.google.cm.mrp.dataprocessor.converters.ErrorCodeConverter.isInvalidShardingSchemeErrorReason;
import static com.google.cm.mrp.dataprocessor.converters.ErrorCodeConverter.isValidRowLevelErrorReason;

import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceShardClientHttpClient;
import com.google.cm.mrp.selectors.LookupProtoFormatSelector.LookupProtoFormatHandler;
import com.google.cm.shared.api.model.Code;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.Method;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Client for a single shard in the lookup service. */
public final class LookupServiceShardClientImpl implements LookupServiceShardClient {

  private static final Logger logger = LogManager.getLogger(LookupServiceShardClientImpl.class);

  private final CloseableHttpAsyncClient httpClient;
  private final LookupProtoFormatHandler lookupProtoFormatHandler;

  /** Constructor for {@link LookupServiceShardClientImpl}. */
  @Inject
  public LookupServiceShardClientImpl(
      @LookupServiceShardClientHttpClient CloseableHttpAsyncClient httpClient,
      LookupProtoFormatHandler lookupProtoFormatHandler) {
    this.httpClient = httpClient;
    this.lookupProtoFormatHandler = lookupProtoFormatHandler;
  }

  /** {@inheritDoc} */
  @Override
  public LookupResponse lookupRecords(String shardEndpoint, LookupRequest lookupRequest)
      throws LookupServiceShardClientException {
    logger.log(
        Level.getLevel(DETAIL.name()),
        "LookupServiceShardClient starting to send request to lookup service for Shard: {}",
        shardEndpoint);
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      var request = SimpleHttpRequest.create(Method.POST, URI.create(shardEndpoint));
      byte[] content = lookupProtoFormatHandler.getContentBytes(lookupRequest);
      request.setBody(content, lookupProtoFormatHandler.getContentType());
      request.setHeader(HttpHeaders.CONTENT_LENGTH, content.length);
      long requestSetupTimeMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);

      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Thread was interrupted.");
      }
      SimpleHttpResponse response = httpClient.execute(request, null).get();
      long totalRequestTimeMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);

      if (!Code.isSuccessStatusCode(response.getCode())) {
        logger.log(
            Level.getLevel(DETAIL.name()),
            "LookupServiceShardClient successfully received response. "
                + "Record count: {}, Total request time: {} ms (request setup: {} ms), Shard: {}",
            lookupRequest.getDataRecordsCount(),
            totalRequestTimeMs,
            requestSetupTimeMs,
            shardEndpoint);
        int statusCode = response.getCode();
        String httpStatusCodeName = Code.fromHttpStatusCode(statusCode).name();
        String message =
            "LookupServiceShardClient HTTP response status: "
                + statusCode
                + " "
                + httpStatusCodeName;
        String errorReason;
        try {
          errorReason = lookupProtoFormatHandler.getErrorResponse(response).getMessage();
          if (lookupProtoFormatHandler.getErrorResponse(response).getDetailsCount() > 0) {
            String responseErrorReason =
                lookupProtoFormatHandler.getErrorResponse(response).getDetails(0).getReason();
            if (isValidRowLevelErrorReason(responseErrorReason)
                || isInvalidShardingSchemeErrorReason(responseErrorReason)) {
              errorReason = responseErrorReason;
            }
          }
        } catch (IOException ignored) {
          errorReason = ErrorReason.UNKNOWN.name();
        }
        throw new LookupServiceShardClientException(httpStatusCodeName, errorReason, message);
      }

      var result = lookupProtoFormatHandler.getLookupResponse(response);
      logger.log(
          Level.getLevel(DETAIL.name()),
          "LookupServiceShardClient successfully received response. "
              + "Record count: {}, Total request time: {} ms (request setup: {} ms), Shard: {}",
          lookupRequest.getDataRecordsCount(),
          totalRequestTimeMs,
          requestSetupTimeMs,
          shardEndpoint);

      return result;

    } catch (ExecutionException ex) {
      String message = "LookupServiceShardClient threw an exception.";
      logger.error(message);
      throw new LookupServiceShardClientException(Code.UNKNOWN.name(), message, ex.getCause());
    } catch (InterruptedException | IOException ex) {
      String message = "LookupServiceShardClient threw an exception.";
      logger.error(message);
      throw new LookupServiceShardClientException(Code.UNKNOWN.name(), message, ex);
    }
  }
}
