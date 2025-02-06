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

package com.google.cm.mrp.clients.orchestratorclient;

import static com.google.cm.util.ProtoUtils.getProtoFromJson;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorClientHttpRequestFactory;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorEndpoint;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;
import com.google.cm.shared.api.model.Code;
import com.google.inject.Inject;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for orchestrator service. */
public final class OrchestratorClientImpl implements OrchestratorClient {

  private static final Logger logger = LoggerFactory.getLogger(OrchestratorClientImpl.class);

  private static final String GET_CURRENT_SCHEME_API_PATH = "/v1/shardingschemes:latest";
  private static final String CLUSTER_GROUP_ID_PARAM = "clusterGroupId";

  // TODO(b/309460286): Replace the google http client with Apache client and add retries
  private final HttpRequestFactory httpRequestFactory;
  private final String endpoint;

  /** Constructor for {@link OrchestratorClientImpl}. */
  @Inject
  public OrchestratorClientImpl(
      @OrchestratorClientHttpRequestFactory HttpRequestFactory httpRequestFactory,
      @OrchestratorEndpoint String endpoint) {
    this.httpRequestFactory = httpRequestFactory;
    this.endpoint = endpoint;
  }

  /** {@inheritDoc} */
  @Override
  public GetCurrentShardingSchemeResponse getCurrentShardingScheme(String clusterGroupId)
      throws OrchestratorClientException {
    logger.info("OrchestratorClient starts retrieving sharding scheme.");

    try {

      GenericUrl url = new GenericUrl(endpoint);
      url.setRawPath(GET_CURRENT_SCHEME_API_PATH);
      url.put(CLUSTER_GROUP_ID_PARAM, clusterGroupId);
      HttpResponse response = httpRequestFactory.buildGetRequest(url).execute();
      var result =
          getProtoFromJson(response.parseAsString(), GetCurrentShardingSchemeResponse.class);
      logger.info("OrchestratorClient successfully retrieved sharding scheme.");
      return result;

    } catch (HttpResponseException ex) {
      int statusCode = ex.getStatusCode();
      String codeName = Code.fromHttpStatusCode(statusCode).name();
      String message = "OrchestratorClient HTTP response status: " + statusCode + " " + codeName;
      throw new OrchestratorClientException(codeName, message, ex);
    } catch (IOException | RuntimeException ex) {
      String message = "OrchestratorClient threw an exception.";
      logger.error(message);
      throw new OrchestratorClientException(Code.UNKNOWN.name(), message, ex);
    }
  }
}
