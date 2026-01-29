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

package com.google.cm.mrp.clients.cryptoclient.gcp;

import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.common.base.Strings;
import com.google.scp.operator.cpio.cryptoclient.EncryptionKeyFetchingService;
import com.google.scp.operator.cpio.cryptoclient.HttpEncryptionKeyFetchingService;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import com.google.scp.operator.cpio.cryptoclient.MultiPartyHybridEncryptionKeyServiceImpl;
import com.google.scp.operator.cpio.cryptoclient.MultiPartyHybridEncryptionKeyServiceParams;
import com.google.scp.shared.api.util.HttpClientWrapper;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import com.google.scp.shared.gcp.util.GcpHttpInterceptorUtil;
import java.time.Duration;
import java.util.Optional;
import javax.inject.Inject;

/** A provider class for creating {@link MultiPartyHybridEncryptionKeyServiceImpl} instances. */
public final class MultiPartyHybridEncryptionKeyServiceProvider
    implements HybridEncryptionKeyServiceProvider {
  private static final int NUMBER_OF_COORDINATORS = 2;
  private static final int COORDINATOR_HTTPCLIENT_MAX_ATTEMPTS = 6;
  private static final double COORDINATOR_HTTPCLIENT_RETRY_MULTIPLIER = 3.0;
  private static final Duration COORDINATOR_HTTPCLIENT_RETRY_INITIAL_INTERVAL =
      Duration.ofSeconds(5);

  private final AeadProvider aeadProvider;
  private final MetricClient metricClient;

  @Inject
  public MultiPartyHybridEncryptionKeyServiceProvider(AeadProviderFactory aeadProviderFactory, MetricClient metricClient) {
    // Hybrid keys are assumed to be GCP
    this.aeadProvider = aeadProviderFactory.createGcpAeadProvider();
    this.metricClient = metricClient;
  }

  /**
   * Gets a {@link MultiPartyHybridEncryptionKeyServiceImpl} instance tailored to the given {@link
   * CoordinatorKeyInfo}.
   */
  @Override
  public HybridEncryptionKeyService getHybridEncryptionKeyService(
      CoordinatorKeyInfo coordinatorKeyInfo) throws HybridEncryptionKeyServiceProviderException {
    if (coordinatorKeyInfo.getCoordinatorInfoList().size() != NUMBER_OF_COORDINATORS) {
      throw new HybridEncryptionKeyServiceProviderException(
          new IllegalArgumentException("Number of Coordinators doesn't match"));
    }

    CloudAeadSelector cloudAeadSelectorA;
    CloudAeadSelector cloudAeadSelectorB;
    CoordinatorInfo coordinatorInfoA = coordinatorKeyInfo.getCoordinatorInfo(0);
    CoordinatorInfo coordinatorInfoB = coordinatorKeyInfo.getCoordinatorInfo(1);

    try {
      // The KMS identity service account is optional for coordinator credential creation.
      cloudAeadSelectorA =
          aeadProvider.getAeadSelector(
              AeadProviderParameters.forWipProviderAndOptionalServiceAccount(
                  coordinatorInfoA.getKmsWipProvider(),
                  Optional.ofNullable(Strings.emptyToNull(coordinatorInfoA.getKmsIdentity()))));
      cloudAeadSelectorB =
          aeadProvider.getAeadSelector(
              AeadProviderParameters.forWipProviderAndOptionalServiceAccount(
                  coordinatorInfoB.getKmsWipProvider(),
                  Optional.ofNullable(Strings.emptyToNull(coordinatorInfoB.getKmsIdentity()))));
    } catch (AeadProviderException e) {
      throw new HybridEncryptionKeyServiceProviderException(e.getCause());
    }

    String audienceUrlA =
        !coordinatorInfoA.getKeyServiceAudienceUrl().isEmpty()
            ? coordinatorInfoA.getKeyServiceAudienceUrl()
            : coordinatorInfoA.getKeyServiceEndpoint();
    String audienceUrlB =
        !coordinatorInfoB.getKeyServiceAudienceUrl().isEmpty()
            ? coordinatorInfoB.getKeyServiceAudienceUrl()
            : coordinatorInfoB.getKeyServiceEndpoint();
    EncryptionKeyFetchingService keyFetchingServiceA =
        getEncryptionKeyFetchingService(audienceUrlA, coordinatorInfoA.getKeyServiceEndpoint());
    EncryptionKeyFetchingService keyFetchingServiceB =
        getEncryptionKeyFetchingService(audienceUrlB, coordinatorInfoB.getKeyServiceEndpoint());

    MultiPartyHybridEncryptionKeyServiceParams params =
        MultiPartyHybridEncryptionKeyServiceParams.builder()
            .setCoordAKeyFetchingService(keyFetchingServiceA)
            .setCoordBKeyFetchingService(keyFetchingServiceB)
            .setCoordAAeadService(cloudAeadSelectorA)
            .setCoordBAeadService(cloudAeadSelectorB)
            .setMetricClient(metricClient)
            .setEnablePrivateKeyDecryptionRetries(true)
            .build();

    return MultiPartyHybridEncryptionKeyServiceImpl.newInstance(params);
  }

  private static EncryptionKeyFetchingService getEncryptionKeyFetchingService(
      String audienceUrl, String keyServiceEndpoint) {
    // GCP custom audiences provide a way to authorize requests using key service endpoints.
    HttpClientWrapper httpClient = getHttpClientWrapper(audienceUrl);
    return new HttpEncryptionKeyFetchingService(httpClient, keyServiceEndpoint);
  }

  private static HttpClientWrapper getHttpClientWrapper(String audienceUrl) {
    return HttpClientWrapper.builder()
        .setInterceptor(GcpHttpInterceptorUtil.createHttpInterceptor(audienceUrl))
        .setExponentialBackoff(
            COORDINATOR_HTTPCLIENT_RETRY_INITIAL_INTERVAL,
            COORDINATOR_HTTPCLIENT_RETRY_MULTIPLIER,
            COORDINATOR_HTTPCLIENT_MAX_ATTEMPTS)
        .build();
  }
}
