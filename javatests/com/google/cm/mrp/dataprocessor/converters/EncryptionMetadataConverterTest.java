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

package com.google.cm.mrp.dataprocessor.converters;

import static com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType.KEY_TYPE_XCHACHA20_POLY1305;
import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.UNSPECIFIED;
import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_METADATA_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.EncryptionMetadataProto;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.AwsWrappedKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EncryptionMetadataConverterTest {

  @Test
  public void convertToLookupKeyType_success() {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider("testWip"))))
            .build();

    KeyType result =
        EncryptionMetadataConverter.convertToLookupKeyType(
            encryptionMetadata.getEncryptionKeyInfo().getWrappedKeyInfo().getKeyType());

    assertThat(result).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
  }

  @Test
  public void convertToLookupKeyType_unsupportedTypeFailure() {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(UNSPECIFIED)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider("testWip"))))
            .build();

    assertThrows(
        JobProcessorException.class,
        () ->
            EncryptionMetadataConverter.convertToLookupKeyType(
                encryptionMetadata.getEncryptionKeyInfo().getWrappedKeyInfo().getKeyType()));
  }

  @Test
  public void convertToBackendEncryptionMetadata_wrappedKeySuccess() {
    var testKeyType = WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
    String wip = "testWip";
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder().setKeyType(testKeyType).setKmsWipProvider(wip)))
            .build();

    EncryptionMetadata result =
        EncryptionMetadataConverter.convertToBackendEncryptionMetadata(encryptionMetadata);

    assertThat(result.hasEncryptionKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().hasWrappedKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().getKeyType())
        .isEqualTo(XCHACHA20_POLY1305);
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().hasGcpWrappedKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().hasAwsWrappedKeyInfo()).isFalse();
    assertThat(
            result
                .getEncryptionKeyInfo()
                .getWrappedKeyInfo()
                .getGcpWrappedKeyInfo()
                .getWipProvider())
        .isEqualTo(wip);
  }

  @Test
  public void convertToBackendEncryptionMetadata_awsWrappedKeySuccess() {
    var testKeyType =
        EncryptionMetadataProto.EncryptionMetadata.AwsWrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
    String audience = "audience";
    String role = "role";
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setAwsWrappedKeyInfo(
                        EncryptionMetadataProto.EncryptionMetadata.AwsWrappedKeyInfo.newBuilder()
                            .setKeyType(testKeyType)
                            .setRoleArn(role)
                            .setAudience(audience)))
            .build();

    EncryptionMetadata result =
        EncryptionMetadataConverter.convertToBackendEncryptionMetadata(encryptionMetadata);

    assertThat(result.hasEncryptionKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().hasWrappedKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().getKeyType())
        .isEqualTo(XCHACHA20_POLY1305);
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().hasGcpWrappedKeyInfo()).isFalse();
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().hasAwsWrappedKeyInfo()).isTrue();
    var awsBackendProto = result.getEncryptionKeyInfo().getWrappedKeyInfo().getAwsWrappedKeyInfo();
    assertThat(awsBackendProto.getAudience()).isEqualTo(audience);
    assertThat(awsBackendProto.getRoleArn()).isEqualTo(role);
  }

  @Test
  public void convertToBackendEncryptionMetadata_wrappedKeyWithSpaces_Success() {
    var testKeyType = WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
    String wip = " projects/123/locations/global/workloadIdentityPools/wip/providers/wip-pvdr ";
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder().setKeyType(testKeyType).setKmsWipProvider(wip)))
            .build();

    EncryptionMetadata result =
        EncryptionMetadataConverter.convertToBackendEncryptionMetadata(encryptionMetadata);

    assertThat(result.hasEncryptionKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().hasWrappedKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().getWrappedKeyInfo().getKeyType())
        .isEqualTo(XCHACHA20_POLY1305);
    assertThat(
            result
                .getEncryptionKeyInfo()
                .getWrappedKeyInfo()
                .getGcpWrappedKeyInfo()
                .getWipProvider())
        .isEqualTo("projects/123/locations/global/workloadIdentityPools/wip/providers/wip-pvdr");
  }

  @Test
  public void convertToBackendEncryptionMetadata_noEncryptionKeyInfoFailure() {
    var encryptionMetadata = EncryptionMetadataProto.EncryptionMetadata.getDefaultInstance();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                EncryptionMetadataConverter.convertToBackendEncryptionMetadata(encryptionMetadata));
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_PARAMETERS);
  }

  @Test
  public void convertToBackendEncryptionMetadata_coordinatorKeySuccess() {
    String testWip1 = "testWip1";
    String testSA1 = "testSA1";
    String testEndpoint1 = "testEndpoint1";
    String testCloudFunctionUrl1 = "testCloudFunctionUrl1";
    String testWip2 = "testWip2";
    String testSA2 = "testSA2";
    String testEndpoint2 = "testEndpoint2";
    String testCloudFunctionUrl2 = "testCloudFunctionUrl2";
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(
                        CoordinatorKeyInfo.newBuilder()
                            .addCoordinatorInfo(
                                CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(testEndpoint1)
                                    .setKmsWipProvider(testWip1)
                                    .setKmsIdentity(testSA1)
                                    .setKeyServiceAudienceUrl(testCloudFunctionUrl1))
                            .addCoordinatorInfo(
                                CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(testEndpoint2)
                                    .setKmsWipProvider(testWip2)
                                    .setKmsIdentity(testSA2)
                                    .setKeyServiceAudienceUrl(testCloudFunctionUrl2))))
            .build();

    EncryptionMetadata result =
        EncryptionMetadataConverter.convertToBackendEncryptionMetadata(encryptionMetadata);

    assertThat(result.hasEncryptionKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().hasCoordinatorKeyInfo()).isTrue();
    assertThat(result.getEncryptionKeyInfo().getCoordinatorKeyInfo().getCoordinatorInfoCount())
        .isEqualTo(2);
    var coordinator1 = result.getEncryptionKeyInfo().getCoordinatorKeyInfo().getCoordinatorInfo(0);
    var coordinator2 = result.getEncryptionKeyInfo().getCoordinatorKeyInfo().getCoordinatorInfo(1);
    assertThat(coordinator1.getKeyServiceEndpoint()).isEqualTo(testEndpoint1);
    assertThat(coordinator1.getKmsIdentity()).isEqualTo(testSA1);
    assertThat(coordinator1.getKmsWipProvider()).isEqualTo(testWip1);
    assertThat(coordinator1.getKeyServiceAudienceUrl()).isEqualTo(testCloudFunctionUrl1);
    assertThat(coordinator2.getKeyServiceEndpoint()).isEqualTo(testEndpoint2);
    assertThat(coordinator2.getKmsIdentity()).isEqualTo(testSA2);
    assertThat(coordinator2.getKmsWipProvider()).isEqualTo(testWip2);
    assertThat(coordinator2.getKeyServiceAudienceUrl()).isEqualTo(testCloudFunctionUrl2);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_gcpWrappedKeySuccess() {
    String wip = "testWip";
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider(wip))))
            .build();
    String dek = "testDek";
    String kek = "testKek";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                DataRecordEncryptionKeys.WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .build())
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getWrappedKeyInfo().getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(result.getWrappedKeyInfo().getEncryptedDek()).isEqualTo(dek);
    assertThat(result.getWrappedKeyInfo().getKekKmsResourceId()).isEqualTo(kek);
    assertThat(result.getWrappedKeyInfo().getKmsWipProvider()).isEqualTo(wip);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_wrappedKeyInDataRecord_success() {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider(("")))))
            .build();
    String dek = "testDek";
    String kek = "testKek";
    String wip = "testWip";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .setGcpWrappedKeys(GcpWrappedKeys.newBuilder().setWipProvider(wip)))
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getWrappedKeyInfo().getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(result.getWrappedKeyInfo().getEncryptedDek()).isEqualTo(dek);
    assertThat(result.getWrappedKeyInfo().getKekKmsResourceId()).isEqualTo(kek);
    assertThat(result.getWrappedKeyInfo().getKmsWipProvider()).isEqualTo(wip);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_wrappedKeyWithSpaces_Success() {
    String wip = " projects/123/locations/global/workloadIdentityPools/wip/providers/wip-pvdr ";
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider(wip))))
            .build();
    String dek = " testDEK ";
    String kek = " gcp-kms://projects/123/locations/us1/keyRings/ring/cryptoKeys/key ";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                DataRecordEncryptionKeys.WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .build())
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getWrappedKeyInfo().getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(result.getWrappedKeyInfo().getEncryptedDek()).isEqualTo("testDEK");
    assertThat(result.getWrappedKeyInfo().getKekKmsResourceId())
        .isEqualTo("gcp-kms://projects/123/locations/us1/keyRings/ring/cryptoKeys/key");
    assertThat(result.getWrappedKeyInfo().getKmsWipProvider())
        .isEqualTo("projects/123/locations/global/workloadIdentityPools/wip/providers/wip-pvdr");
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_awsWrappedKeySuccess() {
    String audience = "testAud";
    String role = "testRole";
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setAwsWrappedKeyInfo(
                                AwsWrappedKeyInfo.newBuilder()
                                    .setAudience(audience)
                                    .setRoleArn(role)
                                    .build())))
            .build();
    String dek = "testDek";
    String kek = "testKek";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                DataRecordEncryptionKeys.WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .build())
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getWrappedKeyInfo().getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(result.getWrappedKeyInfo().getEncryptedDek()).isEqualTo(dek);
    assertThat(result.getWrappedKeyInfo().getKekKmsResourceId()).isEqualTo(kek);
    assertThat(result.getWrappedKeyInfo().getAwsWrappedKeyInfo().getAudience()).isEqualTo(audience);
    assertThat(result.getWrappedKeyInfo().getAwsWrappedKeyInfo().getRoleArn()).isEqualTo(role);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_awsWrappedKeyInDataRecord_success() {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)
                            .setAwsWrappedKeyInfo(AwsWrappedKeyInfo.newBuilder().setRoleArn(("")))))
            .build();
    String dek = "testDek";
    String kek = "testKek";
    String role = "testRole";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .setAwsWrappedKeys(AwsWrappedKeys.newBuilder().setRoleArn(role)))
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getWrappedKeyInfo().getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(result.getWrappedKeyInfo().getEncryptedDek()).isEqualTo(dek);
    assertThat(result.getWrappedKeyInfo().getKekKmsResourceId()).isEqualTo(kek);
    assertThat(result.getWrappedKeyInfo().getAwsWrappedKeyInfo().getRoleArn()).isEqualTo(role);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_coordinatorKeySuccess() {
    String endpoint1 = "testEndpoint1";
    String identity1 = "testIdentity1";
    String wip1 = "testWip1";
    String testCloudFunctionUrl1 = "testCloudFunctionUrl1";
    String endpoint2 = "testEndpoint2";
    String identity2 = "testIdentity2";
    String wip2 = "testWip2";
    String testCloudFunctionUrl2 = "testCloudFunctionUrl2";
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(
                        EncryptionMetadata.CoordinatorKeyInfo.newBuilder()
                            .addCoordinatorInfo(
                                EncryptionMetadata.CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(endpoint1)
                                    .setKmsWipProvider(wip1)
                                    .setKmsIdentity(identity1)
                                    .setKeyServiceAudienceUrl(testCloudFunctionUrl1)
                                    .build())
                            .addCoordinatorInfo(
                                EncryptionMetadata.CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(endpoint2)
                                    .setKmsWipProvider(wip2)
                                    .setKmsIdentity(identity2)
                                    .setKeyServiceAudienceUrl(testCloudFunctionUrl2)
                                    .build())))
            .build();
    String keyId = "testKeyId";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId(keyId).build())
            .build();

    var result =
        EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
            dataRecordEncryptionKeys, encryptionMetadata);

    assertThat(result.getCoordinatorKeyInfo().getKeyId()).isEqualTo(keyId);
    assertThat(result.getCoordinatorKeyInfo().getCoordinatorInfoList())
        .hasSize(
            encryptionMetadata
                .getEncryptionKeyInfo()
                .getCoordinatorKeyInfo()
                .getCoordinatorInfoList()
                .size());
    var coordinator1 = result.getCoordinatorKeyInfo().getCoordinatorInfo(0);
    var coordinator2 = result.getCoordinatorKeyInfo().getCoordinatorInfo(1);
    assertThat(coordinator1.getKeyServiceEndpoint()).isEqualTo(endpoint1);
    assertThat(coordinator1.getKmsWipProvider()).isEqualTo(wip1);
    assertThat(coordinator1.getKmsIdentity()).isEqualTo(identity1);
    assertThat(coordinator1.getKeyServiceAudienceUrl()).isEqualTo(testCloudFunctionUrl1);
    assertThat(coordinator2.getKeyServiceEndpoint()).isEqualTo(endpoint2);
    assertThat(coordinator2.getKmsWipProvider()).isEqualTo(wip2);
    assertThat(coordinator2.getKmsIdentity()).isEqualTo(identity2);
    assertThat(coordinator2.getKeyServiceAudienceUrl()).isEqualTo(testCloudFunctionUrl2);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_noEncryptionMetadataFailure() {
    EncryptionMetadata encryptionMetadata = EncryptionMetadata.newBuilder().build();
    String keyId = "testKeyId";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId(keyId).build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
                    dataRecordEncryptionKeys, encryptionMetadata));

    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_METADATA_CONFIG_ERROR);
  }

  @Test
  public void convertToLookupEncryptionKeyInfo_badWrappedKeyInfoFailure() {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        EncryptionMetadata.WrappedKeyInfo.newBuilder()
                            .setKeyType(XCHACHA20_POLY1305)))
            .build();
    String dek = "testDek";
    String kek = "testKek";
    var dataRecordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                DataRecordEncryptionKeys.WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek(dek)
                    .setKekUri(kek)
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
                    dataRecordEncryptionKeys, encryptionMetadata));

    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_METADATA_CONFIG_ERROR);
  }
}
