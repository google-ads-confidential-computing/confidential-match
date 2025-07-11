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

package com.google.cm.mrp;

import static com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType.KEY_TYPE_XCHACHA20_POLY1305;
import static com.google.cm.lookupserver.api.LookupProto.LookupResult.Status.STATUS_FAILED;
import static com.google.cm.lookupserver.api.LookupProto.LookupResult.Status.STATUS_SUCCESS;
import static com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
import static com.google.cm.mrp.backend.EncodingTypeProto.EncodingType.BASE64;
import static com.google.cm.mrp.backend.EncodingTypeProto.EncodingType.HEX;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.JOB_RESULT_CODE_UNKNOWN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_FAILURE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_SCHEMA_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.decryptEncryptedKeyset;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptDek;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateAeadUri;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateXChaChaKeyset;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getAeadFromJsonKeyset;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getHybridDecryptFromJsonKeyset;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getHybridEncryptFromJsonKeyset;
import static com.google.cm.mrp.testutils.gcp.Constants.TEST_HYBRID_PRIVATE_KEYSET;
import static com.google.cm.mrp.testutils.gcp.Constants.TEST_KEK_JSON;
import static com.google.cm.testutils.gcp.TestingContainer.TEST_RUNNER_HOSTNAME;
import static com.google.cm.util.ProtoUtils.getJsonFromProto;
import static com.google.cm.util.ProtoUtils.getProtoFromJson;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.truth.Truth.assertThat;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.scp.operator.protos.frontend.api.v1.ReturnCodeProto.ReturnCode.RETRIES_EXHAUSTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockserver.model.Format.LOG_ENTRIES;
import static org.mockserver.model.HttpError.error;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.Protocol.HTTP_1_1;
import static org.mockserver.model.Protocol.HTTP_2;
import static org.mockserver.verify.VerificationTimes.atLeast;
import static org.mockserver.verify.VerificationTimes.atMost;
import static org.mockserver.verify.VerificationTimes.exactly;
import static org.mockserver.verify.VerificationTimes.never;

import com.google.acai.Acai;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.json.Json;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cm.lookupserver.api.LookupProto;
import com.google.cm.lookupserver.api.LookupProto.DataRecord;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.LookupKey;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.lookupserver.api.LookupProto.MatchedDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchOutputDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchedOutputField;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.api.EncryptionMetadataProto;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.testutils.AeadKeyGenerator;
import com.google.cm.mrp.testutils.HybridKeyGenerator;
import com.google.cm.mrp.testutils.gcp.JobServiceGcpIntegrationTestModule;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse.Shard;
import com.google.cm.shared.api.errors.ErrorResponseProto.Details;
import com.google.cm.shared.api.errors.ErrorResponseProto.ErrorResponse;
import com.google.cm.testutils.gcp.CloudFunctionEmulator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.BaseEncoding;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.scp.operator.protos.frontend.api.v1.CreateJobRequestProto.CreateJobRequest;
import com.google.scp.operator.protos.frontend.api.v1.CreateJobResponseProto.CreateJobResponse;
import com.google.scp.operator.protos.frontend.api.v1.GetJobResponseProto.GetJobResponse;
import com.google.scp.operator.protos.frontend.api.v1.JobStatusProto.JobStatus;
import com.google.scp.shared.util.Base64Util;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;
import org.mockserver.model.Delay;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.MediaType;
import org.mockserver.model.Protocol;
import org.mockserver.socket.PortFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

@RunWith(JUnit4.class)
public final class MrpGcpIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(MrpGcpIntegrationTest.class);
  private static final Slf4jLogConsumer frontendLogConsumer =
      new Slf4jLogConsumer(logger).withPrefix("frontend").withSeparateOutputStreams();
  private static final Slf4jLogConsumer workerLogConsumer =
      new Slf4jLogConsumer(logger).withPrefix("worker").withSeparateOutputStreams();

  private static final String DATA_BUCKET = "integration-test-bucket";
  private static final String INPUT_PREFIX = "input";
  private static final String OUTPUT_PREFIX = "output";
  private static final String CREATE_JOB_API_PATH = "/v1alpha/createJob";
  private static final String GET_JOB_API_PATH = "/v1alpha/getJob";
  private static final String GET_SCHEME_API_PATH = "/v1/shardingschemes:latest";
  private static final String LOOKUP_API_PATH = "/v1/lookup";
  private static final String GET_JOB_JOB_REQUEST_ID_PARAM = "job_request_id";
  private static final String GENERIC_SCHEMA_JSON =
      "{\"columns\":["
          + "{\"columnName\":\"Email1\",\"columnAlias\":\"email\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"Phone1\",\"columnAlias\":\"phone\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"FirstName1\",\"columnAlias\":\"first_name\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"LastName1\",\"columnAlias\":\"last_name\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"ZipCode1\",\"columnAlias\":\"zip_code\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"CountryCode1\",\"columnAlias\":\"country_code\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1}"
          + "],\"dataFormat\":\"CSV\",\"skipHeaderRecord\":true}";
  private static final String PROTO_SCHEMA_JSON =
      "{\"columns\":["
          + "{\"columnName\":\"Email1\",\"columnAlias\":\"email\","
          + "\"columnType\":\"STRING\"},"
          + "{\"columnName\":\"Phone1\",\"columnAlias\":\"phone\","
          + "\"columnType\":\"STRING\"},"
          + "{\"columnName\":\"FirstName1\",\"columnAlias\":\"first_name\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"LastName1\",\"columnAlias\":\"last_name\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"ZipCode1\",\"columnAlias\":\"zip_code\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1},"
          + "{\"columnName\":\"CountryCode1\",\"columnAlias\":\"country_code\","
          + "\"columnType\":\"STRING\",\"columnGroup\":1}"
          + "],\"dataFormat\":\"SERIALIZED_PROTO\",\"skipHeaderRecord\":true}";
  private static final String GENERIC_DATA_CSV =
      "Email1,Phone1,FirstName1,LastName1,ZipCode1,CountryCode1\n"
          + "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=,"
          + "/2X0BO9Lm8qz/7DSzxLY7PStmHANhUl9VjiLVsjd5Wo=,"
          + "RcCL8imzm3wBlD6mOKePzlNgjFjD7+0/3gCtPCXLFgI=,"
          + "ShCExkbnQW3ci+ZaKUocUyr00qskEl1PsQ+s3BY0tPA=,"
          + "19898,US\n"
          + "SdKMJphS1g5K1jw4n7hCXVUf3nPpRS6r+MjkQf4JEHM=,"
          + "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=,"
          + "XvbxhrZ4flZrlRZvRn4tyZ0HXlc6sdawVlxw/ob7Azw=,"
          + "jXgCWSZiQSnh8qG3xUefUNel//ESOe+3ro5WQivyOgA=,"
          + "11821,US\n"
          + "x6cw9DrSWSbWjpfmxMfZ2cGDNS3fTqJspH1i8ouV5e8=,"
          + "R+Uw0FO5RUgQeX21OroOyEhYxEd/0rI7DlKLVA/UtvE=,"
          + "5K4fs+ajBGmX+UvZObJclI6kjxNkYs5rs5mA35SYvf8=,"
          + "7LZ+L7e9AZnng+DuIDdBIzlBy1uj78VIurPmGIZ24Is=,"
          + "83243,US\n"
          + ", ,, ,, \n";

  private static final String GENERIC_DATA_HEX_CSV =
      "Email1,Phone1,FirstName1,LastName1,ZipCode1,CountryCode1\n"
          + "a6a773b65adff2ed5a527d73b2092f3b6688a32c6c36e96c2c074ef0296d46fd,"
          + "ff65f404ef4b9bcab3ffb0d2cf12d8ecf4ad98700d85497d56388b56c8dde56a,"
          + "45c08bf229b39b7c01943ea638a78fce53608c58c3efed3fde00ad3c25cb1602,"
          + "4a1084c646e7416ddc8be65a294a1c532af4d2ab24125d4fb10facdc1634b4f0,"
          + "19898,US\n"
          + "49d28c269852d60e4ad63c389fb8425d551fde73e9452eabf8c8e441fe091073,"
          + "aeacedfdee21f8264f08a63dd4c242c3087e8ee1a966e4e533a7701ca5161eaa,"
          + "5ef6f186b6787e566b95166f467e2dc99d075e573ab1d6b0565c70fe86fb033c,"
          + "8d78025926624129e1f2a1b7c5479f50d7a5fff11239efb7ae8e56422bf23a00,"
          + "11821,US\n"
          + "c7a730f43ad25926d68e97e6c4c7d9d9c183352ddf4ea26ca47d62f28b95e5ef,"
          + "47e530d053b9454810797db53aba0ec84858c4477fd2b23b0e528b540fd4b6f1,"
          + "e4ae1fb3e6a3046997f94bd939b25c948ea48f136462ce6bb39980df9498bdff,"
          + "ecb67e2fb7bd0199e783e0ee203741233941cb5ba3efc548bab3e6188676e08b,"
          + "83243,US\n"
          + ", ,, ,, \n";
  private static final int NUM_GENERIC_DATA_FIELDS = 18;
  private static final Aead TEST_KEK_AEAD = getAeadFromJsonKeyset(TEST_KEK_JSON);
  private static final String WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON =
      "{\"columns\":["
          + "{\"column_name\":\"Email\",\"column_alias\":\"email\",\"encrypted\":true},"
          + "{\"column_name\":\"Phone\",\"column_alias\":\"phone\",\"encrypted\":true},"
          + "{\"column_name\":\"FirstName\",\"column_alias\":\"first_name\",\"encrypted\":true},"
          + "{\"column_name\":\"LastName\",\"column_alias\":\"last_name\",\"encrypted\":true},"
          + "{\"column_name\":\"ZipCode\",\"column_alias\":\"zip_code\"},"
          + "{\"column_name\":\"CountryCode\",\"column_alias\":\"country_code\"},"
          + "{\"column_name\":\"DEK\",\"column_alias\":\"encrypted_dek\"},"
          + "{\"column_name\":\"KEK\",\"column_alias\":\"kek_uri\"}"
          + "],\"dataFormat\":\"CSV\",\"skipHeaderRecord\":true}";

  private static final String WRAPPED_KEY_ENCRYPTED_FIELDS_AND_WIP_SCHEMA_JSON =
      "{\"columns\":["
          + "{\"column_name\":\"Email\",\"column_alias\":\"email\",\"encrypted\":true},"
          + "{\"column_name\":\"Phone\",\"column_alias\":\"phone\",\"encrypted\":true},"
          + "{\"column_name\":\"FirstName\",\"column_alias\":\"first_name\",\"encrypted\":true},"
          + "{\"column_name\":\"LastName\",\"column_alias\":\"last_name\",\"encrypted\":true},"
          + "{\"column_name\":\"ZipCode\",\"column_alias\":\"zip_code\"},"
          + "{\"column_name\":\"CountryCode\",\"column_alias\":\"country_code\"},"
          + "{\"column_name\":\"DEK\",\"column_alias\":\"encrypted_dek\"},"
          + "{\"column_name\":\"KEK\",\"column_alias\":\"kek_uri\"},"
          + "{\"column_name\":\"WIP\",\"column_alias\":\"wip_provider\"}"
          + "],\"dataFormat\":\"CSV\",\"skipHeaderRecord\":true}";
  private static final String WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER =
      "Email,Phone,FirstName,LastName,ZipCode,CountryCode,DEK,KEK";
  private static final String TEST_WIP = "testWip";
  private static final String COORD_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON =
      "{\"columns\":["
          + "{\"column_name\":\"Email\",\"column_alias\":\"email\",\"encrypted\":true},"
          + "{\"column_name\":\"Phone\",\"column_alias\":\"phone\",\"encrypted\":true},"
          + "{\"column_name\":\"FirstName\",\"column_alias\":\"first_name\",\"encrypted\":true},"
          + "{\"column_name\":\"LastName\",\"column_alias\":\"last_name\",\"encrypted\":true},"
          + "{\"column_name\":\"ZipCode\",\"column_alias\":\"zip_code\"},"
          + "{\"column_name\":\"CountryCode\",\"column_alias\":\"country_code\"},"
          + "{\"column_name\":\"CoordinatorKey\",\"column_alias\":\"coordinator_key_id\"}"
          + "],\"dataFormat\":\"CSV\",\"skipHeaderRecord\":true}";

  private static final String COORD_KEY_ENCRYPTED_FIELDS_CSV_HEADER =
      "Email,Phone,FirstName,LastName,ZipCode,CountryCode,CoordinatorKey";
  private static final String TEST_KS_ENDPOINT_1 = "testKeyServiceEndpoint1";
  private static final String TEST_KS_WIPP_1 = "testKmsWipProvider1";
  private static final String TEST_KS_AUDIENCE_URL_1 = "keyServiceAudienceUrl1";
  private static final String TEST_KS_ENDPOINT_2 = "testKeyServiceEndpoint2";
  private static final String TEST_KS_WIPP_2 = "testKmsWipProvider2";
  private static final String TEST_KS_AUDIENCE_URL_2 = "keyServiceAudienceUrl2";

  private static final HybridEncrypt TEST_HYBRID_ENCRYPT =
      getHybridEncryptFromJsonKeyset(TEST_HYBRID_PRIVATE_KEYSET, TEST_KEK_JSON);
  private static final HybridDecrypt TEST_HYBRID_DECRYPT =
      getHybridDecryptFromJsonKeyset(TEST_HYBRID_PRIVATE_KEYSET, TEST_KEK_JSON);
  private static final String GTAG_ENCRYPTED_SCHEMA_JSON =
      "{\"columns\":[{\"column_name\":\"coordinator_key_id\",\"column_type\":\"STRING\",\"column_alias\":\"coordinator_key_id\"},"
          + "{\"column_name\":\"gtag_grouped_pii\",\"column_type\":\"STRING\",\"column_alias\":\"gtag_grouped_pii\","
          + "\"encrypted\":true,\"column_encoding\":\"BASE64_URL\",\"column_format\":\"GTAG\","
          + "\"nested_schema\":{\"columns\":["
          + "{\"column_name\":\"em\",\"column_type\":\"STRING\",\"column_alias\":\"email\",\"encrypted\":true},"
          + "{\"column_name\":\"pn\",\"column_type\":\"STRING\",\"column_alias\":\"phone\",\"encrypted\":true},"
          + "{\"column_name\":\"fn\",\"column_type\":\"STRING\",\"column_alias\":\"first_name\",\"encrypted\":true,\"column_group\":0},"
          + "{\"column_name\":\"ln\",\"column_type\":\"STRING\",\"column_alias\":\"last_name\",\"encrypted\":true,\"column_group\":0},"
          + "{\"column_name\":\"pc\",\"column_type\":\"STRING\",\"column_alias\":\"zip_code\",\"column_group\":0},"
          + "{\"column_name\":\"co\",\"column_type\":\"STRING\",\"column_alias\":\"country_code\",\"column_group\":0},"
          + "{\"column_name\":\"error_codes\",\"column_type\":\"STRING\",\"column_alias\":\"error_codes\"}]"
          + ",\"data_format\":\"DATA_FORMAT_UNSPECIFIED\",\"skip_header_record\":false}},"
          + "{\"column_name\":\"metadata\",\"column_type\":\"STRING\",\"column_alias\":\"metadata\"}],"
          + "\"output_columns\":[{\"column\":{\"column_name\":\"coordinator_key_id\"}},"
          + "{\"condensed_column\":{\"column_name\":\"matched_grouped_pii\",\"condense_mode\":\"CONDENSE_COLUMNS_PROTO\","
          + "\"subcolumns\":[{\"column\":{\"column_name\":\"em\"}},{\"column\":{\"column_name\":\"pn\"}},"
          + "{\"composite_column\":{\"column_name\":\"address\",\"columns\":[{\"column_name\":\"fn\"}"
          + ",{\"column_name\":\"ln\"},{\"column_name\":\"pc\"},{\"column_name\":\"co\"}]}}"
          + ",{\"column\":{\"column_name\":\"error_codes\"}}]}},"
          + "{\"column\":{\"column_name\":\"metadata\"}},{\"column\":{\"column_name\":\"row_status\"}}],"
          + "\"data_format\":\"CSV\",\"skip_header_record\":true}";
  private static final String GTAG_CSV_HEADER = "coordinator_key_id,gtag_grouped_pii,metadata";
  private static final String GTAG_PATTERN = "tv.1~em.%s~pn.%s~fn0.%s~ln0.%s~co0.%s~pc0.%s";
  private static final String INVALID_SCHEME_RESPONSE_JSON =
      "{\"code\":3,\"message\":\"The request sharding scheme is not valid.\","
          + "\"details\":[{\"reason\":\"INVALID_SCHEME\",\"domain\":\"LookupService\"}]}";

  private static final int MOCKSERVER_PORT = PortFactory.findFreePort();

  private static final class TestEnv extends JobServiceGcpIntegrationTestModule {
    @Override
    public int getMockServerPort() {
      return MOCKSERVER_PORT;
    }
  }

  // contains only emails and phones for now
  private static List<String> hashedPii;
  private static List<String> wrappedKeyEncryptedEmails;
  private static List<String> wrappedKeyEncryptedPhones;
  private static List<String> coordKeyEncryptedEmails;
  private static List<String> coordKeyEncryptedPhones;
  private static String wrappedEncryptedFieldsCsv;
  private static String coordKeyEncryptedFieldsCsv;
  private static String gtagEncryptedDataCsv;

  static {
    setupTestData();
  }

  @Rule public final Acai acai = new Acai(TestEnv.class);
  @Rule public final MockServerRule mockServerRule = new MockServerRule(this, MOCKSERVER_PORT);

  @Inject private GenericContainer<?> worker;
  @Inject private CloudFunctionEmulator frontend;
  @Inject private Storage gcsClient;
  @Inject private HttpRequestFactory httpRequestFactory;
  private MockServerClient mockClient; // Automatically set by the mock server rule

  private static boolean shouldInit = true; // Init some instance resources exactly once

  @Before
  public void setUp() {
    if (shouldInit) {
      frontend.followOutput(frontendLogConsumer);
      worker.followOutput(workerLogConsumer);
      gcsClient.create(BucketInfo.newBuilder(DATA_BUCKET).build());
      shouldInit = false;
    }
  }

  @After
  public void cleanUp() {
    logger.info(mockClient.retrieveRecordedRequestsAndResponses(request(), LOG_ENTRIES));
  }

  @Test
  public void createAndGet_noMatches_success() throws Exception {
    String gcsFolder = "noMatches";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches = ImmutableList.of();
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(matches));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    assertThat(actualLookupRequest.hasEncryptionKeyInfo()).isFalse();
  }

  @Test
  public void createAndGet_noMatches_encryptedWithWrappedKey_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "noMatchesWrappedEncrypted";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(wrappedEncryptedFieldsCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequest(gcsFolder, "customer_match");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson =
        getJsonFromProto(createResponseWithMatchedKeysAndFailures(List.of(), List.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    assertThat(actualNumUnmatched).isEqualTo(NUM_GENERIC_DATA_FIELDS);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    WrappedKeyInfo actualWrappedKeyInfo =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class)
            .getEncryptionKeyInfo()
            .getWrappedKeyInfo();
    assertThat(actualWrappedKeyInfo.getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(actualWrappedKeyInfo.getKmsWipProvider()).isEqualTo("testWip");
    assertThat(actualWrappedKeyInfo.getEncryptedDek()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKekKmsResourceId()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKmsIdentity()).isEmpty();
  }

  @Test
  public void createAndGet_noMatches_encryptedWithCoordKey_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "noMatchesCoordEncrypted";
    createGcsSchemaFile(gcsFolder, GTAG_ENCRYPTED_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(gtagEncryptedDataCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest = createCoordKeyEncryptedJobRequest(gcsFolder, "mic");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson =
        getJsonFromProto(createResponseWithMatchedKeysAndFailures(List.of(), List.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);

    for (int i = 1; i < outputFiles.get(0).split("\n").length; i++) {
      String outputCondensedString = outputFiles.get(0).split("\n")[i].split(",")[1];
      // special case for blank output
      if (outputCondensedString.length() < 6) {
        assertThat(expandCondensedResponseColumn(outputCondensedString)).isEqualTo(",,,,,");
      } else {
        assertThat(expandCondensedResponseColumn(outputCondensedString))
            .isEqualTo("UNMATCHED,UNMATCHED,UNMATCHED,UNMATCHED,UNMATCHED,UNMATCHED");
      }
    }

    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    // Verify that LookupRequest contains the hashed Pii.
    List<String> receivedHashedPii =
        actualLookupRequest.getDataRecordsList().stream()
            .map(
                record -> {
                  String encryptedKey = record.getLookupKey().getKey();
                  try {
                    return HybridKeyGenerator.decryptString(TEST_HYBRID_DECRYPT, encryptedKey);
                  } catch (Exception e) {
                    throw new IllegalStateException("Failed to decrypt the lookup key", e);
                  }
                })
            .collect(Collectors.toList());
    List<String> expectedPii =
        hashedPii.stream().filter(Predicate.not(String::isBlank)).collect(Collectors.toList());
    assertTrue(receivedHashedPii.containsAll(expectedPii));
    // Verify that LookupRequest contains valid coordinator key information.
    LookupProto.EncryptionKeyInfo.CoordinatorKeyInfo actualCoordKeyInfo =
        actualLookupRequest.getEncryptionKeyInfo().getCoordinatorKeyInfo();
    assertThat(actualCoordKeyInfo.getCoordinatorInfoCount()).isEqualTo(2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_2);
  }

  @Test
  public void createAndGet_someMatches_success() throws Exception {
    String gcsFolder = "someMatches";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            "/2X0BO9Lm8qz/7DSzxLY7PStmHANhUl9VjiLVsjd5Wo=",
            "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=",
            "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=");
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(matches));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    assertThat(actualLookupRequest.hasEncryptionKeyInfo()).isFalse();
  }

  @Test
  public void createAndGet_someMatchesHexHashed_success() throws Exception {
    String gcsFolder = "someMatchesHex";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=",
            "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=",
            hashString(
                "5K4fs+ajBGmX+UvZObJclI6kjxNkYs5rs5mA35SYvf8="
                    + "7LZ+L7e9AZnng+DuIDdBIzlBy1uj78VIurPmGIZ24Is="
                    + "us"
                    + "83243"));
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(matches));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_HEX_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder, "mic", "HEX"));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - 6; // 2 email/phone + 4 for an address
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    assertThat(actualLookupRequest.hasEncryptionKeyInfo()).isFalse();
  }

  @Test
  public void createAndGet_someMatchesJoinMode_success() throws Exception {
    String gcsFolder = "someMatchesJoinMode";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);

    List<String> hashMatches =
        ImmutableList.of(
            "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=",
            "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=",
            hashString(
                "5K4fs+ajBGmX+UvZObJclI6kjxNkYs5rs5mA35SYvf8="
                    + "7LZ+L7e9AZnng+DuIDdBIzlBy1uj78VIurPmGIZ24Is="
                    + "us"
                    + "83243"));
    List<AssociatedData> gaiaMatches =
        ImmutableList.of(
            AssociatedData.of("encrypted_gaia_id", copyFromUtf8("1111")),
            AssociatedData.of(
                "encrypted_gaia_id", List.of(copyFromUtf8("2222"), copyFromUtf8("3333"))),
            AssociatedData.of("encrypted_gaia_id", copyFromUtf8("4444")));
    var lookupResponse = createResponseWithMatchedKeysAndAssociatedData(hashMatches, gaiaMatches);
    String lookupResponseJson = getJsonFromProto(lookupResponse);
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, PROTO_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(setupHashedSerializedProtoData()));

    GetJobResponse response =
        createAndGet(
            createCreateJobRequest(
                gcsFolder,
                /* application= */ "mic",
                /* encodingType= */ "BASE64",
                /* mode= */ "JOIN"));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    var outputProtos =
        Arrays.stream(outputFiles.get(0).split("\n"))
            .map(this::base64DecodeProto)
            .collect(Collectors.toList());
    List<MatchKey> nonMatches =
        outputProtos.stream()
            .map(cfmProto -> getAllMatchKeys(cfmProto, false))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - 6; // 2 email/phone + 4 for an address
    AtomicInteger actualNumUnmatched = new AtomicInteger(); // atomic since lambda requires final
    nonMatches.forEach(
        matchKey -> {
          if (matchKey.hasField()) {
            assertThat(matchKey.getField().getMatchedOutputFieldsList()).isEmpty();
            assertThat(matchKey.getField().getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
            actualNumUnmatched.getAndIncrement();
          } else {
            assertThat(matchKey.getCompositeField().getMatchedOutputFieldsList()).isEmpty();
            matchKey
                .getCompositeField()
                .getChildFieldsList()
                .forEach(
                    cf -> {
                      assertThat(cf.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
                      actualNumUnmatched.getAndIncrement();
                    });
          }
        });
    assertThat(actualNumUnmatched.get()).isEqualTo(expectedNumUnmatched);
    List<MatchKey> matchResults =
        outputProtos.stream()
            .map(cfmProto -> getAllMatchKeys(cfmProto, true))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    assertThat(matchResults).hasSize(3);
    Set<String> processed = new HashSet<>(3);
    matchResults.forEach(
        match -> {
          // single field
          if (match.hasField()) {
            var field = match.getField();
            if (field.getKeyValue().getStringValue().equals(hashMatches.get(0))) {
              assertThat(field.getMatchedOutputFieldsList()).hasSize(1);
              var joinedField = field.getMatchedOutputFieldsList().get(0);
              assertThat(joinedField.getKeyValueCount()).isEqualTo(1);
              assertThat(joinedField.getKeyValue(0).getKey()).isEqualTo("encrypted_gaia_id");
              assertThat(fromBase64(joinedField.getKeyValue(0).getStringValue())).isEqualTo("1111");
              processed.add(field.getKeyValue().getStringValue());
            } else if (field.getKeyValue().getStringValue().equals(hashMatches.get(1))) {
              assertThat(field.getMatchedOutputFieldsList()).hasSize(2);
              var sortedMatchedGaias = sortFieldByGaia(field.getMatchedOutputFieldsList());
              var joinedField = sortedMatchedGaias.get(0);
              assertThat(joinedField.getKeyValueCount()).isEqualTo(1);
              assertThat(joinedField.getKeyValue(0).getKey()).isEqualTo("encrypted_gaia_id");
              assertThat(fromBase64(joinedField.getKeyValue(0).getStringValue())).isEqualTo("2222");
              joinedField = sortedMatchedGaias.get(1);
              assertThat(joinedField.getKeyValueCount()).isEqualTo(1);
              assertThat(joinedField.getKeyValue(0).getKey()).isEqualTo("encrypted_gaia_id");
              assertThat(fromBase64(joinedField.getKeyValue(0).getStringValue())).isEqualTo("3333");
              processed.add(field.getKeyValue().getStringValue());
            } else {
              fail("field does not match");
            }
          }
          // composite field
          else {
            var field = match.getCompositeField();
            var addressNames =
                ImmutableList.of("FirstName1", "LastName1", "ZipCode1", "CountryCode1");
            assertThat(match.getCompositeField().getChildFieldsList()).hasSize(4);
            var orderedChildList =
                field.getChildFieldsList().stream()
                    .sorted(
                        Comparator.comparingInt(
                            cf -> addressNames.indexOf(cf.getKeyValue().getKey())))
                    .map(cf -> cf.getKeyValue().getStringValue())
                    .collect(Collectors.toList());
            assertThat(orderedChildList.get(0))
                .isEqualTo("5K4fs+ajBGmX+UvZObJclI6kjxNkYs5rs5mA35SYvf8=");
            assertThat(orderedChildList.get(1))
                .isEqualTo("7LZ+L7e9AZnng+DuIDdBIzlBy1uj78VIurPmGIZ24Is=");
            assertThat(orderedChildList.get(2)).isEqualTo("83243");
            assertThat(orderedChildList.get(3)).isEqualTo("US");

            assertThat(field.getMatchedOutputFieldsList()).hasSize(1);
            var joinedField = field.getMatchedOutputFieldsList().get(0);
            assertThat(joinedField.getKeyValueCount()).isEqualTo(1);
            assertThat(joinedField.getKeyValue(0).getKey()).isEqualTo("encrypted_gaia_id");
            assertThat(fromBase64(joinedField.getKeyValue(0).getStringValue())).isEqualTo("4444");
            processed.add(String.join("", orderedChildList));
          }
        });
    assertThat(processed).hasSize(3);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    assertThat(actualLookupRequest.hasEncryptionKeyInfo()).isFalse();
    assertThat(actualLookupRequest.getAssociatedDataKeysList())
        .containsExactly("encrypted_gaia_id");
  }

  @Test
  public void createAndGet_someMatches_encryptedWithWrappedKey_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "someMatchesEncrypted";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(wrappedEncryptedFieldsCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequest(gcsFolder, "customer_match");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        List.of(
            wrappedKeyEncryptedEmails.get(0),
            wrappedKeyEncryptedEmails.get(1),
            wrappedKeyEncryptedPhones.get(0),
            wrappedKeyEncryptedPhones.get(1));
    String lookupResponseJson =
        getJsonFromProto(createResponseWithMatchedKeysAndFailures(matches, List.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    WrappedKeyInfo actualWrappedKeyInfo =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class)
            .getEncryptionKeyInfo()
            .getWrappedKeyInfo();
    assertThat(actualWrappedKeyInfo.getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(actualWrappedKeyInfo.getKmsWipProvider()).isEqualTo("testWip");
    assertThat(actualWrappedKeyInfo.getEncryptedDek()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKekKmsResourceId()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKmsIdentity()).isEmpty();
  }

  @Test
  public void createAndGet_someMatches_encryptedWithWrappedKeyAndRowWip_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "rowWipSomeMatchesEncrypted";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_AND_WIP_SCHEMA_JSON);
    String[] lines = wrappedEncryptedFieldsCsv.split("\n");
    lines[0] = lines[0] + "," + "WIP" + "\n";
    for (int i = 1; i < lines.length; ++i) {
      lines[i] = lines[i] + "," + TEST_WIP + "\n";
    }
    createGcsDataFiles(gcsFolder, List.of(String.join("", lines)));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequestWithWip(gcsFolder, "mic", "");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        List.of(
            wrappedKeyEncryptedEmails.get(0),
            wrappedKeyEncryptedEmails.get(1),
            wrappedKeyEncryptedPhones.get(0),
            wrappedKeyEncryptedPhones.get(1));
    String lookupResponseJson =
        getJsonFromProto(createResponseWithMatchedKeysAndFailures(matches, List.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    WrappedKeyInfo actualWrappedKeyInfo =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class)
            .getEncryptionKeyInfo()
            .getWrappedKeyInfo();
    assertThat(actualWrappedKeyInfo.getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(actualWrappedKeyInfo.getKmsWipProvider()).isEqualTo(TEST_WIP);
    assertThat(actualWrappedKeyInfo.getEncryptedDek()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKekKmsResourceId()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKmsIdentity()).isEmpty();
  }

  @Test
  public void createAndGet_someMatches_encryptedWithCoordKey_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "someMatchesEncryptedCoord";
    createGcsSchemaFile(gcsFolder, COORD_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(coordKeyEncryptedFieldsCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest = createCoordKeyEncryptedJobRequest(gcsFolder, "mic");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        List.of(
            coordKeyEncryptedEmails.get(0),
            coordKeyEncryptedEmails.get(1),
            coordKeyEncryptedPhones.get(0),
            coordKeyEncryptedPhones.get(1));
    String lookupResponseJson =
        getJsonFromProto(createResponseWithMatchedKeysAndFailures(matches, List.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);

    // Verify that LookupRequest contains valid coordinator key information.
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupProto.EncryptionKeyInfo.CoordinatorKeyInfo actualCoordKeyInfo =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class)
            .getEncryptionKeyInfo()
            .getCoordinatorKeyInfo();
    assertThat(actualCoordKeyInfo.getCoordinatorInfoCount()).isEqualTo(2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_2);
  }

  @Test
  public void createAndGet_allMatches_encryptedWithWrappedKeyAndHexEncoded_success()
      throws Exception {
    // Setup GCS input files
    String gcsFolder = "allMatchesHexEncrypted";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    String hexEncryptedLines = setupHexEncryptedData();
    createGcsDataFiles(gcsFolder, List.of(hexEncryptedLines));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequestWithEncoding(gcsFolder, "mic_audience", "HEX");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient
        .when(lookupRequest)
        .respond(
            request -> {
              LookupRequest receivedLookupRequest =
                  getProtoFromJson(request.getBodyAsString(), LookupRequest.class);
              ImmutableList<LookupResult> results =
                  receivedLookupRequest.getDataRecordsList().stream()
                      .map(DataRecord::getLookupKey)
                      .map(
                          lookupKey ->
                              LookupResult.newBuilder()
                                  .setStatus(STATUS_SUCCESS)
                                  .setClientDataRecord(
                                      DataRecord.newBuilder().setLookupKey(lookupKey))
                                  .addMatchedDataRecords(
                                      MatchedDataRecord.newBuilder().setLookupKey(lookupKey))
                                  .build())
                      .collect(toImmutableList());
              return getMockServerResponse(
                  200,
                  getJsonFromProto(
                      LookupResponse.newBuilder().addAllLookupResults(results).build()));
            });

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    assertThat(actualNumUnmatched).isEqualTo(0); // all matches
    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    WrappedKeyInfo actualWrappedKeyInfo =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class)
            .getEncryptionKeyInfo()
            .getWrappedKeyInfo();
    assertThat(actualWrappedKeyInfo.getKeyType()).isEqualTo(KEY_TYPE_XCHACHA20_POLY1305);
    assertThat(actualWrappedKeyInfo.getKmsWipProvider()).isEqualTo("testWip");
    assertThat(actualWrappedKeyInfo.getEncryptedDek()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKekKmsResourceId()).isNotEmpty();
    assertThat(actualWrappedKeyInfo.getKmsIdentity()).isEmpty();

    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    // Verify that LookupRequest contains the hashed Pii.
    List<String> receivedHashedPii =
        actualLookupRequest.getDataRecordsList().stream()
            .map(
                record -> {
                  String encryptedKey = record.getLookupKey().getKey();
                  try {
                    Aead dek =
                        decryptEncryptedKeyset(
                            TEST_KEK_AEAD, actualWrappedKeyInfo.getEncryptedDek());
                    return AeadKeyGenerator.decryptString(dek, encryptedKey);
                  } catch (Exception e) {
                    throw new IllegalStateException("Failed to decrypt the lookup key", e);
                  }
                })
            .collect(Collectors.toList());
    List<String> expectedPii =
        hashedPii.stream().filter(Predicate.not(String::isBlank)).collect(Collectors.toList());
    assertTrue(receivedHashedPii.containsAll(expectedPii));
  }

  @Test
  public void createAndGet_allMatches_micEncryptedWithCoordKey_success() throws Exception {
    // Setup GCS input files
    String gcsFolder = "allMatchesCoordEncrypted";
    createGcsSchemaFile(gcsFolder, GTAG_ENCRYPTED_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(gtagEncryptedDataCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest = createCoordKeyEncryptedJobRequest(gcsFolder, "mic");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient
        .when(lookupRequest)
        .respond(
            request -> {
              LookupRequest receivedLookupRequest =
                  getProtoFromJson(request.getBodyAsString(), LookupRequest.class);
              ImmutableList<LookupResult> results =
                  receivedLookupRequest.getDataRecordsList().stream()
                      .map(DataRecord::getLookupKey)
                      .map(
                          lk ->
                              LookupResult.newBuilder()
                                  .setStatus(STATUS_SUCCESS)
                                  .setClientDataRecord(DataRecord.newBuilder().setLookupKey(lk))
                                  .addMatchedDataRecords(
                                      MatchedDataRecord.newBuilder().setLookupKey(lk))
                                  .build())
                      .collect(toImmutableList());
              return getMockServerResponse(
                  200,
                  getJsonFromProto(
                      LookupResponse.newBuilder().addAllLookupResults(results).build()));
            });

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);

    String[] originalPiiList = GENERIC_DATA_CSV.replace(",US", ",us").split("\n");
    originalPiiList[originalPiiList.length - 1] = ",,,,,";
    Set<String> inputHashedPiiList =
        new HashSet<>(
            Arrays.asList(Arrays.copyOfRange(originalPiiList, 1, originalPiiList.length)));
    Set<String> matchedHashedPiiList = new HashSet<>();
    for (int i = 1; i < outputFiles.get(0).split("\n").length; i++) {
      String outputCondensedString = outputFiles.get(0).split("\n")[i].split(",")[1];
      matchedHashedPiiList.add(expandCondensedResponseColumn(outputCondensedString));
    }
    assertThat(matchedHashedPiiList).isEqualTo(inputHashedPiiList);

    // Verify that requests to the lookup service contain the expected encryption info
    HttpRequest[] actualLookupRequests = mockClient.retrieveRecordedRequests(lookupRequest);
    assertThat(actualLookupRequests).hasLength(1);
    LookupRequest actualLookupRequest =
        getProtoFromJson(actualLookupRequests[0].getBodyAsString(), LookupRequest.class);
    // Verify that LookupRequest contains the hashed Pii.
    List<String> receivedHashedPii =
        actualLookupRequest.getDataRecordsList().stream()
            .map(
                record -> {
                  String encryptedKey = record.getLookupKey().getKey();
                  try {
                    return HybridKeyGenerator.decryptString(TEST_HYBRID_DECRYPT, encryptedKey);
                  } catch (Exception e) {
                    throw new IllegalStateException("Failed to decrypt the lookup key", e);
                  }
                })
            .collect(Collectors.toList());
    List<String> expectedPii =
        hashedPii.stream().filter(Predicate.not(String::isBlank)).collect(Collectors.toList());
    assertTrue(receivedHashedPii.containsAll(expectedPii));
    // Verify that LookupRequest contains valid coordinator key information.
    LookupProto.EncryptionKeyInfo.CoordinatorKeyInfo actualCoordKeyInfo =
        actualLookupRequest.getEncryptionKeyInfo().getCoordinatorKeyInfo();
    assertThat(actualCoordKeyInfo.getCoordinatorInfoCount()).isEqualTo(2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(0).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_1);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceEndpoint())
        .isEqualTo(TEST_KS_ENDPOINT_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKmsWipProvider())
        .isEqualTo(TEST_KS_WIPP_2);
    assertThat(actualCoordKeyInfo.getCoordinatorInfo(1).getKeyServiceAudienceUrl())
        .isEqualTo(TEST_KS_AUDIENCE_URL_2);
  }

  @Test
  public void createAndGet_partialSuccessFromLookupServiceNotAllowed_retryThenFailure()
      throws Exception {
    String gcsFolder = "FailedJobLookupServicePartialSuccessNotAllowed";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            wrappedKeyEncryptedEmails.get(0),
            wrappedKeyEncryptedEmails.get(1),
            wrappedKeyEncryptedEmails.get(2));
    List<String> failures = ImmutableList.of(wrappedKeyEncryptedPhones.get(0));
    var lookupResponse =
        createResponseWithMatchedKeysAndFailures(matches, failures/* encrypted= */ );
    mockClient
        .when(lookupRequest)
        .respond(getMockServerResponse(200, getJsonFromProto(lookupResponse)));
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(wrappedEncryptedFieldsCsv));

    GetJobResponse response =
        createAndGet(createWrappedKeyEncryptedJobRequest(gcsFolder, "customer_match"));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, exactly(2)); // 2 job-level retries
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_partialSuccessFromLookupServiceAllowed_success() throws Exception {
    String gcsFolder = "SuccessLookupServicePartialSuccessAllowed";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            wrappedKeyEncryptedEmails.get(0),
            wrappedKeyEncryptedEmails.get(1),
            wrappedKeyEncryptedEmails.get(2));
    List<String> failures = ImmutableList.of(wrappedKeyEncryptedPhones.get(0));
    var lookupResponse =
        createResponseWithMatchedKeysAndFailures(matches, failures/* encrypted= */ );
    mockClient
        .when(lookupRequest)
        .respond(getMockServerResponse(200, getJsonFromProto(lookupResponse)));
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(wrappedEncryptedFieldsCsv));

    GetJobResponse response = createAndGet(createWrappedKeyEncryptedJobRequest(gcsFolder, "copla"));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(PARTIAL_SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsCount()).isEqualTo(2);
    assertThat(
            response.getResultInfo().getErrorSummary().getErrorCountsList().stream()
                .anyMatch(
                    errorCount ->
                        errorCount.getCategory().equals("TOTAL_ERRORS")
                            && errorCount.getCount() == 1))
        .isTrue();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    var resultLines = outputFiles.get(0).split("\n");
    // 1 header + 4 rows
    assertThat(resultLines).hasLength(5);
    String[] headerFields = resultLines[0].split(",");
    // One more for status
    assertThat(headerFields)
        .hasLength(WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER.split(",").length + 1);
    assertThat(headerFields[headerFields.length - 1].strip()).isEqualTo("row_status");
    // First row should be errored
    String[] errorRowFields = resultLines[1].split(",");
    for (int i = 0; i < 6; i++) {
      assertThat(errorRowFields[i]).isEqualTo("ERROR");
    }
    assertThat(errorRowFields[headerFields.length - 1].strip()).isEqualTo("DECRYPTION_ERROR");
    // Row 1: Email matches only
    String[] row1Fields = resultLines[2].split(",");
    assertThat(row1Fields[0]).isEqualTo("SdKMJphS1g5K1jw4n7hCXVUf3nPpRS6r+MjkQf4JEHM=");
    for (int i = 1; i < 6; i++) {
      assertThat(row1Fields[i]).isEqualTo("UNMATCHED");
    }
    assertThat(row1Fields[headerFields.length - 1].strip()).isEqualTo("SUCCESS");
    // Row 2: Email matches only
    String[] row2Fields = resultLines[3].split(",");
    assertThat(row2Fields[0]).isEqualTo("x6cw9DrSWSbWjpfmxMfZ2cGDNS3fTqJspH1i8ouV5e8=");
    for (int i = 1; i < 6; i++) {
      assertThat(row2Fields[i]).isEqualTo("UNMATCHED");
    }
    assertThat(row1Fields[row2Fields.length - 1].strip()).isEqualTo("SUCCESS");
  }

  @Test
  public void createAndGet_noInputData_success() throws Exception {
    String gcsFolder = "emptyInput";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    // No input data file is created in GCS for this test

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, never());
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
  }

  @Test
  public void createAndGet_updateScheme_success() throws Exception {
    String gcsFolder = "updateScheme";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            "/2X0BO9Lm8qz/7DSzxLY7PStmHANhUl9VjiLVsjd5Wo=",
            "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=",
            "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=");
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(matches));
    HttpResponse lookupResponseGood = getMockServerResponse(200, lookupResponseJson);
    HttpResponse lookupResponseBad = getMockServerResponse(400, INVALID_SCHEME_RESPONSE_JSON);
    mockClient.when(lookupRequest, Times.exactly(1)).respond(lookupResponseBad);
    mockClient.when(lookupRequest).respond(lookupResponseGood);
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(2));
    mockClient.verify(lookupRequest, atLeast(2));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
    List<String> outputFiles = getGcsOutputFileContents(gcsFolder);
    assertThat(outputFiles).hasSize(1);
    int actualNumUnmatched = outputFiles.get(0).split("UNMATCHED").length - 1;
    int expectedNumUnmatched = NUM_GENERIC_DATA_FIELDS - matches.size();
    assertThat(actualNumUnmatched).isEqualTo(expectedNumUnmatched);
  }

  @Test
  public void createAndGet_badInputData_failure() throws Exception {
    String gcsFolder = "badInputData";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(",$,^,{}\n\t\\,54321,badData"));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(INVALID_INPUT_FILE_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_wrappedKeyCannotDecryptData_failure() throws Exception {
    // Setup GCS input files
    String gcsFolder = "cannotDecryptDataWrappedKey";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    // Prevent decryption with bad data
    String[] encryptedRow = wrappedEncryptedFieldsCsv.split("\n")[1].split(",");
    encryptedRow[0] = Base64.getEncoder().encodeToString("cannotDecrypt".getBytes());
    String badEncryptedData =
        String.join("\n", WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER, String.join(",", encryptedRow));
    createGcsDataFiles(gcsFolder, List.of(String.join("\n", badEncryptedData)));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequest(gcsFolder, "customer_match");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response that should never happen
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(DECRYPTION_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_wrappedKeyCannotDecryptDataPartialErrorTurnsToJobError_failure()
      throws Exception {
    // Setup GCS input files
    String gcsFolder = "wrappedKeyCannotDecryptDataPartialErrorTurnsToJobError";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    // Prevent decryption with bad data
    String[] encryptedRow = wrappedEncryptedFieldsCsv.split("\n")[1].split(",");
    encryptedRow[0] = Base64.getEncoder().encodeToString("cannotDecrypt".getBytes());
    String badEncryptedData =
        String.join("\n", WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER, String.join(",", encryptedRow));
    createGcsDataFiles(gcsFolder, List.of(String.join("\n", badEncryptedData)));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest = createWrappedKeyEncryptedJobRequest(gcsFolder, "copla");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response that should never happen
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(1);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(DECRYPTION_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsCount()).isEqualTo(2);
    assertThat(
            response.getResultInfo().getErrorSummary().getErrorCountsList().stream()
                .anyMatch(
                    errorCount ->
                        errorCount.getCategory().equals("TOTAL_ERRORS")
                            && errorCount.getCount() == 1))
        .isTrue();
  }

  @Test
  public void createAndGet_wrappedKeyOneRowAllBlank_partialSuccess() throws Exception {
    // Setup GCS input files
    String gcsFolder = "createAndGet_wrappedKeySomeRowsAllBlank_partialSuccess";
    createGcsSchemaFile(gcsFolder, WRAPPED_KEY_ENCRYPTED_FIELDS_SCHEMA_JSON);
    // Add a row with all blank columns to the standard wrapped key test data
    createGcsDataFiles(gcsFolder, List.of(wrappedEncryptedFieldsCsv + ", ,  ,, ,  , , "));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest = createWrappedKeyEncryptedJobRequest(gcsFolder, "copla");
    // Setup scheme response
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Setup lookup response
    List<String> matches =
        List.of(
            wrappedKeyEncryptedEmails.get(0),
            wrappedKeyEncryptedEmails.get(1),
            wrappedKeyEncryptedEmails.get(2));
    List<String> failures = List.of();
    LookupResponse lookupResponse = createResponseWithMatchedKeysAndFailures(matches, failures);
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient
        .when(lookupRequest)
        .respond(getMockServerResponse(200, getJsonFromProto(lookupResponse)));

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    // Verify that request was successful and the match count is expected
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(1);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(PARTIAL_SUCCESS.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsCount()).isEqualTo(2);
    assertThat(
            response.getResultInfo().getErrorSummary().getErrorCountsList().stream()
                .anyMatch(
                    errorCount ->
                        errorCount.getCategory().equals("TOTAL_ERRORS")
                            && errorCount.getCount() == 1))
        .isTrue();
  }

  @Test
  public void createAndGet_badSchema_failure() throws Exception {
    String gcsFolder = "badSchema";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, "notSchemaJson");
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode())
        .isEqualTo(INVALID_SCHEMA_FILE_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_badSchema_encryptedWithWrappedKey_failure() throws Exception {
    // Setup GCS input files
    String gcsFolder = "badSchemaWrappedKey";
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, List.of(wrappedEncryptedFieldsCsv));
    // Setup frontend request
    CreateJobRequest encryptedJobRequest =
        createWrappedKeyEncryptedJobRequest(gcsFolder, "customer_match");
    // Setup requests that should never happen
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);

    GetJobResponse response = createAndGet(encryptedJobRequest);

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode())
        .isEqualTo(MISSING_ENCRYPTION_COLUMN.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_noSchema_failure() throws Exception {
    String gcsFolder = "noSchema";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    // Schema file is not created in GCS for this test
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(MISSING_SCHEMA_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList()).isEmpty();
  }

  @Test
  public void createAndGet_missingGcsFolder_failure() throws Exception {
    String gcsFolder = "missingGcsFolder";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    // GCS folder and files are not created in this test

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(MISSING_SCHEMA_ERROR.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList()).isEmpty();
  }

  @Test
  public void createAndGet_missingGcsOutputBucket_retryThenFailure() throws Exception {
    String gcsFolder = "missingGcsOutputBucket";
    String outputBucket = "doesNotExist";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    // Lookup returns some matches, but the job still fails because of the missing output bucket
    // A missing folder in an existing bucket would not cause an error, the folder would be created
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    List<String> matches =
        ImmutableList.of(
            "/2X0BO9Lm8qz/7DSzxLY7PStmHANhUl9VjiLVsjd5Wo=",
            "rqzt/e4h+CZPCKY91MJCwwh+juGpZuTlM6dwHKUWHqo=",
            "pqdztlrf8u1aUn1zsgkvO2aIoyxsNulsLAdO8CltRv0=");
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(matches));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));
    String inputFolder = Path.of(INPUT_PREFIX, gcsFolder).toString();
    String outputFolder = Path.of(OUTPUT_PREFIX, outputBucket).toString();
    var dataOwners =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataLocation.newBuilder()
                            .setInputDataBucketName(DATA_BUCKET)
                            .setInputDataBlobPrefix(inputFolder)
                            .setIsStreamed(true)));
    var createJobRequest =
        CreateJobRequest.newBuilder()
            .setJobRequestId(UUID.randomUUID().toString())
            .setInputDataBucketName(DATA_BUCKET)
            .setInputDataBlobPrefix(inputFolder)
            .setOutputDataBucketName(outputBucket)
            .setOutputDataBlobPrefix(outputFolder)
            .setPostbackUrl("unused")
            .putJobParameters("application_id", "customer_match")
            .putJobParameters("data_owner_list", getJsonFromProto(dataOwners))
            .build();

    GetJobResponse response = createAndGet(createJobRequest);

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, atLeast(1));
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(JOB_RESULT_CODE_UNKNOWN.name());
    var outputFiles = gcsClient.list(DATA_BUCKET, Storage.BlobListOption.prefix(outputFolder));
    int numOutputFiles = Iterators.size(outputFiles.iterateAll().iterator());
    assertThat(numOutputFiles).isEqualTo(0);
  }

  @Test
  public void createAndGet_lookupHttpError_retryThenFailure() throws Exception {
    String gcsFolder = "lookupHttpError";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient.when(lookupRequest).respond(getMockServerResponse(500, ""));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, exactly(2)); // 2 job-level retries
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createAndGet_orchestratorHttpError_retryThenFailure() throws Exception {
    String gcsFolder = "orchestratorHttpError";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    mockClient.when(schemeRequest).respond(getMockServerResponse(500, ""));
    // Force a scheme refresh if the scheme is cached
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    HttpResponse lookupResponse = getMockServerResponse(400, INVALID_SCHEME_RESPONSE_JSON);
    mockClient.when(lookupRequest).respond(lookupResponse);
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    // 2 job-level retries
    mockClient.verify(schemeRequest, atMost(2));
    mockClient.verify(lookupRequest, atMost(2));
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
  }

  @Test
  public void createJob_blankRequestJson_failure() throws Exception {
    String gcsFolder = "blankRequestJson";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    var url = new GenericUrl("http://" + frontend.getHostEndpoint());
    url.setRawPath(CREATE_JOB_API_PATH);
    var content = ByteArrayContent.fromString(Json.MEDIA_TYPE, "");
    String response = httpRequestFactory.buildPostRequest(url, content).execute().parseAsString();

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    ErrorResponse errorResponse = getProtoFromJson(response, ErrorResponse.class);
    assertThat(errorResponse.getCode()).isEqualTo(3); // GRPC INVALID_ARGUMENT
    assertThat(errorResponse.getMessage()).contains("Expect message object but got: null");
    assertThat(errorResponse.getDetails(0).getReason()).isEqualTo("JSON_ERROR");
  }

  @Test
  public void createJob_badRequestJson_failure() throws Exception {
    String gcsFolder = "badRequestJson";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient.when(lookupRequest).respond(getMockServerResponse(200, lookupResponseJson));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    var url = new GenericUrl("http://" + frontend.getHostEndpoint());
    url.setRawPath(CREATE_JOB_API_PATH);
    var content = ByteArrayContent.fromString(Json.MEDIA_TYPE, "badJson");
    String response = httpRequestFactory.buildPostRequest(url, content).execute().parseAsString();

    mockClient.verify(schemeRequest, never());
    mockClient.verify(lookupRequest, never());
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    ErrorResponse errorResponse = getProtoFromJson(response, ErrorResponse.class);
    assertThat(errorResponse.getCode()).isEqualTo(3); // GRPC INVALID_ARGUMENT
    assertThat(errorResponse.getMessage()).contains("Expect message object but got: \"badJson\"");
    assertThat(errorResponse.getDetails(0).getReason()).isEqualTo("JSON_ERROR");
  }

  @Test
  public void createJob_lookupServerRetriableStatus_retryThenFailure() throws Exception {
    String gcsFolder = "lookupServerRetriableStatus";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient.when(lookupRequest).respond(getMockServerResponse(504, "DEADLINE_EXCEEDED"));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, exactly(4)); // 2 LS requests * 2 job retries
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createJob_lookupServerConnectionClosed_retryThenFailure() throws Exception {
    String gcsFolder = "lookupServerConnectionClosed";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    mockClient.when(lookupRequest, Times.exactly(4)).error(error().withDropConnection(true));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, exactly(4)); // 2 LS requests * 2 job retries
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  @Test
  public void createJob_lookupServerResponseTimeout_retryThenFailure() throws Exception {
    String gcsFolder = "lookupServerResponseTimeout";
    HttpRequest schemeRequest = getMockServerRequest("GET", GET_SCHEME_API_PATH, HTTP_1_1);
    String schemeResponseJson = getJsonFromProto(createSchemeResponse());
    mockClient.when(schemeRequest).respond(getMockServerResponse(200, schemeResponseJson));
    HttpRequest lookupRequest = getMockServerRequest("POST", LOOKUP_API_PATH, HTTP_2);
    String lookupResponseJson = getJsonFromProto(createResponseWithMatchedKeys(ImmutableList.of()));
    mockClient
        .when(lookupRequest, Times.exactly(4))
        .respond(getMockServerResponseWithDelay(200, lookupResponseJson, 3));
    createGcsSchemaFile(gcsFolder, GENERIC_SCHEMA_JSON);
    createGcsDataFiles(gcsFolder, ImmutableList.of(GENERIC_DATA_CSV));

    GetJobResponse response = createAndGet(createCreateJobRequest(gcsFolder));

    mockClient.verify(schemeRequest, atMost(1));
    mockClient.verify(lookupRequest, exactly(4)); // 2 LS requests * 2 job retries
    assertThat(getGcsOutputFileContents(gcsFolder)).hasSize(0);
    assertThat(response.getResultInfo().getReturnCode()).isEqualTo(RETRIES_EXHAUSTED.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorMessagesList())
        .contains(LOOKUP_SERVICE_FAILURE.name());
    assertThat(response.getResultInfo().getErrorSummary().getErrorCountsList()).isEmpty();
  }

  // Creates a job, polls for the job to finish, then returns the response
  private GetJobResponse createAndGet(CreateJobRequest request) throws Exception {
    String responseJson = callCreateJobApi(getJsonFromProto(request));
    var expectedResponse = CreateJobResponse.getDefaultInstance();
    if (!getProtoFromJson(responseJson, CreateJobResponse.class).equals(expectedResponse)) {
      throw new RuntimeException("Unexpected job creation response:\n" + responseJson);
    }
    return getFinishedJob(request.getJobRequestId());
  }

  private String callCreateJobApi(String body) throws Exception {
    var url = new GenericUrl("http://" + frontend.getHostEndpoint());
    url.setRawPath(CREATE_JOB_API_PATH);
    var content = ByteArrayContent.fromString(Json.MEDIA_TYPE, body);
    var response = httpRequestFactory.buildPostRequest(url, content).execute();
    if (!response.isSuccessStatusCode()) {
      throw new RuntimeException("Unexpected createJob HTTP status: " + response.getStatusCode());
    }
    return response.parseAsString();
  }

  private String callGetJobApi(String jobRequestId) throws Exception {
    var url = new GenericUrl("http://" + frontend.getHostEndpoint());
    url.setRawPath(GET_JOB_API_PATH);
    url.put(GET_JOB_JOB_REQUEST_ID_PARAM, jobRequestId);
    return httpRequestFactory.buildGetRequest(url).execute().parseAsString();
  }

  @SuppressWarnings("BusyWait") // Thread::sleep
  private GetJobResponse getFinishedJob(String jobId) throws Exception {
    Duration pollingDelay = Duration.of(5, ChronoUnit.SECONDS);
    Duration pollingTimeout = Duration.of(3, ChronoUnit.MINUTES);
    Instant timeToStop = Instant.now().plus(pollingTimeout).minus(pollingDelay);
    GetJobResponse result = getProtoFromJson(callGetJobApi(jobId), GetJobResponse.class);

    // Poll until getJob API responds with a finished job, or timeout
    while (!result.getJobStatus().equals(JobStatus.FINISHED)) {
      if (Instant.now().isAfter(timeToStop)) {
        throw new IllegalStateException("Exceeded timeout of " + pollingTimeout.toSeconds() + "s.");
      }

      Thread.sleep(pollingDelay.toMillis()); // 5 seconds
      result = getProtoFromJson(callGetJobApi(jobId), GetJobResponse.class);
    }

    return result;
  }

  private GetCurrentShardingSchemeResponse createSchemeResponse() {
    return GetCurrentShardingSchemeResponse.newBuilder()
        .setShardingSchemeId("GenericCurrentScheme")
        .setType("jch")
        .addShards(
            Shard.newBuilder()
                .setShardNumber(0)
                .setServerAddressUri(
                    "https://"
                        + TEST_RUNNER_HOSTNAME
                        + ":"
                        + mockClient.getPort()
                        + LOOKUP_API_PATH))
        .build();
  }

  private void createGcsSchemaFile(String prefix, String content) throws Exception {
    createGcsFile(prefix, "schema.json", content);
  }

  private void createGcsDataFiles(String prefix, List<String> contents) throws Exception {
    for (int i = 0; i < contents.size(); ++i) {
      createGcsFile(prefix, "input_" + i + ".csv", contents.get(i));
    }
  }

  private void createGcsFile(String prefix, String filename, String content) throws Exception {
    String path = Path.of(INPUT_PREFIX, prefix, filename).toString();
    var byteStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    gcsClient.createFrom(BlobInfo.newBuilder(DATA_BUCKET, path).build(), byteStream);
  }

  private List<String> getGcsOutputFileContents(String prefix) throws Exception {
    String outputFolder = Path.of(OUTPUT_PREFIX, prefix).toString() + '/';
    var allFiles = gcsClient.list(DATA_BUCKET, Storage.BlobListOption.prefix(outputFolder));
    var results = ImmutableList.<String>builder();
    for (var file : allFiles.iterateAll()) {
      if (file.getName().startsWith(outputFolder)) {
        byte[] rawBytes = Channels.newInputStream(file.reader()).readAllBytes();
        results.add(new String(rawBytes, StandardCharsets.UTF_8));
      }
    }
    return results.build();
  }

  private String expandCondensedResponseColumn(String condensedOutput) {
    Map<String, String> keyValueMap = new LinkedHashMap<>();
    keyValueMap.put("em", "");
    keyValueMap.put("pn", "");
    keyValueMap.put("fn", "");
    keyValueMap.put("ln", "");
    keyValueMap.put("co", "");
    keyValueMap.put("pc", "");
    try {
      CondensedResponseColumn outputProto =
          CondensedResponseColumn.parseFrom(Base64Util.fromBase64String(condensedOutput));
      outputProto
          .getColumnsList()
          .forEach(
              column -> {
                switch (column.getColumnTypeCase()) {
                  case COLUMN_GROUP:
                    column
                        .getColumnGroup()
                        .getSubcolumnsList()
                        .forEach(
                            keyValue ->
                                keyValueMap.replace(keyValue.getKey(), keyValue.getValue()));
                    break;
                  case COLUMN:
                    keyValueMap.replace(column.getColumn().getKey(), column.getColumn().getValue());
                    break;
                }
              });
    } catch (Exception e) {
      String message = "Failed to parse CondensedResponseColumn from string.";
      logger.error(message + e.getMessage());
      throw new RuntimeException(message, e);
    }
    return String.join(",", keyValueMap.values());
  }

  private static void setupTestData() {
    hashedPii = new ArrayList<>();
    wrappedKeyEncryptedEmails = new ArrayList<>();
    wrappedKeyEncryptedPhones = new ArrayList<>();
    coordKeyEncryptedEmails = new ArrayList<>();
    coordKeyEncryptedPhones = new ArrayList<>();
    try {
      String[] lines = GENERIC_DATA_CSV.split("\n");

      // wrappedKey data
      String testUri = generateAeadUri();
      var dekKeyset = generateXChaChaKeyset();
      Aead dekAead = dekKeyset.getPrimitive(Aead.class);
      String encryptedDek = encryptDek(dekKeyset, TEST_KEK_AEAD);
      List<String> wrappedEncryptedLines = new ArrayList<>(lines.length);
      wrappedEncryptedLines.add(WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER + "\n");

      // coordkey data
      List<String> coordKeyEncryptedLines = new ArrayList<>(lines.length);
      coordKeyEncryptedLines.add(COORD_KEY_ENCRYPTED_FIELDS_CSV_HEADER + "\n");

      // gtag data
      List<String> gtagEncryptedLines = new ArrayList<>(lines.length);
      gtagEncryptedLines.add(GTAG_CSV_HEADER + "\n");

      for (int i = 1; i < lines.length; i++) {
        String line = lines[i];
        String[] columns = line.split(",");
        hashedPii.add(columns[0]);
        hashedPii.add(columns[1]);

        // WrappedKey data
        String[] wrappedKeyEncryptedColumns = new String[columns.length + 2];
        wrappedKeyEncryptedColumns[0] = tryAeadEncryptString(dekAead, columns[0]);
        wrappedKeyEncryptedEmails.add(wrappedKeyEncryptedColumns[0]);
        wrappedKeyEncryptedColumns[1] = tryAeadEncryptString(dekAead, columns[1]);
        wrappedKeyEncryptedPhones.add(wrappedKeyEncryptedColumns[1]);
        wrappedKeyEncryptedColumns[2] = tryAeadEncryptString(dekAead, columns[2]);
        wrappedKeyEncryptedColumns[3] = tryAeadEncryptString(dekAead, columns[3]);
        wrappedKeyEncryptedColumns[4] = columns[4];
        wrappedKeyEncryptedColumns[5] = columns[5];
        wrappedKeyEncryptedColumns[6] = encryptedDek;
        wrappedKeyEncryptedColumns[7] = testUri;
        wrappedEncryptedLines.add(String.join(",", wrappedKeyEncryptedColumns) + "\n");

        // CoordKey data
        String[] coordKeyEncryptedColumns = new String[columns.length + 1];
        coordKeyEncryptedColumns[0] = tryHybridEncryptString(columns[0]);
        coordKeyEncryptedEmails.add(coordKeyEncryptedColumns[0]);
        coordKeyEncryptedColumns[1] = tryHybridEncryptString(columns[1]);
        coordKeyEncryptedPhones.add(coordKeyEncryptedColumns[1]);
        coordKeyEncryptedColumns[2] = tryHybridEncryptString(columns[2]);
        coordKeyEncryptedColumns[3] = tryHybridEncryptString(columns[3]);
        coordKeyEncryptedColumns[4] = columns[4];
        coordKeyEncryptedColumns[5] = columns[5];
        coordKeyEncryptedColumns[6] = "unusedCoordinatorKey";
        coordKeyEncryptedLines.add(String.join(",", coordKeyEncryptedColumns) + "\n");

        // GTAG data
        String[] newGtagColumns = new String[3];
        newGtagColumns[0] = "unusedCoordinatorKey";
        newGtagColumns[1] = getEncryptedGtagString(columns);
        newGtagColumns[2] = "testMetadata";
        gtagEncryptedLines.add(String.join(",", newGtagColumns) + "\n");
      }
      wrappedEncryptedFieldsCsv = String.join("", wrappedEncryptedLines);
      coordKeyEncryptedFieldsCsv = String.join("", coordKeyEncryptedLines);
      gtagEncryptedDataCsv = String.join("", gtagEncryptedLines);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Cannot encrypt test data", e);
    }
  }

  private static String setupHashedSerializedProtoData() {
    String[] lines = GENERIC_DATA_CSV.split("\n");
    String[] columnHeaders = lines[0].split(",");
    List<String> protoLines = new ArrayList<>(lines.length);

    for (int i = 1; i < lines.length; i++) {
      String line = lines[i];
      String[] columns = line.split(",");
      List<MatchKey> matchKeys = new ArrayList<>(columns.length);

      matchKeys.add(buildSingleField(columnHeaders[0], columns[0]));
      matchKeys.add(buildSingleField(columnHeaders[1], columns[1]));
      String[][] addressValues = {
        {columnHeaders[2], columns[2]},
        {columnHeaders[3], columns[3]},
        {columnHeaders[4], columns[4]},
        {columnHeaders[5], columns[5]}
      };
      matchKeys.add(buildCompositeField("address", addressValues));
      var proto = ConfidentialMatchDataRecord.newBuilder().addAllMatchKeys(matchKeys).build();
      protoLines.add(base64().encode(proto.toByteArray()));
    }
    return String.join("\n", protoLines);
  }

  private static MatchKey buildSingleField(String key, String value) {
    return MatchKey.newBuilder()
        .setField(
            Field.newBuilder().setKeyValue(KeyValue.newBuilder().setKey(key).setStringValue(value)))
        .build();
  }

  private static MatchKey buildCompositeField(String key, String[][] keyValueQuads) {
    var builder = CompositeField.newBuilder();
    builder.setKey(key);
    for (String[] keyValue : keyValueQuads) {
      builder.addChildFields(
          CompositeChildField.newBuilder()
              .setKeyValue(KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(keyValue[1])));
    }
    return MatchKey.newBuilder().setCompositeField(builder).build();
  }

  private static String setupHexEncryptedData() {
    try {
      // Encrypt base64 for now
      String[] lines = GENERIC_DATA_CSV.split("\n");

      // wrappedKey data
      String testUri = generateAeadUri();
      var dekKeyset = generateXChaChaKeyset();
      Aead dekAead = dekKeyset.getPrimitive(Aead.class);
      String encryptedDek = encryptDek(dekKeyset, TEST_KEK_AEAD);
      List<String> wrappedEncryptedLines = new ArrayList<>(lines.length);
      wrappedEncryptedLines.add(WRAPPED_KEY_ENCRYPTED_FIELDS_CSV_HEADER + "\n");

      for (int i = 1; i < lines.length; i++) {
        String line = lines[i];
        String[] columns = line.split(",");
        String[] wrappedKeyEncryptedColumns = new String[columns.length + 2];
        wrappedKeyEncryptedColumns[0] = tryAeadEncryptString(dekAead, columns[0], HEX);
        wrappedKeyEncryptedColumns[1] = tryAeadEncryptString(dekAead, columns[1], HEX);
        wrappedKeyEncryptedColumns[2] = tryAeadEncryptString(dekAead, columns[2], HEX);
        wrappedKeyEncryptedColumns[3] = tryAeadEncryptString(dekAead, columns[3], HEX);
        wrappedKeyEncryptedColumns[4] = columns[4];
        wrappedKeyEncryptedColumns[5] = columns[5];
        wrappedKeyEncryptedColumns[6] = encryptedDek;
        wrappedKeyEncryptedColumns[7] = testUri;
        wrappedEncryptedLines.add(String.join(",", wrappedKeyEncryptedColumns) + "\n");
      }
      return String.join("", wrappedEncryptedLines);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Cannot encrypt test data", e);
    }
  }

  private static String tryAeadEncryptString(Aead aead, String plaintext)
      throws GeneralSecurityException {
    return tryAeadEncryptString(aead, plaintext, BASE64);
  }

  private static String tryAeadEncryptString(Aead aead, String plaintext, EncodingType encodingType)
      throws GeneralSecurityException {
    if (plaintext.isBlank()) {
      return plaintext;
    } else {
      return encryptString(aead, plaintext, encodingType);
    }
  }

  private static String getEncryptedGtagString(String[] columns) {
    String taggingString =
        String.format(
            GTAG_PATTERN, columns[0], columns[1], columns[2], columns[3], columns[4], columns[5]);
    return tryHybridEncryptString(taggingString, true);
  }

  private static String tryHybridEncryptString(String input) {
    return tryHybridEncryptString(input, /* base64Url= */ false);
  }

  private static String tryHybridEncryptString(String input, boolean base64Url) {
    try {
      byte[] cipherText = TEST_HYBRID_ENCRYPT.encrypt(input.getBytes(UTF_8), new byte[0]);
      return base64Url
          ? BaseEncoding.base64Url().omitPadding().encode(cipherText)
          : base64().omitPadding().encode(cipherText);
    } catch (Exception e) {
      String message = "Failed to encrypt the string with Hybrid encrypt" + e.getMessage();
      logger.error(message);
      throw new IllegalStateException(message, e);
    }
  }

  private static LookupResponse createResponseWithMatchedKeys(List<String> hashMatches) {
    return createResponse(hashMatches, ImmutableList.of(), ImmutableList.of());
  }

  private static LookupResponse createResponseWithMatchedKeysAndAssociatedData(
      List<String> hashMatches, List<AssociatedData> associatedData) {
    return createResponse(hashMatches, ImmutableList.of(), associatedData);
  }

  private static LookupResponse createResponseWithMatchedKeysAndFailures(
      List<String> matches, List<String> failures) {
    return createResponse(matches, failures, ImmutableList.of());
  }

  private static LookupResponse createResponse(
      List<String> matches, List<String> failures, List<AssociatedData> associatedData) {
    List<String> lookupKeys = new ArrayList<>(matches);
    ImmutableList<LookupResult> results =
        IntStream.range(0, lookupKeys.size())
            .boxed()
            .map(
                idx -> {
                  var lk = LookupKey.newBuilder().setKey(lookupKeys.get(idx));
                  var lsResultBuilder =
                      LookupResult.newBuilder()
                          .setStatus(STATUS_SUCCESS)
                          .setClientDataRecord(DataRecord.newBuilder().setLookupKey(lk));
                  if (matches.contains(lk.getKey())) {
                    if (!associatedData.isEmpty()) {
                      associatedData
                          .get(idx)
                          .values()
                          .forEach(
                              val -> {
                                var lookupRecord = MatchedDataRecord.newBuilder().setLookupKey(lk);
                                lookupRecord.addAllAssociatedData(ImmutableList.of(val));
                                lsResultBuilder.addMatchedDataRecords(lookupRecord);
                              });
                    } else {
                      lsResultBuilder.addMatchedDataRecords(
                          MatchedDataRecord.newBuilder().setLookupKey(lk));
                    }
                  }
                  return lsResultBuilder.build();
                })
            .collect(toImmutableList());
    ImmutableList<LookupResult> failedResults =
        failures.stream()
            .map(k -> LookupKey.newBuilder().setKey(k))
            .map(
                lk ->
                    LookupResult.newBuilder()
                        .setClientDataRecord(DataRecord.newBuilder().setLookupKey(lk))
                        .setStatus(STATUS_FAILED)
                        .setErrorResponse(
                            ErrorResponse.newBuilder()
                                .setCode(3)
                                // only decryption errors can be received
                                .setMessage(
                                    "An encryption/decryption error occurred while processing the"
                                        + " request.")
                                .addDetails(
                                    Details.newBuilder()
                                        .setReason("2415853572")
                                        .setDomain("LookupService")))
                        .build())
            .collect(toImmutableList());
    return LookupResponse.newBuilder()
        .addAllLookupResults(results)
        .addAllLookupResults(failedResults)
        .build();
  }

  private static CreateJobRequest createCreateJobRequest(String prefix) throws Exception {
    return createCreateJobRequest(prefix, "customer_match");
  }

  private static CreateJobRequest createCreateJobRequest(String prefix, String applicationId)
      throws Exception {
    return createCreateJobRequest(prefix, applicationId, /* encodingType= */ "");
  }

  private static CreateJobRequest createCreateJobRequest(
      String prefix, String applicationId, String encodingType) throws Exception {
    return createCreateJobRequest(prefix, applicationId, encodingType, /* mode= */ "");
  }

  private static CreateJobRequest createCreateJobRequest(
      String prefix, String applicationId, String encodingType, String mode) throws Exception {
    String inputFolder = Path.of(INPUT_PREFIX, prefix).toString();
    String outputFolder = Path.of(OUTPUT_PREFIX, prefix).toString();
    var dataOwners =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataLocation.newBuilder()
                            .setInputDataBucketName(DATA_BUCKET)
                            .setInputDataBlobPrefix(inputFolder)
                            .setIsStreamed(true)));
    var request =
        CreateJobRequest.newBuilder()
            .setJobRequestId(UUID.randomUUID().toString())
            .setInputDataBucketName(DATA_BUCKET)
            .setInputDataBlobPrefix(inputFolder)
            .setOutputDataBucketName(DATA_BUCKET)
            .setOutputDataBlobPrefix(outputFolder)
            .putJobParameters("application_id", applicationId)
            .putJobParameters("data_owner_list", getJsonFromProto(dataOwners));

    if (!encodingType.isBlank()) {
      request.putJobParameters("encoding_type", encodingType);
    }
    if (!mode.isBlank()) {
      request.putJobParameters("mode", mode);
    }
    return request.build();
  }

  private static CreateJobRequest createCoordKeyEncryptedJobRequest(
      String prefix, String applicationId) throws Exception {
    var createJobRequest = createCreateJobRequest(prefix, applicationId);
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(
                        CoordinatorKeyInfo.newBuilder()
                            .addCoordinatorInfo(
                                CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(TEST_KS_ENDPOINT_1)
                                    .setKmsWipProvider(TEST_KS_WIPP_1)
                                    .setKeyServiceAudienceUrl(TEST_KS_AUDIENCE_URL_1))
                            .addCoordinatorInfo(
                                CoordinatorInfo.newBuilder()
                                    .setKeyServiceEndpoint(TEST_KS_ENDPOINT_2)
                                    .setKmsWipProvider(TEST_KS_WIPP_2)
                                    .setKeyServiceAudienceUrl(TEST_KS_AUDIENCE_URL_2))))
            .build();

    return createJobRequest.toBuilder()
        .putJobParameters("encryption_metadata", getJsonFromProto(encryptionMetadata))
        .build();
  }

  private static CreateJobRequest createWrappedKeyEncryptedJobRequest(
      String prefix, String applicationId) throws Exception {
    return createWrappedKeyEncryptedJobRequest(
        prefix, applicationId, TEST_WIP, /* encodingType= */ "");
  }

  private static CreateJobRequest createWrappedKeyEncryptedJobRequestWithWip(
      String prefix, String applicationId, String wip) throws Exception {
    return createWrappedKeyEncryptedJobRequest(prefix, applicationId, wip, /* encodingType= */ "");
  }

  private static CreateJobRequest createWrappedKeyEncryptedJobRequestWithEncoding(
      String prefix, String applicationId, String encodingType) throws Exception {
    return createWrappedKeyEncryptedJobRequest(prefix, applicationId, TEST_WIP, encodingType);
  }

  private static CreateJobRequest createWrappedKeyEncryptedJobRequest(
      String prefix, String applicationId, String wip, String encodingType) throws Exception {
    var encryptionMetadataBuilder =
        EncryptionMetadata.WrappedKeyInfo.newBuilder().setKeyType(XCHACHA20_POLY1305);
    if (!wip.isBlank()) {
      encryptionMetadataBuilder.setKmsWipProvider(wip);
    }

    var createJobRequest = createCreateJobRequest(prefix, applicationId, encodingType);
    var encryptionMetadata =
        EncryptionMetadataProto.EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(encryptionMetadataBuilder.build()))
            .build();

    return createJobRequest.toBuilder()
        .putJobParameters("encryption_metadata", getJsonFromProto(encryptionMetadata))
        .build();
  }

  private static HttpRequest getMockServerRequest(String method, String path, Protocol protocol) {
    return request().withMethod(method).withPath(path).withProtocol(protocol);
  }

  private static HttpResponse getMockServerResponse(int status, String body) {
    return getMockServerResponseWithDelay(status, body, 0);
  }

  private static HttpResponse getMockServerResponseWithDelay(
      int status, String body, int delaySec) {
    var response =
        HttpResponse.response()
            .withStatusCode(status)
            .withContentType(MediaType.APPLICATION_JSON_UTF_8)
            .withBody(body, StandardCharsets.UTF_8);
    if (delaySec > 0) {
      response.withDelay(Delay.seconds(delaySec));
    }
    return response;
  }

  private String hashString(String s) {
    return base64().encode(sha256().hashBytes(s.getBytes(UTF_8)).asBytes());
  }

  private ConfidentialMatchOutputDataRecord base64DecodeProto(String base64EncodedString) {
    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      return ConfidentialMatchOutputDataRecord.parseFrom(decodedBytes);
    } catch (Exception e) {
      fail("Failed to decode ConfidentialMatchOutputDataRecord: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  // get all match keys by checking the values to see if it contains "UNMATCHED"
  private List<MatchKey> getAllMatchKeys(ConfidentialMatchOutputDataRecord proto, boolean matched) {
    return proto.getMatchKeysList().stream()
        .filter(
            matchKey -> {
              if (matchKey.hasField()) {
                String value = matchKey.getField().getKeyValue().getStringValue();
                return matched ^ value.equals("UNMATCHED");
              } else {
                // Create a set of all the child fields. if they were unmatched, the set would only
                // have 1 value
                // of "UNMATCHED"
                String value =
                    String.join(
                        "",
                        matchKey.getCompositeField().getChildFieldsList().stream()
                            .map(childField -> childField.getKeyValue().getStringValue())
                            .collect(Collectors.toSet()));
                return matched ^ value.equals("UNMATCHED");
              }
            })
        .collect(Collectors.toList());
  }

  /** Sort MatchedOutputField results by the value of the gaia Ids */
  private List<MatchedOutputField> sortFieldByGaia(List<MatchedOutputField> inputList) {
    String gaiaKey = "encrypted_gaia_id";
    List<MatchedOutputField> mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        ((matchedOutputField0, matchedOutputField1) -> {
          var gaia0 =
              matchedOutputField0.getKeyValueList().stream()
                  .filter(field -> field.getKey().equals(gaiaKey))
                  .findFirst();
          var gaia1 =
              matchedOutputField1.getKeyValueList().stream()
                  .filter(field -> field.getKey().equals(gaiaKey))
                  .findFirst();
          String val0 = gaia0.map(KeyValue::getStringValue).orElse("yyy");
          String val1 = gaia1.map(KeyValue::getStringValue).orElse("zzz");
          return val0.compareTo(val1);
        }));
    return mutableList;
  }

  private String fromBase64(String encoded) {
    return ByteString.copyFrom(BaseEncoding.base64().decode(encoded)).toStringUtf8();
  }

  private static class AssociatedData {

    private static AssociatedData of(String key, ByteString value) {
      return of(key, ImmutableList.of(value));
    }

    private static AssociatedData of(String key, List<ByteString> values) {
      List<LookupProto.KeyValue> keyValues = new ArrayList<>();
      values.forEach(
          value -> {
            var kv = LookupProto.KeyValue.newBuilder().setKey(key).setBytesValue(value).build();
            keyValues.add(kv);
          });
      return new AssociatedData(keyValues);
    }

    private final List<LookupProto.KeyValue> values;

    AssociatedData(List<LookupProto.KeyValue> values) {
      this.values = values;
    }

    List<LookupProto.KeyValue> values() {
      return this.values;
    }
  }
}
