# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package(default_visibility = ["//visibility:public"])

java_test(
    name = "FeatureFlagProviderImplTest",
    srcs = ["FeatureFlagProviderImplTest.java"],
    deps = [
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_configclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:mockito",
    ],
)

java_test(
    name = "StartupConfigProviderImplTest",
    srcs = ["StartupConfigProviderImplTest.java"],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp:startup_config",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_configclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:mockito",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "MatchJobProcessorTest",
    srcs = ["MatchJobProcessorTest.java"],
    resources = [
        "testdata/invalid_job_params_input_data_paths_and_prefix.json",
        "testdata/invalid_job_params_input_data_paths_without_schema_path.json",
        "testdata/invalid_job_params_invalid_encryption_metadata.json",
        "testdata/invalid_job_params_lookup_endpoint.json",
        "testdata/invalid_job_params_multiple_data_owners.json",
        "testdata/invalid_job_params_multiple_wrapped_keys.json",
        "testdata/valid_job_params_encrypted.json",
        "testdata/valid_job_params_hashed.json",
        "testdata/valid_job_params_mic.json",
        "testdata/valid_job_params_with_input_data_paths_and_schema_path.json",
        "testdata/valid_job_params_with_input_data_prefix_and_schema_path.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/util",
        "//java/external:clients_jobclient_gcp",  # includes testing job client
        "//java/external:clients_jobclient_model",
        "//java/external:clients_metricclient",
        "//java/external:clients_metricclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:mockito",
        "//java/external:operator_backend_protos",
        "//java/external:operator_frontend_protos",
        "//java/external:protobuf_java",
        "//java/external:protobuf_java_util",
        "//java/external:shared_proto_util",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "MatchWorkerTest",
    srcs = ["MatchWorkerTest.java"],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp/selectors",
        "//java/com/google/cm/mrp/testing",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_jobclient_gcp",  # includes testing job client
        "//java/external:clients_jobclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:mockito",
    ],
)

java_test(
    name = "WorkerPullWorkServiceTest",
    srcs = ["WorkerPullWorkServiceTest.java"],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp/selectors",
        "//java/com/google/cm/mrp/testing",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_jobclient_gcp",  # includes testing job client
        "//java/external:clients_jobclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:mockito",
        "//java/external:operator_backend_protos",
        "//protos/mrp/api:proto",
    ],
)

java_test(
    name = "MrpGcpIntegrationTest",
    size = "large",
    srcs = ["MrpGcpIntegrationTest.java"],
    env = {"TESTCONTAINERS_RYUK_DISABLED": "true"},
    runtime_deps = ["//java/external:slf4j_simple"],
    deps = [
        "//java/com/google/cm/util",
        "//java/external:acai",
        "//java/external:gcp_storage",
        "//java/external:google_api_client",
        "//java/external:google_http_client",
        "//java/external:google_truth",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:mockserver_client",
        "//java/external:mockserver_core",
        "//java/external:mockserver_junit_rule",
        "//java/external:operator_frontend_protos",
        "//java/external:shared_proto",
        "//java/external:shared_util",
        "//java/external:slf4j",
        "//java/external:testcontainers",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/testutils",
        "//javatests/com/google/cm/mrp/testutils/gcp:JobServiceGcpIntegrationTestModule",
        "//javatests/com/google/cm/testutils/gcp",
        "//protos/lookup_server/api:java_proto",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
        "//protos/orchestrator/api:java_proto",
        "//protos/shared/api/errors:java_proto",
    ],
)
