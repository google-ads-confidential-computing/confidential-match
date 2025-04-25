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
    name = "DataProcessorImplTest",
    srcs = ["DataProcessorImplTest.java"],
    resources = [
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/cryptoclient/gcp",
        "//java/com/google/cm/mrp/clients/lookupserviceclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/destinations",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/preparers",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/mrp/dataprocessor/writers",
        "//java/com/google/cm/mrp/models",
        "//java/com/google/cm/util",
        "//java/external:clients_blobstorageclient",
        "//java/external:clients_blobstorageclient_model",
        "//java/external:google_truth",
        "//java/external:guice",
        "//java/external:mockito",
        "//java/external:shared_crypto_tink",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "DataProcessorTaskTest",
    srcs = ["DataProcessorTaskTest.java"],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp/clients/lookupserviceclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/preparers",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/mrp/dataprocessor/writers",
        "//java/com/google/cm/mrp/selectors",
        "//java/external:mockito",
    ],
)

java_test(
    name = "DataMatcherImplTest",
    srcs = ["DataMatcherImplTest.java"],
    resources = [
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/transformations",
        "//java/com/google/cm/mrp/models",
        "//java/com/google/cm/util",
        "//java/external:mockito",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "LookupServerDataSourceTest",
    srcs = ["LookupServerDataSourceTest.java"],
    resources = [
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/lookupserviceclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/transformations",
        "//java/com/google/cm/mrp/models",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:google_truth8",
        "//java/external:mockito",
        "//java/external:protobuf_java",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/lookup_server/api:java_proto",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
        "//protos/shared/api/errors:java_proto",
    ],
)

java_test(
    name = "BlobStoreStreamDataSourceTest",
    srcs = ["BlobStoreStreamDataSourceTest.java"],
    resources = [
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/mrp/models",
        "//java/external:clients_blobstorageclient",
        "//java/external:clients_blobstorageclient_model",
        "//java/external:clients_metricclient",
        "//java/external:gcp_storage",
        "//java/external:google_api_client",
        "//java/external:google_http_client",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "MatchColumnsListTest",
    srcs = ["MatchColumnsListTest.java"],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/transformations",
        "//java/external:google_truth",
        "//java/external:google_truth_proto",
        "//java/external:mockito",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/backend:java_proto",
    ],
)
