# Copyright 2024 Google LLC
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

java_library(
    name = "dataprocessor",
    srcs = glob(["*.java"]),
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/cryptoclient/gcp",
        "//java/com/google/cm/mrp/clients/lookupserviceclient",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/destinations",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/preparers",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/mrp/dataprocessor/transformations",
        "//java/com/google/cm/mrp/dataprocessor/writers",
        "//java/com/google/cm/util",
        "//java/external:apache_commons_csv",
        "//java/external:apache_commons_lang",
        "//java/external:autovalue",
        "//java/external:autovalue_annotations",
        "//java/external:clients_blobstorageclient",
        "//java/external:clients_blobstorageclient_model",
        "//java/external:clients_metricclient",
        "//java/external:clients_metricclient_model",
        "//java/external:gcp_storage",
        "//java/external:google_api_client",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:guice-assistedinject",
        "//java/external:javax_inject",
        "//java/external:protobuf_java",
        "//java/external:resilience4j_core",
        "//java/external:resilience4j_retry",
        "//java/external:shared_util",
        "//java/external:slf4j",
        "//java/external:tink",
        "//protos/lookup_server/api:java_proto",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)
