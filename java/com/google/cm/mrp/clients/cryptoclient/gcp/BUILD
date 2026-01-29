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

java_library(
    name = "gcp",
    srcs = glob(["*.java"]),
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/cryptoclient/exceptions",
        "//java/com/google/cm/mrp/clients/cryptoclient/utils",
        "//java/com/google/cm/util",
        "//java/external:autovalue",
        "//java/external:autovalue_annotations",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_cryptoclient",
        "//java/external:clients_metricclient",
        "//java/external:findbugs",
        "//java/external:google_auth_library_oauth2_http",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:guice-assistedinject",
        "//java/external:jackson_annotations",
        "//java/external:jackson_core",
        "//java/external:jackson_databind",
        "//java/external:javax_inject",
        "//java/external:protobuf_java",
        "//java/external:shared_api_util",
        "//java/external:shared_crypto_tink",
        "//java/external:shared_gcp_util",
        "//java/external:shared_mapper",
        "//java/external:shared_util",
        "//java/external:slf4j",
        "//java/external:tink",
        "//java/external:tink-gcpkms",
        "//protos/mrp/backend:java_proto",
    ],
)
