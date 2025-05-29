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
    name = "aws",
    srcs = glob(["*.java"]),
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/clients/attestation",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/external:apache_commons_lang",
        "//java/external:apache_httpclient5",
        "//java/external:apache_httpcore5",
        "//java/external:aws_apache_http",
        "//java/external:aws_auth",
        "//java/external:aws_core",
        "//java/external:aws_http_client_spi",
        "//java/external:aws_kms",
        "//java/external:aws_sts",
        "//java/external:crypto_awskmsv2",
        "//java/external:google_http_client",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:guice-assistedinject",
        "//java/external:javax_inject",
        "//java/external:shared_crypto_tink",
        "//java/external:slf4j",
        "//java/external:tink",
        "//protos/mrp/backend:java_proto",
    ],
)
