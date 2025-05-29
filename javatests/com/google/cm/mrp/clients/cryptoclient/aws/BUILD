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
    name = "AwsAeadProviderTest",
    srcs = ["AwsAeadProviderTest.java"],
    deps = [
        "//java/com/google/cm/mrp/clients/attestation",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/cryptoclient/aws",
        "//java/external:apache_httpclient5",
        "//java/external:apache_httpcore5",
        "//java/external:aws_kms",
        "//java/external:aws_sts",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:protobuf_java",
        "//java/external:shared_crypto_tink",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/clients/testutils",
        "//protos/mrp/backend:java_proto",
    ],
)
