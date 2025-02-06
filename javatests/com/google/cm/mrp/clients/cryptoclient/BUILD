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

java_test(
    name = "AeadCryptoClientTest",
    srcs = ["AeadCryptoClientTest.java"],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:shared_crypto_tink",
        "//java/external:shared_util",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/testutils",
        "//javatests/com/google/cm/mrp/testutils/fakes",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "HybridCryptoClientTest",
    srcs = ["HybridCryptoClientTest.java"],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/util",
        "//java/external:clients_cryptoclient",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:shared_crypto_tink",
        "//java/external:shared_util",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/testutils",
        "//javatests/com/google/cm/mrp/testutils/fakes",
        "//protos/mrp/backend:java_proto",
    ],
)
