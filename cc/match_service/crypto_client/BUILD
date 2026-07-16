# Copyright 2026 Google LLC
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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//cc:__subpackages__"])

cc_library(
    name = "aead_crypto_key",
    srcs = ["aead_crypto_key.cc"],
    hdrs = ["aead_crypto_key.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:tink_cc",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_test(
    name = "aead_crypto_key_test",
    size = "small",
    srcs = ["aead_crypto_key_test.cc"],
    deps = [
        ":aead_crypto_key",
        ":crypto_key_interface",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_str_format",
        "//cc/external:google_gtest_main",
        "//cc/external:tink_cc",
        "//cc/external:tink_cc_mock_aead",
        "//cc/external:tink_cc_proto",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "hybrid_decrypt_crypto_key",
    srcs = ["hybrid_decrypt_crypto_key.cc"],
    hdrs = ["hybrid_decrypt_crypto_key.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/external:tink_cc_hybrid",
        "//cc/match_service/error",
    ],
)

cc_test(
    name = "hybrid_decrypt_crypto_key_test",
    srcs = ["hybrid_decrypt_crypto_key_test.cc"],
    deps = [
        ":hybrid_decrypt_crypto_key",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:tink_cc",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "crypto_key_interface",
    hdrs = ["crypto_key_interface.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//protos/core:cc_proto",
    ],
)

cc_library(
    name = "fake_crypto_key",
    hdrs = ["fake_crypto_key.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/external:absl_status",
        "//cc/external:google_gtest",
        "//protos/core:cc_proto",
    ],
)

cc_library(
    name = "mock_crypto_key",
    hdrs = ["mock_crypto_key.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/external:absl_status",
        "//cc/external:google_gtest",
        "//protos/core:cc_proto",
    ],
)

cc_library(
    name = "aead_crypto_client",
    srcs = ["aead_crypto_client.cc"],
    hdrs = ["aead_crypto_client.h"],
    deps = [
        ":aead_crypto_key",
        ":crypto_client_interface",
        ":tink_utils",
        "//cc/core/async",
        "//cc/core/error:status_macros",
        "//cc/core/logger",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:tink_cc",
        "//cc/external:tink_cc_cleartext_keyset_handle",
        "//cc/match_service/error",
        "//cc/match_service/kms_client:kms_client_interface",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "hybrid_crypto_client",
    srcs = ["hybrid_crypto_client.cc"],
    hdrs = ["hybrid_crypto_client.h"],
    deps = [
        ":crypto_client_interface",
        ":hybrid_decrypt_crypto_key",
        ":tink_utils",
        "//cc/external:absl_bind_front",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_dual_writing_metric_client",
        "//cc/external:scp_cpio_dual_writing_metric_client_interface",
        "//cc/external:scp_cpio_key_fetcher_interface",
        "//cc/external:scp_cpio_ondemand_key_fetcher",
        "//cc/external:scp_cpio_private_key_client_interface",
        "//cc/external:tink_cc_hpke",
        "//cc/external:tink_cc_hybrid",
        "//protos/match_service/backend:key_fetcher_options_cc_proto",
        "//protos/match_service/backend:private_key_endpoints_cc_proto",
    ],
)

cc_test(
    name = "hybrid_crypto_client_test",
    srcs = ["hybrid_crypto_client_test.cc"],
    deps = [
        ":hybrid_crypto_client",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_cpio_key_fetcher_interface",
        "//cc/external:scp_test_utils",
    ],
)

cc_library(
    name = "mock_crypto_client",
    hdrs = ["mock_crypto_client.h"],
    deps = [
        ":crypto_client_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:google_gtest",
        "//cc/external:scp_core_async_executor_mock",
        "//cc/external:scp_core_interface_lib",
        "//protos/core:cc_proto",
    ],
)

cc_library(
    name = "crypto_client_interface",
    hdrs = ["crypto_client_interface.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_interface_lib",
        "//protos/core:cc_proto",
    ],
)

cc_test(
    name = "aead_crypto_client_test",
    size = "small",
    srcs = ["aead_crypto_client_test.cc"],
    deps = [
        ":aead_crypto_client",
        ":aead_crypto_key",
        ":crypto_key_interface",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_str_format",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_client_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/external:tink_cc",
        "//cc/external:tink_cc_mock_aead",
        "//cc/external:tink_cc_proto",
        "//cc/match_service/error",
        "//cc/match_service/kms_client:kms_client_interface",
        "//cc/match_service/kms_client:mock_kms_client",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "tink_utils",
    srcs = ["tink_utils.cc"],
    hdrs = ["tink_utils.h"],
    deps = [
        ":crypto_key_interface",
        "//cc/external:tink_cc",
        "//cc/external:tink_cc_cleartext_keyset_handle",
        "//cc/match_service/error",
    ],
)

cc_test(
    name = "tink_utils_test",
    srcs = ["tink_utils_test.cc"],
    deps = [
        ":tink_utils",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_strings",
        "//cc/external:google_gtest_main",
        "//cc/external:tink_cc",
        "//cc/external:tink_cc_hpke",
    ],
)
