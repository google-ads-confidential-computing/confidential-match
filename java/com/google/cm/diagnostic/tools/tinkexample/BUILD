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
    name = "tinkexample",
    srcs = ["TinkExampleApplication.java"],
    deps = [
        "//java/external:apache_commons_csv",
        "//java/external:jcommander",
        "//java/external:tink",
        "//java/external:tink-gcpkms",
    ],
)

java_binary(
    name = "TinkExampleApplication",
    main_class = "com.google.cm.diagnostic.tools.tinkexample.TinkExampleApplication",
    runtime_deps = [":tinkexample"],
)

alias(
    name = "TinkExampleApplicationDeploy",
    actual = ":TinkExampleApplication_deploy.jar",
)

java_library(
    name = "tinkexampleaws",
    srcs = ["TinkExampleApplicationAws.java"],
    deps = [
        "//java/external:apache_commons_csv",
        "//java/external:aws_apache_http",
        "//java/external:aws_auth",
        "//java/external:aws_http_client_spi",
        "//java/external:crypto_awskmsv2",
        "//java/external:jcommander",
        "//java/external:tink",
    ],
)

java_binary(
    name = "TinkExampleApplicationAws",
    main_class = "com.google.cm.diagnostic.tools.tinkexample.TinkExampleApplicationAws",
    runtime_deps = [":tinkexampleaws"],
)

alias(
    name = "TinkExampleApplicationAwsDeploy",
    actual = ":TinkExampleApplicationAws_deploy.jar",
)
