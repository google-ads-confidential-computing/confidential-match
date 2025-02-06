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
    name = "NoOpJobProcessorTest",
    srcs = ["NoOpJobProcessorTest.java"],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp/testing",
        "//java/external:clients_jobclient_gcp",  # includes testing job client
        "//java/external:clients_jobclient_model",
        "//java/external:google_truth",
        "//java/external:google_truth8",
    ],
)
