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
    name = "lookupserviceclient",
    srcs = glob([
        "*.java",
        "model/*.java",
    ]),
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/lookupserviceclient/converters",
        "//java/com/google/cm/mrp/clients/orchestratorclient",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/selectors",
        "//java/com/google/cm/shared/api/model",
        "//java/com/google/cm/util",
        "//java/com/google/cm/util/gcp",
        "//java/external:apache_httpclient5",
        "//java/external:apache_httpcore5",
        "//java/external:autovalue",
        "//java/external:autovalue_annotations",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:protobuf_java",
        "//java/external:slf4j",
        "//protos/lookup_server/api:java_proto",
        "//protos/mrp/backend:java_proto",
        "//protos/orchestrator/api:java_proto",
        "//protos/shared/api/errors:java_proto",
    ],
)
