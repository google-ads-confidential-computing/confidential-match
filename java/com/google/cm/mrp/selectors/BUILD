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
    name = "selectors",
    srcs = glob(["*.java"]),
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/util",
        "//java/external:apache_httpclient5",
        "//java/external:apache_httpcore5",
        "//java/external:clients_blobstorageclient",
        "//java/external:clients_blobstorageclient_gcp",
        "//java/external:clients_configclient",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_jobclient_gcp",
        "//java/external:clients_lifecycleclient_gcp",
        "//java/external:clients_lifecycleclient_local",
        "//java/external:clients_metricclient",
        "//java/external:clients_metricclient_gcp",
        "//java/external:clients_notificationclient_gcp",
        "//java/external:clients_notificationclient_local",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:slf4j",
        "//protos/lookup_server/api:java_proto",
        "//protos/mrp/backend:java_proto",
        "//protos/shared/api/errors:java_proto",
    ],
)
