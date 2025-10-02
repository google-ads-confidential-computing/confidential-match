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
    name = "LocalGcpFrontendHttpFunction",
    testonly = True,
    srcs = [
        "Constants.java",
        "LocalGcpFrontendHttpFunction.java",
    ],
    deps = [
        "//java/external:clients_configclient_gcp",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:jackson_databind",
        "//java/external:operator_backend_metadatadb_protos",
        "//java/external:operator_backend_protos",
        "//java/external:operator_dao_jobqueue_gcp",
        "//java/external:operator_dao_metadatadb_gcp",
        "//java/external:operator_frontend_protos",
        "//java/external:operator_frontend_service",
        "//java/external:operator_frontend_service_converter",
        "//java/external:operator_frontend_service_gcp",
        "//java/external:operator_frontend_tasks_gcp",
        "//java/external:shared_mapper",
    ],
)

java_binary(
    name = "LocalGcpFrontend",
    testonly = True,
    runtime_deps = [
        ":LocalGcpFrontendHttpFunction",
    ],
)

alias(
    name = "LocalGcpFrontendDeploy",
    actual = ":LocalGcpFrontend_deploy.jar",
)

java_library(
    name = "LocalGcpWorkerApplication",
    testonly = True,
    srcs = [
        "Constants.java",
        "LocalGcpWorkerApplication.java",
    ],
    deps = [
        "//java/com/google/cm/mrp:matchworker_runner",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/clients/lookupserviceclient",
        "//java/com/google/cm/mrp/clients/lookupserviceclient/gcp",
        "//java/com/google/cm/mrp/clients/orchestratorclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/selectors",
        "//java/com/google/cm/util",
        "//java/external:apache_httpclient5",
        "//java/external:apache_httpcore5",
        "//java/external:clients_blobstorageclient_gcp",
        "//java/external:clients_configclient_gcp",
        "//java/external:clients_cryptoclient",
        "//java/external:clients_jobclient_gcp",
        "//java/external:gcp_core",
        "//java/external:gcp_storage",
        "//java/external:google_http_client",
        "//java/external:guava",
        "//java/external:guice",
        "//java/external:guice-assistedinject",
        "//java/external:jackson_databind",
        "//java/external:mockserver_core",
        "//java/external:shared_crypto_tink",
        "//java/external:shared_mapper",
        "//java/external:tink",
        "//javatests/com/google/cm/mrp/testutils",
    ],
)

java_binary(
    name = "LocalGcpWorker",
    testonly = True,
    main_class = "com.google.cm.mrp.testutils.gcp.LocalGcpWorkerApplication",
    runtime_deps = [
        ":LocalGcpWorkerApplication",
        "//java/external:slf4j_simple",
    ],
)

alias(
    name = "LocalGcpWorkerDeploy",
    actual = ":LocalGcpWorker_deploy.jar",
)

java_library(
    name = "JobServiceGcpIntegrationTestModule",
    testonly = True,
    srcs = [
        "Constants.java",
        "JobServiceGcpIntegrationTestModule.java",
    ],
    runtime_deps = [
        ":LocalGcpFrontendDeploy",
        ":LocalGcpWorkerDeploy",
    ],
    deps = [
        "//java/external:google_http_client",
        "//java/external:guice",
        "//java/external:testcontainers",
        "//javatests/com/google/cm/testutils/gcp",
    ],
)
