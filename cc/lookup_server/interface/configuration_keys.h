/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CC_LOOKUP_SERVER_INTERFACE_CONFIGURATION_KEYS_H_
#define CC_LOOKUP_SERVER_INTERFACE_CONFIGURATION_KEYS_H_

#include "absl/strings/string_view.h"

namespace google::confidential_match::lookup_server {

// Configuration key for the number of HTTP2 server threads.
inline constexpr char kTotalHttp2ServerThreadsCount[] =
    "http2server_threads_count";
// Configuration key for the max concurrent queue size for async executor.
inline constexpr char kAsyncExecutorQueueSize[] = "async_executor_queue_size";
// Configuration key for the number of async executor threads.
inline constexpr char kAsyncExecutorThreadsCount[] =
    "async_executor_threads_count";
// Configuration key for the max concurrent queue size for IO async executor.
inline constexpr char kIoAsyncExecutorQueueSize[] =
    "io_async_executor_queue_size";
// Configuration key for the number of IO async executor threads.
inline constexpr char kIoAsyncExecutorThreadsCount[] =
    "io_async_executor_threads_count";
// Configuration key for the host address used for Lookup Server.
inline constexpr char kLookupServiceHostAddress[] = "host_address";
// Configuration key for the port on which Lookup Server runs.
inline constexpr char kLookupServiceHostPort[] = "host_port";
// Configuration key for the port on which the healthcheck service runs.
inline constexpr char kHealthServicePort[] = "health_service_port";
// Configuration key for the list of log level types to be outputted.
inline constexpr char kEnabledLogLevels[] = "enabled_log_levels";
// Configuration key determining whether to enable HTTPS.
inline constexpr char kHttp2ServerUseTls[] = "http2_server_use_tls";
// Configuration key for the path to the private key file used for HTTPS.
inline constexpr char kHttp2ServerPrivateKeyFilePath[] =
    "http2_server_private_key_file_path";
// Configuration key for the path to the cert file used for HTTPS.
inline constexpr char kHttp2ServerCertificateFilePath[] =
    "http2_server_certificate_file_path";
// Configuration key for the address to the Orchestrator.
inline constexpr char kOrchestratorHostAddress[] = "orchestrator_host_address";
// Configuration key for the cluster group ID for this Lookup Server instance.
inline constexpr char kClusterGroupId[] = "cluster_group_id";
// Configuration key for the cluster ID for this Lookup Server instance.
inline constexpr char kClusterId[] = "cluster_id";
// Configuration key for environment name for this Lookup Server instance.
inline constexpr char kEnvironment[] = "environment";
// Configuration key for the KMS KEK name to use with this Lookup Server
// instance.
inline constexpr char kKmsResourceName[] = "kms_resource_name";
// Configuration key for the KMS region to use with this Lookup Server instance.
inline constexpr char kKmsRegion[] = "kms_region";
// Configuration key for the GCP Workload Identity Pool (WIP) provider for KMS
// permissions. See:
// https://cloud.google.com/iam/docs/workload-identity-federation#pools
inline constexpr char kKmsWipProvider[] = "kms_wip_provider";
// The audience to use for authentication of incoming request JWTs.
inline constexpr char kJwtAudience[] = "jwt_audience";
// The emails within JWTs that are authorized to send incoming requests.
inline constexpr char kJwtAllowedEmails[] = "jwt_allowed_emails";
// The JWT subjects that are authorized to send incoming requests.
// The values for this key are found in the `sub` field after decoding the
// JWT payload. JWTs can be decoded via a tool like https://jwt.io
inline constexpr char kJwtAllowedSubjects[] = "jwt_allowed_subjects";
// The amount of time to wait between data refreshes, in minutes.
inline constexpr char kDataRefreshIntervalMins[] = "data_refresh_interval_mins";
// Optional configuration key for the hash bucket count to use for storage.
// This determines the initial size of the OneTBB hash map, and is
// recommended to be set at map construction time for optimal performance.
inline constexpr char kStorageHashBucketCount[] = "storage_hash_bucket_count";
// The maximum number of files to stream concurrently when loading match data.
inline constexpr char kMaxConcurrentStreamedFileReads[] =
    "max_concurrent_streamed_file_reads";

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_CONFIGURATION_KEYS_H_
