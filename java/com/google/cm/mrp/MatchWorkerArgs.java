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

package com.google.cm.mrp;

import com.beust.jcommander.Parameter;
import com.google.cm.mrp.selectors.BlobStorageClientSelector;
import com.google.cm.mrp.selectors.ClientConfigSelector;
import com.google.cm.mrp.selectors.JobClientSelector;
import com.google.cm.mrp.selectors.LifecycleClientSelector;
import com.google.cm.mrp.selectors.LookupProtoFormatSelector;
import com.google.cm.mrp.selectors.MetricClientSelector;
import com.google.cm.mrp.selectors.NotificationClientSelector;
import com.google.cm.mrp.selectors.ParameterClientSelector;
import java.nio.file.Path;
import java.util.Optional;

/** CLI parameter definitions, with fields that should be loaded using JCommander. */
public final class MatchWorkerArgs {
  ///////////////////////////////
  // Module Selector Arguments
  ///////////////////////////////

  @Parameter(names = "--blob_storage_client", description = "Blob storage module selector.")
  private BlobStorageClientSelector blobStorageClientSelector = BlobStorageClientSelector.LOCAL_FS;

  @Parameter(names = "--client_config_env", description = "Client config module selector.")
  private ClientConfigSelector clientConfigSelector = ClientConfigSelector.NONE;

  @Parameter(names = "--job_client", description = "Job client module selector.")
  private JobClientSelector jobClientSelector = JobClientSelector.LOCAL_FILE;

  @Parameter(names = "--lifecycle_client", description = "Lifecycle client module selector.")
  private LifecycleClientSelector lifecycleClientSelector = LifecycleClientSelector.LOCAL;

  @Parameter(names = "--metric_client", description = "Metric client module selector.")
  private MetricClientSelector metricClientSelector = MetricClientSelector.LOCAL;

  @Parameter(names = "--notification_client", description = "Notification client implementation")
  private NotificationClientSelector notificationClientSelector = NotificationClientSelector.LOCAL;

  @Parameter(names = "--param_client", description = "Parameter client module selector.")
  private ParameterClientSelector parameterClientSelector = ParameterClientSelector.LOCAL_ARGS;

  @Parameter(
      names = "--local_file_worker_input_path",
      description = "Path to a file used as input for the local job client.")
  private String localJobClientInputFilePath = "";

  @Parameter(
      names = "--local_file_worker_output_path",
      description = "Path to a file used as output for the local job client. Optional.")
  private String localJobClientOutputFilePath = null;

  ///////////////////////////////
  // Job Service Arguments
  ///////////////////////////////

  @Parameter(
      names = "--gcp_job_max_num_attempts",
      description = "Maximum attempts allowed for a job. Defaults to 5. GCP only.")
  private int gcpJobMaxNumAttempts = 5;

  ///////////////////////////////
  // MRP Arguments
  ///////////////////////////////

  @Parameter(
      names = "--mrp_thread_pool_size",
      description = "MRP thread pool size. Defaults to 10.")
  private int mrpThreadPoolSize = 10;

  @Parameter(
      names = "--lookup_client_thread_pool_size",
      description = "Lookup client thread pool size. Defaults to 10.")
  private int lookupClientThreadPoolSize = 10;

  @Parameter(
      names = "--input_data_chunk_size",
      description = "Maximum number of records to read for each data chunk. Defaults to 1,000.")
  private int inputDataChunkSize = 1_000;

  @Parameter(
      names = "--lookup_client_cluster_group_id",
      description = "Cluster group ID used by the lookup service client.")
  private String lookupClientClusterGroupId = "";

  @Parameter(
      names = "--lookup_client_max_records_per_request",
      description = "Lookup client max records per request. Defaults to 1,000.")
  private int lookupClientMaxRecordsPerRequest = 1_000;

  @Parameter(
      names = "--lookup_client_max_retries",
      description = "Lookup client max retries per request. Defaults to 7.")
  private int lookupClientMaxRequestRetries = 7;

  @Parameter(names = "--orchestrator_endpoint", description = "Orchestrator endpoint.")
  private String orchestratorEndpoint = "";

  @Parameter(
      names = "--lookup_service_audience",
      description = "The audience of credentials present in requests to the lookup service.")
  private String lookupServiceAudience = "";

  @Parameter(
      names = "--max_records_per_output_file",
      description = "Max records per output file. Defaults to 1,000,000.")
  private int maxRecordsPerOutputFile = 1_000_000;

  @Parameter(
      names = "--lookup_proto_format",
      description = "Serialization format for protocol buffers sent in lookup requests.")
  private LookupProtoFormatSelector lookupProtoFormat = LookupProtoFormatSelector.JSON;

  @Parameter(
      names = "--job_processor_max_retries",
      description = "Max retries for retriable errors caught by the job processor. Defaults to 3.")
  private int jobProcessorMaxRetries = 3;

  @Parameter(
      names = "--job_queue_retry_delay_sec",
      description =
          "Delay to use when sending a job back to the JobQueue for retrying. Defaults to 600sec.")
  private int jobQueueRetryDelaySec = 600;

  @Parameter(
      names = "--aws_default_kms_audience",
      description = "The audience of credentials in AWS KMS operations.")
  private String awsDefaultKmsAudience = "";

  @Parameter(
      names = "--aws_signatures_list",
      description = "Comma-delineated list of signatures to send with AWS KMS operations.")
  private String awsKmsSignaturesList = "";

  public BlobStorageClientSelector getBlobStorageClientSelector() {
    return blobStorageClientSelector;
  }

  public ClientConfigSelector getClientConfigSelector() {
    return clientConfigSelector;
  }

  public JobClientSelector getJobClientSelector() {
    return jobClientSelector;
  }

  public LifecycleClientSelector getLifecycleClientSelector() {
    return lifecycleClientSelector;
  }

  public MetricClientSelector getMetricClientSelector() {
    return metricClientSelector;
  }

  public NotificationClientSelector getNotificationClientSelector() {
    return notificationClientSelector;
  }

  public ParameterClientSelector getParameterClientSelector() {
    return parameterClientSelector;
  }

  public Path getLocalJobClientInputFilePath() {
    return Path.of(localJobClientInputFilePath);
  }

  public Optional<Path> getLocalJobClientOutputFilePath() {
    return Optional.ofNullable(localJobClientOutputFilePath).map(Path::of);
  }

  public int getGcpJobMaxNumAttempts() {
    return gcpJobMaxNumAttempts;
  }

  public int getMrpThreadPoolSize() {
    return mrpThreadPoolSize;
  }

  public int getLookupClientThreadPoolSize() {
    return lookupClientThreadPoolSize;
  }

  public int getInputDataChunkSize() {
    return inputDataChunkSize;
  }

  public String getLookupClientClusterGroupId() {
    return lookupClientClusterGroupId;
  }

  public int getLookupClientMaxRecordsPerRequest() {
    return lookupClientMaxRecordsPerRequest;
  }

  public int getLookupClientMaxRequestRetries() {
    return lookupClientMaxRequestRetries;
  }

  public String getOrchestratorEndpoint() {
    return orchestratorEndpoint;
  }

  public String getLookupServiceAudience() {
    return lookupServiceAudience;
  }

  public int getMaxRecordsPerOutputFile() {
    return maxRecordsPerOutputFile;
  }

  public LookupProtoFormatSelector getLookupProtoFormat() {
    return lookupProtoFormat;
  }

  public int getJobProcessorMaxRetries() {
    return jobProcessorMaxRetries;
  }

  public int getJobQueueRetryDelaySec() {
    return jobQueueRetryDelaySec;
  }

  public String getAwsDefaultKmsAudience() {
    return awsDefaultKmsAudience;
  }

  public String getAwsKmsSignaturesList() {
    return awsKmsSignaturesList;
  }
}
