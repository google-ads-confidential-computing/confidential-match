# Match Request Processor

MRP is a dataplane (DP) application and the main JVM process that runs
Confidential Match in a TEE environment.

## Building

Use this command to build the worker executable:

```bash
bazel build //java/com/google/cm/mrp:MatchWorkerRunner
```

## Running Locally

This script runs the MRP executable directly on the host machine:

```bash
# Set each flag to an appropriate value before running
bazel run //java/com/google/cm/mrp:MatchWorkerRunner -- \
    --blob_storage_client GCS|LOCAL_FS \
    --client_config_env GCP|NONE \
    --job_client LOCAL_FILE|GCP \
    --lifecycle_client GCP|LOCAL \
    --metric_client GCP|LOCAL \
    --param_client GCP|LOCAL_ARGS \
    --gcs_endpoint "https://gcs_endpoint" \
    --local_file_worker_input_path "input_path" \
    --local_file_worker_output_path "output_path" \
    --gcp_project_id "project_id" \
    --gcp_job_max_num_attempts 5 \
    --gcp_pubsub_endpoint "https://pubsub_endpoint" \
    --gcp_pubsub_max_message_byte_size 100000 \
    --gcp_pubsub_message_lease_seconds 60 \
    --gcp_pubsub_topic_id "topic_id" \
    --gcp_pubsub_subscription_id "pubsub_subscription_id" \
    --gcp_spanner_endpoint "https://spanner_endpoint" \
    --gcp_spanner_instance_id "spanner_instance_id" \
    --gcp_spanner_db_name "spanner_db_name" \
    --gcp_gce_instance_id_override "gce_instance_id" \
    --gcp_gce_instance_name_override "gce_instance_name" \
    --gcp_gce_instance_zone_override "gce_instance_zone"
```

## Example Requests

### CreateJob

This script makes a request to a deployed frontend to create a job, then the MRP
should see and process it:

```bash
INPUT_BUCKET=input_bucket_name    # existing bucket name
INPUT_PREFIX=input_folder_path    # existing folder path with one or more files
                                  # must contain schema.json
OUTPUT_BUCKET=output_bucket_name  # exiting bucket name
OUTPUT_PREFIX=output_folder_path  # will be created, if does not exist
JOB_REQUEST_ID=$(uuidgen)         # random, unique string

# Fill in these values for the deployed frontend
FRONTEND_SERVICE='https://yourFrontendService.a.run.app'
SERVICE_ACCOUNT='yourServiceAccount@yourSAProject.iam.gserviceaccount.com'

curl -v -X POST --oauth2-bearer "$(gcloud auth print-identity-token \
  --include-email --impersonate-service-account=${SERVICE_ACCOUNT})" \
  "${FRONTEND_SERVICE}/v1alpha/createJob" \
  -d "{
      \"jobRequestId\":\"${JOB_REQUEST_ID}\",
      \"accountIdentity\":\"${SERVICE_ACCOUNT}\",
      \"inputDataBucketName\":\"${INPUT_BUCKET}\",
      \"inputDataBlobPrefix\":\"${INPUT_PREFIX}\",
      \"outputDataBucketName\":\"${OUTPUT_BUCKET}\",
      \"outputDataBlobPrefix\":\"${OUTPUT_PREFIX}\",
      \"postbackUrl\":\"test.com\",
      \"jobParameters\":{\"application_id\":\"customer_match\",\"data_owner_list\":\"{\\\"data_owners\\\":[{\\\"data_location\\\":{\\\"input_data_blob_prefix\\\":\\\"${INPUT_PREFIX}\\\",\\\"input_data_bucket_name\\\":\\\"${INPUT_BUCKET}\\\",\\\"is_streamed\\\":true}}]}\"}
      }"
```

### GetJob

After creating a job with the above script, check it status with this script:

```bash
curl -v -X GET --oauth2-bearer "$(gcloud auth print-identity-token \
  --include-email --impersonate-service-account=${SERVICE_ACCOUNT})" \
  "${FRONTEND_SERVICE}/v1alpha/getJob?job_request_id=${JOB_REQUEST_ID}"
```
