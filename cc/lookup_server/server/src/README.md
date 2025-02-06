# Lookup Server

## Build

```bash
bazel build //cc/lookup_server/server/src:lookup_server
```

## Run

This runs Lookup Server locally on a Cloudtop instance, using staging GCP
instances for data.

```bash
enabled_log_levels=Info,Warning,Critical,Error \
async_executor_queue_size=10000 \
async_executor_threads_count=16 \
io_async_executor_queue_size=1000 \
io_async_executor_threads_count=50 \
cluster_group_id=local \
cluster_id=lookup-server-0 \
environment=local \
bazel run //cc/lookup_server/server/src:lookup_server \
--//cc/lookup_server/server/src:platform=gcp_with_test_cpio \
--@com_google_adm_cloud_scp//cc/public/cpio/interface:platform=gcp \
--@com_google_adm_cloud_scp//cc/public/cpio/interface:run_inside_tee=False
```

## Send test requests

### Healthcheck

Sends a request to the healthcheck server.

```bash
curl -v --http2-prior-knowledge "http://localhost:8081/v1/healthcheck"
```

### Lookup request

Sends a match request to a local Lookup Server instance. The MRP staging service
account is impersonated and used for
auth credentials.

```bash
curl -s --http2-prior-knowledge \
  -H "x-gscp-claimed-identity: *@google.com" \
  -H "x-auth-token: $(gcloud auth print-identity-token --include-email --audiences=lookup-server-staging --impersonate-service-account=lookup-caller@my-project.iam.gserviceaccount.com 2>/dev/null)" \
  -X POST \
  -d '{"dataRecords": [{"lookupKey": {"key": "IdSR6AjQ6V89eGWazPu2ERC3UFYHpWM/pK0G18XqXTM="}}], "hashInfo": {"hashType": "HASH_TYPE_SHA_256"}, "keyFormat": "KEY_FORMAT_HASHED", "shardingScheme": {"numShards": "60", "type": "jch"}, "associatedDataKeys": ["encrypted_gaia_id", "pii_type"]}' \
  "http://localhost:8080/v1/lookup"
```
