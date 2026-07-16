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

#ifndef CC_MATCH_SERVICE_SERVICE_MATCH_SERVICE_H_
#define CC_MATCH_SERVICE_SERVICE_MATCH_SERVICE_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/random/random.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "grpcpp/grpcpp.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/tasks/match_task_interface.h"
#include "protos/match_service/api/v1/match_service.grpc.pb.h"

namespace google::confidential_match::match_service {

struct MatchServiceOptions {
  // A list of accounts that are authorized to use the service.
  std::vector<std::string> allowed_client_accounts;
  // Whether to allow all callers for testing.
  // When true, `allowed_client_accounts` is ignored.
  bool allow_all_callers = false;
};

class MatchService final : public api::v1::MatchService::CallbackService {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the MatchService object.
  explicit MatchService(
      scp::core::AsyncExecutorInterface* match_service_async_executor,
      MatchTaskInterface* match_task,
      scp::cpio::MetricClientInterface* metric_client,
      absl::string_view metric_namespace, MatchServiceOptions options)
      : match_service_async_executor_(*match_service_async_executor),
        metric_client_(metric_client),
        metric_namespace_(metric_namespace),
        allow_all_callers_(options.allow_all_callers),
        allowed_client_accounts_(std::move(options.allowed_client_accounts)),
        match_task_(match_task),
        bitgen_() {}

  // Handles a match request.
  grpc::ServerUnaryReactor* Match(
      grpc::CallbackServerContext* context,
      const api::v1::MatchRequest* request,
      api::v1::MatchResponse* response) noexcept override;

 private:
  template <typename TRequest, typename TResponse>
  class RequestContext;

  // Processes a match request.
  void ProcessMatchRequest(
      RequestContext<api::v1::MatchRequest, api::v1::MatchResponse>
          ctx) noexcept;

  // Handles the callback after a match request has been processed.
  void OnProcessMatchRequestCallback(
      RequestContext<api::v1::MatchRequest, api::v1::MatchResponse> ctx,
      AsyncContext<backend::MatchRequest, backend::MatchResponse>&
          result_context) noexcept;

  // Returns true if the client is authorized or all callers are allowed.
  // Otherwise, finishes the request and returns false.
  template <typename TRequest, typename TResponse>
  bool AuthorizeOrFinish(RequestContext<TRequest, TResponse>& ctx) noexcept;

  // The async executor used to process incoming requests.
  scp::core::AsyncExecutorInterface& match_service_async_executor_;
  // The client for logging metrics.
  scp::cpio::MetricClientInterface* metric_client_;
  // The metric namespace.
  absl::string_view metric_namespace_;
  // Whether or not to allow all callers. When true, allowed_client_accounts_
  // is ignored.
  const bool allow_all_callers_;
  // The list of accounts allowed to call the gRPC service.
  const std::vector<std::string> allowed_client_accounts_;
  // The task used for matching requests.
  MatchTaskInterface* match_task_;
  // Bit generator used as a source for randomness.
  absl::BitGen bitgen_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_SERVICE_MATCH_SERVICE_H_
