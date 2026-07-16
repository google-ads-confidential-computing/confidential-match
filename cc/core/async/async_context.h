/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CC_CORE_ASYNC_ASYNC_CONTEXT_H_
#define CC_CORE_ASYNC_ASYNC_CONTEXT_H_

#include <functional>
#include <memory>
#include <typeinfo>
#include <utility>

#include "absl/status/status.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"

#include "cc/core/async/error_codes.h"
#include "cc/core/logger/log.h"

namespace google::confidential_match {

template <typename TRequest, typename TResponse>
struct ExecutionResultAsyncContext;

// AsyncContext controls the lifecycle of any async operations.
//
// The caller sets the request and the callback on the object, and the
// callback operates on an AsyncContext containing a response or error.
template <typename TRequest, typename TResponse>
struct AsyncContext {
 private:
  // Type of the callback function for the async operation.
  using Callback =
      typename std::function<void(AsyncContext<TRequest, TResponse>&)>;

 public:
  // Constructs a new AsyncContext object.
  //
  // logger: The logger object used for logging.
  explicit AsyncContext(std::shared_ptr<LoggerInterface> logger)
      : AsyncContext(
            /* request= */
            nullptr, [](AsyncContext<TRequest, TResponse>&) {}, logger) {}

  // Constructs a new AsyncContext object.
  //
  // request: The request object for the async operation.
  // callback: The callback handler invoked after the async operation
  // completes.
  // logger: The logger object used for logging.
  explicit AsyncContext(std::shared_ptr<TRequest> request,
                        const Callback& callback,
                        std::shared_ptr<LoggerInterface> logger)
      : request(request),
        response(nullptr),
        status(absl::OkStatus()),
        callback(callback),
        logger(logger) {}

  AsyncContext(const AsyncContext& other) = default;
  AsyncContext& operator=(const AsyncContext& other) = default;
  AsyncContext(AsyncContext&& other) = default;
  AsyncContext& operator=(AsyncContext&& other) = default;

  // Finishes the async operation by calling the callback.
  void Finish() noexcept {
    if (status.ok() && response == nullptr) {
      LOG_ERROR(*logger, "Attempted to finish context with null response set.");
      status = absl::InternalError("Async operation did not set a response.");
    }
    if (!status.ok()) {
      // Clients should never read the response with non-OK status, but just
      // in case, null out the response for safety
      response = nullptr;
    }

    if (callback) {
      if (!status.ok()) {
        // typeid(TRequest).name() is an approximation of the context's template
        // types mangled in compiler defined format, mainly for debugging
        // purposes.
        LOG_ERROR(*logger,
                  "AsyncContext finished with error: '%s', RequestType: '%s', "
                  "ResponseType: '%s'",
                  status.ToString(), typeid(TRequest).name(),
                  typeid(TResponse).name());
      }
      callback(*this);
    }
  }

  // Creates a new child context branched from this AsyncContext.
  template <typename TChildRequest, typename TChildResponse>
  AsyncContext<TChildRequest, TChildResponse> CreateAsyncContext() noexcept {
    return AsyncContext<TChildRequest, TChildResponse>(logger);
  }

  // Creates a new child context branched from this AsyncContext.
  //
  // child_request: The request object for the async operation.
  // child_callback: The callback handler invoked after the async operation
  // completes.
  template <typename TChildRequest, typename TChildResponse>
  AsyncContext<TChildRequest, TChildResponse> CreateAsyncContext(
      std::shared_ptr<TChildRequest> child_request,
      const std::function<void(AsyncContext<TChildRequest, TChildResponse>&)>&
          child_callback) noexcept {
    return AsyncContext<TChildRequest, TChildResponse>(child_request,
                                                       child_callback, logger);
  }

  // Creates a new ExecutionResult-based child context from this AsyncContext.
  template <typename TChildRequest, typename TChildResponse>
  ExecutionResultAsyncContext<TChildRequest, TChildResponse>
  CreateExecutionResultAsyncContext() noexcept {
    return ExecutionResultAsyncContext<TChildRequest, TChildResponse>(logger);
  }

  // Creates a new ExecutionResult-based child context from this AsyncContext.
  //
  // child_request: The request object for the async operation.
  // child_callback: The callback handler invoked after the async operation
  // completes.
  template <typename TChildRequest, typename TChildResponse>
  ExecutionResultAsyncContext<TChildRequest, TChildResponse>
  CreateExecutionResultAsyncContext(
      std::shared_ptr<TChildRequest> child_request,
      const std::function<
          void(ExecutionResultAsyncContext<TChildRequest, TChildResponse>&)>&
          child_callback) noexcept {
    return ExecutionResultAsyncContext<TChildRequest, TChildResponse>(
        child_request, child_callback, logger);
  }

  // The input request for the async operation.
  std::shared_ptr<TRequest> request;
  // The output response from the async operation.
  std::shared_ptr<TResponse> response;
  // The status of the async operation. If the operation fails, this will
  // contain the error.
  absl::Status status;
  // Callback function invoked after the async operation is completed.
  Callback callback;
  // The logger object.
  std::shared_ptr<LoggerInterface> logger;
};

// ExecutionResultAsyncContext controls the lifecycle of any async operations.
//
// This is used for interfacing with SCP libraries which return error statuses
// as `ExecutionResult`. Otherwise, status-based AsyncContext is preferred.
//
// The caller sets the request and the callback on the object, and the
// callback operates on an AsyncContext containing a response or error.
template <typename TRequest, typename TResponse>
struct ExecutionResultAsyncContext {
 private:
  // Type of the callback function for the async operation.
  using Callback = typename std::function<void(
      ExecutionResultAsyncContext<TRequest, TResponse>&)>;

 public:
  // Constructs a new ExecutionResultAsyncContext object.
  //
  // logger: The logger object used for logging.
  explicit ExecutionResultAsyncContext(std::shared_ptr<LoggerInterface> logger)
      : ExecutionResultAsyncContext(
            /* request= */
            nullptr, [](ExecutionResultAsyncContext<TRequest, TResponse>&) {},
            logger) {}

  // Constructs a new ExecutionResultAsyncContext object.
  //
  // request: The request object for the async operation.
  // callback: The callback handler invoked after the async operation
  // completes.
  // logger: The logger object used for logging.
  explicit ExecutionResultAsyncContext(std::shared_ptr<TRequest> request,
                                       const Callback& callback,
                                       std::shared_ptr<LoggerInterface> logger)
      : request(request),
        result(scp::core::SuccessExecutionResult()),
        response(nullptr),
        callback(callback),
        logger(logger) {}

  ExecutionResultAsyncContext(const ExecutionResultAsyncContext& other) =
      default;
  ExecutionResultAsyncContext& operator=(
      const ExecutionResultAsyncContext& other) = default;
  ExecutionResultAsyncContext(ExecutionResultAsyncContext&& other) = default;
  ExecutionResultAsyncContext& operator=(ExecutionResultAsyncContext&& other) =
      default;

  // Finishes the async operation by calling the callback.
  void Finish() noexcept {
    if (result.Successful() && response == nullptr) {
      LOG_ERROR(*logger,
                "Attempted to finish ExecutionResult-based context with null "
                "response set.");
      result = scp::core::FailureExecutionResult(ASYNC_INTERNAL_ERROR);
    }
    if (!result.Successful()) {
      // Clients should never read the response with an unsuccessful result,
      // but just in case, null out the response for safety
      response = nullptr;
    }

    if (callback) {
      if (!result.Successful()) {
        // typeid(TRequest).name() is an approximation of the context's template
        // types mangled in compiler defined format, mainly for debugging
        // purposes.
        LOG_ERROR(*logger,
                  "ExecutionResultAsyncContext finished with error: '%d', "
                  "RequestType: '%s', "
                  "ResponseType: '%s'",
                  result.status_code, typeid(TRequest).name(),
                  typeid(TResponse).name());
      }
      callback(*this);
    }
  }

  // Returns an SCP AsyncContext used to interface with SCP libraries.
  scp::core::AsyncContext<TRequest, TResponse>
  CreateScpAsyncContext() noexcept {
    scp::core::AsyncContext<TRequest, TResponse> scp_context;
    scp_context.request = this->request;
    scp_context.callback =
        [*this](scp::core::AsyncContext<TRequest, TResponse>& ctx) mutable {
          result = ctx.result;
          response = std::move(ctx.response);
          Finish();
        };
    return scp_context;
  }

  // Creates a new child context branched from this AsyncContext.
  template <typename TChildRequest, typename TChildResponse>
  AsyncContext<TChildRequest, TChildResponse> CreateAsyncContext() noexcept {
    return AsyncContext<TChildRequest, TChildResponse>(logger);
  }

  // Creates a new child context branched from this AsyncContext.
  //
  // child_request: The request object for the async operation.
  // child_callback: The callback handler invoked after the async operation
  // completes.
  template <typename TChildRequest, typename TChildResponse>
  AsyncContext<TChildRequest, TChildResponse> CreateAsyncContext(
      std::shared_ptr<TChildRequest> child_request,
      const std::function<void(AsyncContext<TChildRequest, TChildResponse>&)>&
          child_callback) noexcept {
    return AsyncContext<TChildRequest, TChildResponse>(child_request,
                                                       child_callback, logger);
  }

  // Creates a new ExecutionResult-based child context from this AsyncContext.
  template <typename TChildRequest, typename TChildResponse>
  ExecutionResultAsyncContext<TChildRequest, TChildResponse>
  CreateExecutionResultAsyncContext() noexcept {
    return ExecutionResultAsyncContext<TChildRequest, TChildResponse>(logger);
  }

  // Creates a new ExecutionResult-based child context from this AsyncContext.
  //
  // child_request: The request object for the async operation.
  // child_callback: The callback handler invoked after the async operation
  // completes.
  template <typename TChildRequest, typename TChildResponse>
  ExecutionResultAsyncContext<TChildRequest, TChildResponse>
  CreateExecutionResultAsyncContext(
      std::shared_ptr<TChildRequest> child_request,
      const std::function<
          void(ExecutionResultAsyncContext<TChildRequest, TChildResponse>&)>&
          child_callback) noexcept {
    return ExecutionResultAsyncContext<TChildRequest, TChildResponse>(
        child_request, child_callback, logger);
  }

  // The input request for the async operation.
  std::shared_ptr<TRequest> request;
  // The execution result of the async operation. If the operation fails, this
  // will contain the error.
  scp::core::ExecutionResult result;
  // The output response from the async operation.
  std::shared_ptr<TResponse> response;
  // Callback function invoked after the async operation is completed.
  Callback callback;
  // The logger object.
  std::shared_ptr<LoggerInterface> logger;
};

// Finishes a context on a thread on the provided AsyncExecutor thread pool.
//
// If the context cannot be finished async, it will be finished
// synchronously on the current thread.
//
// context: The async context to be completed
// async_executor: The executor (thread pool) for the async context to
// be completed on
// priority: The priority for the executor. Defaults to High
template <typename TRequest, typename TResponse>
void FinishContext(
    AsyncContext<TRequest, TResponse>& context,
    const std::shared_ptr<scp::core::AsyncExecutorInterface>& async_executor,
    scp::core::AsyncPriority priority =
        scp::core::AsyncPriority::High) noexcept {
  // Make a copy of context - this way we know async_executor's handle will
  // never go out of scope.
  if (!async_executor
           ->Schedule([context]() mutable { context.Finish(); }, priority)
           .Successful()) {
    context.Finish();
  }
}

// Finishes a context on a thread on the provided AsyncExecutor thread pool.
//
// If the context cannot be finished async, it will be finished
// synchronously on the current thread.
//
// context: The async context to be completed
// status: The error status to finish the context with
// async_executor: The executor (thread pool) for the async context to
// be completed on
// priority: The priority for the executor. Defaults to High
template <typename TRequest, typename TResponse>
void FinishContext(
    AsyncContext<TRequest, TResponse>& context, const absl::Status& status,
    const std::shared_ptr<scp::core::AsyncExecutorInterface>& async_executor,
    scp::core::AsyncPriority priority =
        scp::core::AsyncPriority::High) noexcept {
  context.status = status;
  // Make a copy of context - this way we know async_executor's handle will
  // never go out of scope.
  if (!async_executor
           ->Schedule([context]() mutable { context.Finish(); }, priority)
           .Successful()) {
    context.Finish();
  }
}

// Finishes a context on the current thread.
//
// context: The async context to be completed
template <typename TRequest, typename TResponse>
void FinishContext(AsyncContext<TRequest, TResponse>& context) noexcept {
  context.Finish();
}

// Finishes a context on the current thread.
//
// context: The async context to be completed
// status: The error status to finish the context with
template <typename TRequest, typename TResponse>
void FinishContext(AsyncContext<TRequest, TResponse>& context,
                   const absl::Status& status) noexcept {
  context.status = status;
  context.Finish();
}

// Finishes a context on a thread on the provided AsyncExecutor thread pool.
//
// If the context cannot be finished async, it will be finished
// synchronously on the current thread.
//
// context: The async context to be completed
// async_executor: The executor (thread pool) for the async context to
// be completed on
// priority: The priority for the executor. Defaults to High
template <typename TRequest, typename TResponse>
void FinishContext(
    ExecutionResultAsyncContext<TRequest, TResponse>& context,
    const std::shared_ptr<scp::core::AsyncExecutorInterface>& async_executor,
    scp::core::AsyncPriority priority =
        scp::core::AsyncPriority::High) noexcept {
  // Make a copy of context - this way we know async_executor's handle will
  // never go out of scope.
  if (!async_executor
           ->Schedule([context]() mutable { context.Finish(); }, priority)
           .Successful()) {
    context.Finish();
  }
}

// Finishes a context on a thread on the provided AsyncExecutor thread pool.
//
// If the context cannot be finished async, it will be finished
// synchronously on the current thread.
//
// context: The async context to be completed
// result: The error result to finish the context with
// async_executor: The executor (thread pool) for the async context to
// be completed on
// priority: The priority for the executor. Defaults to High
template <typename TRequest, typename TResponse>
void FinishContext(
    ExecutionResultAsyncContext<TRequest, TResponse>& context,
    const scp::core::ExecutionResult& result,
    const std::shared_ptr<scp::core::AsyncExecutorInterface>& async_executor,
    scp::core::AsyncPriority priority =
        scp::core::AsyncPriority::High) noexcept {
  context.result = result;
  // Make a copy of context - this way we know async_executor's handle will
  // never go out of scope.
  if (!async_executor
           ->Schedule([context]() mutable { context.Finish(); }, priority)
           .Successful()) {
    context.Finish();
  }
}

// Finishes a context on the current thread.
//
// context: The async context to be completed
template <typename TRequest, typename TResponse>
void FinishContext(
    ExecutionResultAsyncContext<TRequest, TResponse>& context) noexcept {
  context.Finish();
}

// Finishes a context on the current thread.
//
// context: The async context to be completed
// result: The error result to finish the context with
template <typename TRequest, typename TResponse>
void FinishContext(ExecutionResultAsyncContext<TRequest, TResponse>& context,
                   const scp::core::ExecutionResult& result) noexcept {
  context.result = result;
  context.Finish();
}

}  // namespace google::confidential_match

#endif  // CC_CORE_ASYNC_ASYNC_CONTEXT_H_
