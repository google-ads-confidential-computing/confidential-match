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

#ifndef CC_LOOKUP_SERVER_PARAMETER_CLIENT_SRC_PARAMETER_CLIENT_H_
#define CC_LOOKUP_SERVER_PARAMETER_CLIENT_SRC_PARAMETER_CLIENT_H_

#include <list>
#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/parameter_client/parameter_client_interface.h"

#include "cc/lookup_server/interface/parameter_client_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Client responsible for fetching parameters
 */
class ParameterClient : public ParameterClientInterface {
 public:
  /**
   * @brief Initializes a ParameterClient.
   */
  ParameterClient();

  /**
   * TODO(b/309462682): Use CPIO configuration helper when ready. This allows
   * not having to supply and construct the prefix for the parameters.
   *
   * @brief Initializes a ParameterClient.
   *
   * @param cpio_parameter_client the underlying CPIO client to use
   * @param environment_name the name of the environment which to prefix the
   * fetched parameters
   */
  explicit ParameterClient(std::shared_ptr<scp::cpio::ParameterClientInterface>
                               cpio_parameter_client,
                           absl::string_view environment_name)
      : cpio_parameter_client_(cpio_parameter_client),
        environment_name_(environment_name) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  scp::core::ExecutionResult GetString(
      absl::string_view parameter_name,
      std::string& out_string) noexcept override;
  scp::core::ExecutionResult GetInt32(absl::string_view parameter_name,
                                      int32_t& out_int) noexcept override;
  scp::core::ExecutionResult GetSizeT(absl::string_view parameter_name,
                                      size_t& out_sizet) noexcept override;
  scp::core::ExecutionResult GetBool(absl::string_view parameter_name,
                                     bool& out_bool) noexcept override;
  scp::core::ExecutionResult GetStringList(
      absl::string_view parameter_name,
      std::list<std::string>& out_list) noexcept override;
  scp::core::ExecutionResult GetInt32List(
      absl::string_view parameter_name,
      std::list<int32_t>& out_list) noexcept override;
  scp::core::ExecutionResult GetSizeTList(
      absl::string_view parameter_name,
      std::list<size_t>& out_list) noexcept override;
  scp::core::ExecutionResult GetBoolList(
      absl::string_view parameter_name,
      std::list<bool>& out_list) noexcept override;

 private:
  /**
   * @brief Fetches a parameter from CPIO or returns an error.
   *
   * @param name the name of the parameter
   * @return the value of the parameter or an error
   */
  scp::core::ExecutionResultOr<std::string> GetParameter(
      absl::string_view name) noexcept;

  // The internal CPIO client that is used to fetch from cloud.
  std::shared_ptr<scp::cpio::ParameterClientInterface> cpio_parameter_client_;
  // Prefix for parameters
  absl::string_view environment_name_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_PARAMETER_CLIENT_SRC_PARAMETER_CLIENT_H_
