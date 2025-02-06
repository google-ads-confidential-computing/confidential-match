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

#ifndef CC_LOOKUP_SERVER_INTERFACE_PARAMETER_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_PARAMETER_CLIENT_INTERFACE_H_

#include <list>
#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/core/utils/src/string_util.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface for a client used to fetch Parameters. */
class ParameterClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~ParameterClientInterface() = default;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a string
   *
   * @param parameter_name the name of the parameter to be fetched
   * @param out_string where to store the output string value
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetString(
      absl::string_view parameter_name, std::string& out_string) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a int32_t
   *
   * @param parameter_name the name of the parameter to be fetched
   *    @param out_int where to store the output int32_t value
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetInt32(absl::string_view parameter_name,
                                              int32_t& out_int) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a size_t
   *
   * @param parameter_name the name of the parameter to be fetched
   *    @param out_sizet where to store the output size_t value
   * @return whether the operation is successful   */
  virtual scp::core::ExecutionResult GetSizeT(absl::string_view parameter_name,
                                              size_t& out_sizet) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a bool
   *
   * @param parameter_name the name of the parameter to be fetched
   *    @param out_bool where to store the output bool value
   * @return whether the operation is successful
   * */
  virtual scp::core::ExecutionResult GetBool(absl::string_view parameter_name,
                                             bool& out_bool) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a list of strings
   *
   * @param parameter_name the name of the parameter to be fetched
   *  @param out_list where to store the output list of strings
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetStringList(
      absl::string_view parameter_name,
      std::list<std::string>& out_list) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a list of int32_ts
   *
   * @param parameter_name the name of the parameter to be fetched
   *      @param out_list where to store the output list of int32_ts
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetInt32List(
      absl::string_view parameter_name,
      std::list<int32_t>& out_list) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a list of size_ts
   *
   * @param parameter_name the name of the parameter to be fetched
   *      @param out_list where to store the output list of size_ts
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetSizeTList(
      absl::string_view parameter_name,
      std::list<size_t>& out_list) noexcept = 0;

  /**
   * @brief Fetches a parameter given a name synchronously and assigns its value
   * to a list of bools
   *
   * @param parameter_name the name of the parameter to be fetched
   *      @param out_list where to store the output list of bools
   * @return whether the operation is successful
   */
  virtual scp::core::ExecutionResult GetBoolList(
      absl::string_view parameter_name, std::list<bool>& out_list) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_PARAMETER_CLIENT_INTERFACE_H_
