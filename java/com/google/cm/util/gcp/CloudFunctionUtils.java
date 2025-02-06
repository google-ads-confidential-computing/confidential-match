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

package com.google.cm.util.gcp;

import com.google.cloud.functions.HttpResponse;
import com.google.cm.util.ProtoUtils;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;

/** Utility methods for use with GCP cloud functions. */
public final class CloudFunctionUtils {

  private CloudFunctionUtils() {}

  /** Loads values from the protocol buffer into the {@link HttpResponse}. */
  public static void loadHttpResponseFromProto(
      HttpResponse httpResponse,
      Message responseProto,
      int statusCode,
      Map<String, String> headers)
      throws IOException {
    httpResponse.getWriter().write(ProtoUtils.getJsonFromProto(responseProto));
    httpResponse.setStatusCode(statusCode);
    headers.forEach(httpResponse::appendHeader);
  }
}
