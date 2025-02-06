// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cc/lookup_server/auth/src/jwt_validator.h"

#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gtest/gtest.h"
#include "public/core/test/interface/execution_result_matchers.h"

#include "cc/lookup_server/auth/src/error_codes.h"

namespace google::confidential_match::lookup_server {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;

// Keyset for the following public/private key pair:
//
// {
//     "p":
//     "7FzNr3biFNCRyk0iX6Sxc18G_T3QorP084kNdvAA59VML7w8o25AZ6FqVh9NThEcw7JjwIE78p2O2kEmLJSijJDT164cPDn5c4TZoSlfJkn5DNSErOP0JBrQosAHh9q-3HDilTj3bpJ4yenl4pLUqrSR3ziE9yNJCwH79z4q6K8",
//     "kty": "RSA",
//     "q":
//     "mtZU6tQNEhb76UBfRmPaByGJMco6En5m0PE00KGP7BZBnpGtULggbuqt6qRL5vhyRw_QHcmZqKrCSMOq0vvipKoEONPJacSZkFtMegvUmwaslQIqHjZNceTh6CsTchckjFSajI44gpy35aDUyIOm8aF8uGkr79-tbugrFdhakDE",
//     "d":
//     "ignnP5gy0ALn-_nxwTWar5Maj4nm4OuKa9Jh67u_Owddt1afuLj5h3rKChcJ9dZMPxxlu6P0YuPx93AH_5nQETWF9WpQAjBRIQ4YXNsELS0mrm7vrFuoQs_YHgu106tYWhnkTzJaZWK8Z_DM9nQ6DK8TNOfzf8-ZdxCRojmSwBC9bx77i1-GLr1lNY8ehhS4wNJqkJ6h6h8cFb-75o3a3JhG3X7zM1qHxqAy5GC5yK8eiN5cdDqsOxIm7sPLhoFUIkVppRn-SwBGlogLwInoDqa45Oa1u8mTZHS5kKqGKnsrNR5kr7CAM4KWSOHz-ZFFqmKI1FEfUo1g46qLiVr-oQ",
//     "e": "AQAB",
//     "use": "sig",
//     "kid": "test",
//     "qi":
//     "cBi7mNwaDPajFNSknw3jmgOZututVtIJSFSSSeNpC94SxLnUhJZLz8sGmDwDyCmOHylEBWxwVjyJjuuAMp6irMU4VrtDJXA9kEmOepBWbvKEcz3KNyJzv9ZOvtMyll38dPDzTwuiyLBzONsXUdOfiSOfClx4htUWypTiXkvh3T8",
//     "dp":
//     "wAjOXCbcnEiwioqMyORIABJO3WVhoy9ObqcFN4LbWYAkvAvVwHqM_SVZ_crExg6FLkI7ZWYaTI3SSGTyHPpN7qKkWvFso0n-7-oZ3yR71-H15IStnsI90y-uHuyhsbD5rKRSkyaLcVyzomjUi3b8Lg0zDwcekKQsbjMYgdISjqU",
//     "alg": "RS256",
//     "dq":
//     "CG1n-8wE1ho4Jc7iIKOop0C1Bee612zXzcGDHWPdwHzJn4bQRxdudHsDckT0-KJHHgUHT8e8PEjnACzeLFOXV10Fk847JS2VKh0-AVqJfNKVLBsNDc3o0y0g-pD1ov0NeTHVo5bODpXXEIF7c1pvCLHAZw0aXhjviJzMU4kODNE",
//     "n":
//     "jvW3tsJL4qc266e4GdLT_o0GXbf-ImNUStw3i2JzK0CWrfrbxTbgpUQUMsyL0HFYf_fdLUutpWx7bqWkg8aRyTDL_-NId9f6y4X6lTvr2r0bof2GTU5JNVThnTvV8OYnWhS7BHLZP2J2GPL5J_hEY0_s6-1YEo6PBHIynq78WoZcyFx94QEgzhb6umqR0ric6CntsYr4jGLOsiA1FxWyvLVMpZYJqjK3aPnhwUdkh0yKL6XSYomFiozx2JcoXCvtk4Ki7B74T6ndwCCtrf90Ngbtaw5bheUJ-EU9IEE8zdQbrbImmskCYa9CRKadTIsNmF3jVFVyC1HJrtgPfp75fw"
// }
constexpr absl::string_view kJwkSet = R"({
"keys": [
  {
  "kty": "RSA",
  "e": "AQAB",
  "use": "sig",
  "kid": "test",
  "alg": "RS256",
  "n": "jvW3tsJL4qc266e4GdLT_o0GXbf-ImNUStw3i2JzK0CWrfrbxTbgpUQUMsyL0HFYf_fdLUutpWx7bqWkg8aRyTDL_-NId9f6y4X6lTvr2r0bof2GTU5JNVThnTvV8OYnWhS7BHLZP2J2GPL5J_hEY0_s6-1YEo6PBHIynq78WoZcyFx94QEgzhb6umqR0ric6CntsYr4jGLOsiA1FxWyvLVMpZYJqjK3aPnhwUdkh0yKL6XSYomFiozx2JcoXCvtk4Ki7B74T6ndwCCtrf90Ngbtaw5bheUJ-EU9IEE8zdQbrbImmskCYa9CRKadTIsNmF3jVFVyC1HJrtgPfp75fw"
  }
]})";

// JWT containing an email of "email1@google.com", subject of "1", issuer of
// "https://accounts.google.com", and audience of "lookup-server"
constexpr absl::string_view kValidJwt1 =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.LHsIfIXiJevg17pNLA5bKCL5s4l27kbXcGQMhBl9d10kBX8-u3mZbUZpZSiVgTJfavZL5z8lchEBszW_vYNOh3OaPwgiE3aY9w4ZigZU4OfpwScTnD3JZi6uvpIwQqbWkJSONkVhcLZfX6bEU50tYyGacFh_oM-r_-jGK1X-7uCqaf-b_hLNOYGItJ194o4FwK-LAlep0D9-lY1dcs72Evy28W9yW65JWxIasiQr7M1ThOzLJieMcM09zvQG7cLihFMXZW3eri2C2iX_BRX14wUpc0mkhaMW_UNYBaIJKInH3tx3nMWJD-_tNGSSvXw6cO64ui_Wq0aii29gT22FvQ)";

// JWT containing an email of "email2@google.com", subject of "2", issuer of
// "https://accounts.google.com", and audience of "lookup-server"
constexpr absl::string_view kValidJwt2 =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMiIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIyIiwiZW1haWwiOiJlbWFpbDJAZ29vZ2xlLmNvbSJ9.ASHGxw-Ye2wCDTx1I92VG-Ngq1g4ig21HNGts_vUeMzxtCw73kRahzoWQRz36LGFFMcdvvnofimBcpw5D7a_Djadme1NyDc7Q7m03-dDCuZ4qIoaPqDmpXk1OWKu-RPRvBJaSrIjuQLPJgWvCGfbHKbwKnryVgFioU44RPawxdKzYjKoaQHI9XyK2wrfn5jtFPNS1nMUH0PaXIPbAAZB71kD_UGirdQBEojvheGNgMkgMe0s0H07usxgt__RfC193fpHa5ag8LgLcrazy0IGMLDR6ZYfyJQbEeEJ-8Lt7FczzMD8gL5qkEWk9fXfPTs29lQ6Pm_kfatIsYgMLh1nSg)";

// JWT with missing email, but with subject "1", issuer of
// "https://accounts.google.com", and audience of "lookup-server"
constexpr absl::string_view kValidJwtSubjectOnly =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIn0.fz_JipnKP_eRDwijfq_3ymeebdLUsfkSnqa23LMvwgejACz-GWPCVhN6dz8IVGVYnPTqBXQVfboTNKnmGcy6bFNJ54i7TtF0Zj_5esHXV3er4Y99sZxGa8eQxE7JsbIMA8e76mIphfAZpYbl6sXKU3EBifGiBc0PCU6nbsEpNgUYn5KadRQdVqQ0T8tJXAkVcK-pztvPj0C7bYz3vfnJi_9QZ9Y9ugMgJ2rXa52pcX_F7WV6Dr87PQg_zHeqa9jaAFOSBbnPWmBSorOMCdys2NJdpllXo8XCvSXfERNJgZDRr7-ZAxuA8rsoShlTBR2saK_DV6tDv1dyeF6694kvGQ)";

// JWT with an invalid signature
constexpr absl::string_view kInvalidSignatureJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.invalid)";

// JWT with the algorithm set to "none"
constexpr absl::string_view kNoneAlgorithmJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.)";

// Expired JWT
constexpr absl::string_view kExpiredJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjoxLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.jW5ASYS5TMMmhOTY-KUU_GaI26yENUH9RlmdTSY4xLCyCYrRpRvznjNymJxlDMj7Ym0Q5ok8ZDEEdNgiUwX1qNrnWJ2dxzt_emN4aP8fFmo075oCxZokguPUPOkeKkGAfET78I0FygrFnrfuiS-WZQY87-N9mH3SXoqKu7DPqXAchdUIZ01gRH4JvlIiJSvMt8SGJJ320UPCei9JW5mxPNsaTttLLQexL8ul2d8yI8L5aaV-xl1ZUoCPgC751CCvRFSzHisQdiRdEJwtEgIgKmY1TvtHQDwtHhEdMiGxMMvCsJMZf88V4YvvAImEVt8TD37drpEYp12i6HcRjI4Hew)";

// JWT with missing expiry
constexpr absl::string_view kMissingExpiryJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMSIsImVtYWlsIjoiZW1haWwxQGdvb2dsZS5jb20ifQ.EYlbKkSgxTuBImggqpKCyvZxKBc3feH2GtNynsJ3XujDne6c1PYm5g_V2VQwvL8cXQjQoWhudKmWOVGTkzkadV5fQQy9SGLMWVbY6IUEtnt5Pp1yxOp4MWlci4qLSDMWBQCyTftI9RA6xmibY48WYPEeVDzzIQZRUcvlMYLuZ9J_R0tIg7bJs_ilsCTNbCUNV3hbvJuE0lr_4MxVex8omHncFq5vJmtedETcoYtjs5anwHRXvfh8KLQYffjANoKVnXe9WakB6vyzOj4vEs8FeDkarNphNJEzFlRmQB8UTzoWhUTJcbj2M-HYydTpqRHPN66TSV8CyB7IMcuaYOntww)";

// JWT with an issuer of "bad"
constexpr absl::string_view kBadIssuerJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6MTYwMDAwMDAwMCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJiYWQiLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.YQs0qP44HdzIFs8Y14tVultY5dDfnR7lkgnSmqxcKGSCMWSVNhssdPXuIbWp7jt4lkRdFKxr7Y07m_1fQcAltY-kdmkhc2Yyl4w18-RgJVYE-IkJJhYYMYbJQNawEUtHnOZ440_Ol8LzkTIBzQrmIxZIRBR5p27d44Fo5QdMuYdECIMXqdS10lpcKQiLrkGJ_6mThS6pNTzw5BLceiKqk9f7y1_tfqP8E1alWgdS6VpTqQXK2PvQ5RIVBCWU5S89SSEEEBOx-u1vEo6jQ7d9FoScDiVC6_46PwRNk8B9qxBYx5EqpAtOi3NVyHFzpnE4JdEXlENsLMe4234psWXj1w)";

// JWT with an audience of "bad"
constexpr absl::string_view kBadAudienceJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJiYWQiLCJhenAiOiIxIiwiaWF0IjoxNjAwMDAwMDAwLCJleHAiOjk5OTk5OTk5OTksImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjEiLCJlbWFpbCI6ImVtYWlsMUBnb29nbGUuY29tIn0.dJsTt5zVEA43PFkM2w1VysE-01Re-aRenNblnQkoDgsC0KvPKXTHGk2d2SXg62GQikFvD7W4q1lrj3LJ-LrUAgGxFwAzvjwXW3WnMBvhFQetxd-Bu0LvWnS4mGeHNfdqnjlrw5mIZ6XEaNtP5X5LUSLfJPqRtdeMFoYTAuiv5FErUMDBaVv6_5mjNm_xA62nVjQ21PaV_HgtTyk4LjCEt7OzUBgBApzC30V2c23zqzpgO8pfbEuhqsXiflXwxyAazsEKvsGOk6VT_WxGovjr8vjk7Vshm7fp69eThv-N3MH7sbE8J6fRXwnkKy9JK7sBJ59eSeIh4B4moGrgRz8y7g)";

// JWT with a missing audience
constexpr absl::string_view kMissingAudienceJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhenAiOiIxIiwiaWF0IjoxNjAwMDAwMDAwLCJleHAiOjk5OTk5OTk5OTksImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjEiLCJlbWFpbCI6ImVtYWlsMUBnb29nbGUuY29tIn0.IseS-rdDBnp_BSpM4YW9lW10AYa67o1MJ7NcyIYIccxNkHZdP1ZzZbhpSpXPlUel-luniu4CJtks3buiaY5keijBbzXu8wMgcLi2sLo-rGaO7KU7ioMcBP7kuKv7ZH5kS6DamzBGV34FGuOj1NrzXYlP8A--o7qPcedXbXmOLT690AYwsvUxG8wL1pUSAk57dAaT-pul3dL3-0GmxXbTurNSH4eF6TFcyy-9LUTvxStJAcER4UZ0cqhEpl70i3kqCpzVUHwORVg5PQmHaIg2FehjBEGzpEJ0eM_293XzyfFbbN-qsidKBUiiyTOmBtr8YOw-LXuJBVv2G8pgBHyeWg)";

// JWT with an issuance time in the future
constexpr absl::string_view kIssuedInFutureJwt =
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJsb29rdXAtc2VydmVyIiwiYXpwIjoiMSIsImlhdCI6OTk5OTk5OTk5OCwiZXhwIjo5OTk5OTk5OTk5LCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxIiwiZW1haWwiOiJlbWFpbDFAZ29vZ2xlLmNvbSJ9.KcmDe6gWUAD-9vLJ7DlnlN82Gg_3QXqFip3H738IYVNRLCkhZfll23cyTWof9dQocFZto-oxxq0W4XB9NwShVlUt4Iyjs5oYNBqCReVgKxR5qkD4nUNy7afut_4FsrJ3rvRb0JNPpN10-F0-rS9T6-XGD8lcMz5zAkXFjFhwrJ6A08-Hp6L1vxXzXEliiZ6KUgwAVAiALsv-pSWU1PH5r1uClJq_WM5so50TyZr9ub7l3NgazlWxBg9p9he9cHP5zZP8KlQ5SfGohaD9y1Cnok7vQY0CNCYvaemkynaXU88s_EhLYGGRu-KZehq_6537rx18-T1a8p3SG-UOword-Q)";

constexpr absl::string_view kEmail1 = "email1@google.com";
constexpr absl::string_view kEmail2 = "email2@google.com";
constexpr absl::string_view kEmail3 = "email3@google.com";
constexpr absl::string_view kSubject1 = "1";
constexpr absl::string_view kSubject2 = "2";
constexpr absl::string_view kSubject3 = "3";
constexpr absl::string_view kIssuer = "https://accounts.google.com";
constexpr absl::string_view kAudience = "lookup-server";

TEST(JwtValidatorTest, ValidateJwtReturnsSuccess) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kValidJwt1);

  EXPECT_SUCCESS(result);
}

TEST(JwtValidatorTest, ValidateJwtWithMultipleEmailsReturnsSuccess) {
  std::vector<std::string> allowed_emails = {
      std::string(kEmail1), std::string(kEmail2), std::string(kEmail3)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kValidJwt2);

  EXPECT_SUCCESS(result);
}

TEST(JwtValidatorTest, ValidateJwtWithMultipleSubjectsReturnsSuccess) {
  std::vector<std::string> allowed_emails = {};
  std::vector<std::string> allowed_subjects = {std::string(kSubject1),
                                               std::string(kSubject2)};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kValidJwt1);

  EXPECT_SUCCESS(result);
}

TEST(JwtValidatorTest, ValidateJwtContainingSubjectOnlyReturnsSuccess) {
  std::vector<std::string> allowed_emails = {};
  std::vector<std::string> allowed_subjects = {std::string(kSubject1),
                                               std::string(kSubject2)};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kValidJwtSubjectOnly);

  EXPECT_SUCCESS(result);
}

TEST(JwtValidatorTest, ValidateJwtWithUnmatchedEmailReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1),
                                             std::string(kEmail3)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kValidJwt2);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED)));
}

TEST(JwtValidatorTest, ValidateJwtWithUnmatchedEmailAndSubjectReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail2),
                                             std::string(kEmail3)};
  std::vector<std::string> allowed_subjects = {std::string(kSubject2)};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kValidJwt1);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED)));
}

TEST(JwtValidatorTest,
     ValidateJwtContainingSubjectOnlyUnmatchedReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail2),
                                             std::string(kEmail3)};
  std::vector<std::string> allowed_subjects = {std::string(kSubject2),
                                               std::string(kSubject3)};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kValidJwtSubjectOnly);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED)));
}

TEST(JwtValidatorTest, ValidateEmptyJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, "");

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateInvalidSignatureJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kInvalidSignatureJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateNoneAlgorithmJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kNoneAlgorithmJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateExpiredJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result = (*auth_validator_or)->Validate(kJwkSet, kExpiredJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateMissingExpiryJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kMissingExpiryJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateMissingEmailJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kValidJwtSubjectOnly);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED)));
}

TEST(JwtValidatorTest, ValidateBadIssuerJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kBadIssuerJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateBadAudienceJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kBadAudienceJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateMissingAudienceJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kMissingAudienceJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

TEST(JwtValidatorTest, ValidateIssuedInFutureJwtReturnsFailure) {
  std::vector<std::string> allowed_emails = {std::string(kEmail1)};
  std::vector<std::string> allowed_subjects = {};
  ExecutionResultOr<std::unique_ptr<JwtValidator>> auth_validator_or =
      JwtValidator::Create(kIssuer, kAudience, allowed_emails,
                           allowed_subjects);
  EXPECT_SUCCESS(auth_validator_or.result());

  ExecutionResult result =
      (*auth_validator_or)->Validate(kJwkSet, kIssuedInFutureJwt);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

}  // namespace google::confidential_match::lookup_server
