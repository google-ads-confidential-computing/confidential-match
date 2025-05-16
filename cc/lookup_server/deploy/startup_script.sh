#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create self-signed cert
mkdir -p /opt/google/ssl/self-signed-certs;
openssl ecparam -name prime256v1 -genkey -out /opt/google/ssl/self-signed-certs/privatekey.pem;
openssl req -new -key /opt/google/ssl/self-signed-certs/privatekey.pem -out /opt/google/ssl/self-signed-certs/csr.pem -subj "/C=US/ST=WA/O=CFM/CN=cfm.admc.goog";
openssl x509 -req -days 7305 -in /opt/google/ssl/self-signed-certs/csr.pem -signkey /opt/google/ssl/self-signed-certs/privatekey.pem -out /opt/google/ssl/self-signed-certs/public.crt;
rm /opt/google/ssl/self-signed-certs/csr.pem;

# launch lookup server
exec /opt/google/lookup_service/lookup_server
