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


container_name='local_lookup_server_build_container'

if [ "$(docker ps -qa -f name=$container_name)" ]; then
    echo "Found container - $container_name. Start removing:"

    docker rm -f $container_name

    sudo chmod 660 /var/run/docker.sock

    echo ""
    echo "Container - $container_name is stopped and removed."
else
    echo "Container - $container_name does not exist!"
fi
