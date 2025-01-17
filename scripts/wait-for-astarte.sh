#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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
#
# SPDX-License-Identifier: Apache-2.0

# Wait for astarte to return 200 on the health endpoints
#
# Needs the $ASTARTE_BASE_DOMAIN env variable to be set

set -exEuo pipefail

ASTARTE_PROTOCOL=${ASTARTE_PROTOCOL:-http}
ASTARTE_BASE_DOMAIN=${ASTARTE_BASE_DOMAIN:-astarte.localhost}

function quit_wait() {
    echo "Exiting"
}

curl_get_status_ok() {
    status=$(curl -L -s -o /dev/null -X GET -w "%{http_code}" "$1")

    if [[ $status =~ ^2.* ]]; then
        return 0
    fi

    return 1
}

trap quit_wait INT
trap quit_wait TERM

printf "Astarte is initializing, waiting for cluster setup"
while true; do
    if curl_get_status_ok "$ASTARTE_PROTOCOL://api.$ASTARTE_BASE_DOMAIN/appengine/health" &&
        curl_get_status_ok "$ASTARTE_PROTOCOL://api.$ASTARTE_BASE_DOMAIN/pairing/health" &&
        curl_get_status_ok "$ASTARTE_PROTOCOL://api.$ASTARTE_BASE_DOMAIN/realmmanagement/health"; then

        echo -e "\nAstarte is ready."
        break
    fi

    for _ in 1 2 3; do
        printf "."
        sleep 1
    done
    printf "\b\b\b   \b\b\b"
done
