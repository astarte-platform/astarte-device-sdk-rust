#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2024-2026 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

set -xeEuo pipefail

# Let's you enable debug mode in the github action
if [[ -n ${RUNNER_DEBUG:-} ]]; then
    set -x
fi

# Trap -e errors
trap 'echo "Exit status $? at line $LINENO from: $BASH_COMMAND"' ERR

export ASTARTE_REALM=${ASTARTE_REALM:-test}
export ASTARTE_BASE_DOMAIN=${ASTARTE_BASE_DOMAIN:-autotest.astarte-platform.org}
export ASTARTE_IGNORE_SSL=${ASTARTE_IGNORE_SSL:-false}
export ASTARTE_API_SCHEMA=${ASTARTE_API_SCHEMA:-https}
export ASTARTE_API_URL=${ASTARTE_API_URL:-$ASTARTE_API_SCHEMA://api.$ASTARTE_BASE_DOMAIN}
export ASTARTE_PAIRING_URL=${ASTARTE_PAIRING_URL:-$ASTARTE_API_URL/pairing}

# install interfaces
astartectl realm-management interfaces sync --non-interactive examples/retention/interfaces/*.json
astartectl realm-management interfaces ls

store_dir="$(mktemp -d)"

astarte_device_id=$(astartectl utils device-id generate-random)
astarte_pairing_token=$(astartectl utils gen-jwt pairing)

jq --null-input \
    --arg realm "$ASTARTE_REALM" \
    --arg device_id "$astarte_device_id" \
    --arg token "$astarte_pairing_token" \
    --arg url "$ASTARTE_PAIRING_URL" \
    --arg dir "$store_dir" \
    '{
            "realm": $realm,
            "device_id": $device_id,
            "pairing_token": $token,
            "pairing_url": $url,
            "store_dir": $dir
        }' >"$store_dir/configuration.json"

sudo ./scripts/offline/network_namespace/create.sh

#cargo build --example retention --features "derive"

# shellcheck disable=SC2024
sudo ./scripts/offline/network_namespace/run.sh \
    ./target/debug/examples/retention \
    --config "$store_dir/configuration.json" \
    -l 30 --timeout-secs 120 >"$store_dir/offline_log.out" &

running_example_pid=$!

sleep 5

# set offline by dropping all packets
sudo ./scripts/offline/network_namespace/set_rules.sh 0ms 100%

# wait for all 30 packets to be sent
wait $running_example_pid

echo "===== example stdout ====="
cat "$store_dir/offline_log.out"
echo "===== end stdout ====="

echo "Setting dropped packets to 0%"
# set online by setting 0% dropped packets
sudo ./scripts/offline/network_namespace/set_rules.sh 0ms 0%

# shellcheck disable=SC2024
sudo ./scripts/offline/network_namespace/run.sh \
    ./target/debug/examples/retention \
    --config "$store_dir/configuration.json" \
    -l 30 --timeout-secs 120

sudo ./scripts/offline/network_namespace/destroy.sh
