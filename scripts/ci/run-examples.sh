#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2026 SECO Mind Srl
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

set -eEuo pipefail

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
astartectl realm-management interfaces sync --non-interactive docs/interfaces/*.json
astartectl realm-management interfaces sync --non-interactive examples/**/interfaces/*.json
astartectl realm-management interfaces ls

register_new_device() {
    store_dir=$1

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
}

run_example() {
    example=$1
    store_dir=$2

    echo "==== RUNNING EXAMPLE $example ===="

    if [[ ! -f $store_dir/configuration.json ]]; then
        register_new_device "$store_dir"
    fi

    if [[ $ASTARTE_IGNORE_SSL == true ]]; then
        ignore_ssl="--ignore-ssl"
    fi

    cargo run \
        --locked \
        --features derive \
        --example "$example" \
        -- --config "$store_dir/configuration.json" -l 10 --timeout-secs 120 "${ignore_ssl[@]}"

}

if [[ $# == 2 ]]; then
    run_example "$1" "$2"
else
    # TODO: test the message-hub client
    examples=(
        individual_datastream
        object_datastream
        registration
        retention
    )

    for example in "${examples[@]}"; do
        store_dir=$(mktemp -d)

        run_example "$example" "$store_dir"

        rm -rf "$store_dir"
    done

    # Run property twice
    store_dir=$(mktemp -d)

    run_example "individual_properties" "$store_dir"

    run_example "individual_properties" "$store_dir"

    rm -rf "$store_dir"
fi
