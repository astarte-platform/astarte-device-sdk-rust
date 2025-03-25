#!/usr/bin/env bash
# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

set -xeEuo pipefail

KEY="${KEY:-}"

if [[ -z $KEY && $# != 1 ]]; then
    echo "e2e.sh <key-path>"
    exit 1
fi

export RUST_LOG=${RUST_LOG:-debug}

astartectl realm-management interfaces sync -y \
    -u http://api.astarte.localhost \
    -r test \
    -k "$KEY" \
    e2e-test/interfaces/*.json \
    e2e-test/interfaces/additional/*.json

export E2E_IGNORE_SSL=true

E2E_DEVICE_ID="$(astartectl utils device-id generate-random)"
E2E_PAIRING_TOKEN="$(astartectl utils gen-jwt pairing -u http://api.astarte.localhost -k "$KEY")"
E2E_TOKEN="$(astartectl utils gen-jwt all-realm-apis -u http://api.astarte.localhost -k "$KEY")"
E2E_STORE_DIR="$(mktemp -d)"

export E2E_DEVICE_ID
export E2E_PAIRING_TOKEN
export E2E_TOKEN
export E2E_STORE_DIR

cargo e2e-test
