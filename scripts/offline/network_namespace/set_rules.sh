#! /usr/bin/env bash

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

set -exEuo pipefail

pushd "$(dirname "$0")"
. ./common_config.sh
popd

echo "Applying traffic control rules..."
LATENCY=$1
LOSS=$2

echo "  - Latency: $LATENCY"
echo "  - Loss:    $LOSS"

tc qdisc change dev "$VETH_HOST" root netem delay "$LATENCY" loss "$LOSS"
