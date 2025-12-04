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

set -eEuo pipefail

# --- Configuration ---
# The name for your network namespace
NAMESPACE="my-test-ns"
# Automatically find the main internet-facing interface
MAIN_INTERFACE=$(ip route | grep '^default' | awk '{print $5}' | head -1)

# The virtual "network cable" interfaces
VETH_HOST="veth-host"
VETH_GUEST="veth-guest"

# IP addresses for the virtual link
IP_HOST="10.10.10.1/24"
IP_GUEST="10.10.10.2/24"
IP_GATEWAY="10.10.10.1"

# File used to store the status of ip forwarding before enabling it
STATUS_FILE="/tmp/ip_forward_status.bak"
