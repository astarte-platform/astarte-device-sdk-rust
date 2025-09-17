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

# THANKS GEMINI

pushd "$(dirname "$0")"
. ./common_config.sh
popd

# --- Pre-flight Checks ---

# 1. Check for root privileges
if [ "$EUID" -ne 0 ]; then
  echo "❌ This script must be run as root. Please use sudo."
  exit 1
fi

echo "🧹 Cleaning up network sandbox..."

# --- Cleanup Steps ---

# Delete the network namespace
# This automatically destroys the virtual interface inside it (veth-guest)
# and any tc rules on the host side (veth-host).
echo "[1/3] Deleting network namespace..."
if ! ip netns list | grep -q "$NAMESPACE"; then
    echo "👍 Network sandbox '$NAMESPACE' does not exist. Nothing to do."
else
    ip netns del "$NAMESPACE"
    echo "👍 Network sandbox '$NAMESPACE' deleted."
fi

# Remove the NAT rule from iptables
# We find the rule using the comment we added in the start script.
echo "[2/3] Removing NAT rule..."
# Get the rule number
RULE_NUM=$(nft -a list table ip nat | grep "net-sandbox-rule" | sed 's/.*# handle //')
if [ ! -z "$RULE_NUM" ]; then
    nft delete rule ip nat postrouting handle "$RULE_NUM"
else
    echo "  - Warning: Could not find the specific NAT rule to delete. It might have been removed already."
fi

# Disable IP forwarding (optional, but good for security)
# echo "[3/3] Restoring IP forwarding to default..."
# NOTE assuming it was already enabled
#sysctl -w net.ipv4.ip_forward=0 > /dev/null

echo "✅ Cleanup complete."
