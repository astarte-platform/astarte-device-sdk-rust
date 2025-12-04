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

set -eEuox pipefail

pushd "$(dirname "$0")"
. ./common_config.sh
popd

# --- Pre-flight Checks ---

# Check for root privileges
if [ "$EUID" -ne 0 ]; then
  echo "❌ This script must be run as root. Please use sudo."
  exit 1
fi

echo "🧹 Cleaning up network sandbox..."

# --- Cleanup Steps ---
# Check if the status file exists
if [ ! -f "$STATUS_FILE" ]; then
    echo "🤷 No status file found at '$STATUS_FILE'. No ip forwarding setting to restore."
else
    # Read the original status from the file
    ORIGINAL_STATUS=$(cat "$STATUS_FILE")
    # Restore the IP forwarding setting
    echo "🔄 Restoring IP forwarding to its original state ('$ORIGINAL_STATUS')..."
    sysctl -w net.ipv4.ip_forward="$ORIGINAL_STATUS" > /dev/null
fi

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
RULE_NUM="$(nft -a list table ip nat | grep "net-sandbox-rule" | sed 's/.*# handle //' || true)"
if [ ! -z "$RULE_NUM" ]; then
    nft delete rule ip nat postrouting handle "$RULE_NUM"
else
    echo "  - Warning: Could not find the specific NAT rule to delete. It might have been removed already."
fi

echo "[3/3] Removing bridge interface"
ip link delete "$VETH_GUEST" || echo "$VETH_GUEST already removed"
ip link delete "$VETH_HOST" || echo "$VETH_HOST already removed"

echo "✅ Cleanup complete."
