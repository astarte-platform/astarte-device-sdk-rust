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

pushd "$(dirname "$0")"
. ./common_config.sh
popd

# --- Pre-flight Checks ---

# Check for root privileges
if [ "$EUID" -ne 0 ]; then
  echo "❌ This script must be run as root. Please use sudo."
  exit 1
fi

# Check if the main interface was found
if [ -z "$MAIN_INTERFACE" ]; then
  echo "❌ Could not determine the main network interface. Please set MAIN_INTERFACE manually."
  exit 1
fi

CURRENT_STATUS=$(sysctl -n net.ipv4.ip_forward)
# Check if ip forwarding is already enabled or enable it
if [ "$CURRENT_STATUS" -eq 1 ]; then
    echo "👍 IP forwarding is already enabled. No changes made."
else
  # If disabled save current status so that we can restore it later
  echo "$CURRENT_STATUS" > "$STATUS_FILE"
  echo "💾 Original status ($CURRENT_STATUS) saved to $STATUS_FILE"
  # enable ip forwarding
  echo "🟡 IP forwarding is disabled. Enabling it now..."
  sysctl -w net.ipv4.ip_forward=1 > /dev/null
fi

echo "🚀 Starting network sandbox..."
echo "---------------------------------"
echo "Namespace:      $NAMESPACE"
echo "Main Interface: $MAIN_INTERFACE"
echo "---------------------------------"

# --- Setup Steps ---

# Create the network namespace
echo "[1/5] Creating network namespace..."
ip netns add "$NAMESPACE"

# Create the virtual ethernet pair (the "cable")
echo "[2/5] Creating virtual network link..."
ip link add "$VETH_HOST" type veth peer name "$VETH_GUEST"
# Move one end of the "cable" into the namespace
ip link set "$VETH_GUEST" netns "$NAMESPACE"

# Configure IP addresses and bring up the links
echo "[3/5] Configuring IP addresses..."
ip addr add "$IP_HOST" dev "$VETH_HOST"
ip link set "$VETH_HOST" up

ip netns exec "$NAMESPACE" ip addr add "$IP_GUEST" dev "$VETH_GUEST"
ip netns exec "$NAMESPACE" ip link set "$VETH_GUEST" up
ip netns exec "$NAMESPACE" ip link set lo up
ip netns exec "$NAMESPACE" ip route add default via "$IP_GATEWAY"

echo "[4/5] Enabling NAT for internet access..."
# The rule is added with a comment so it's easy to find and delete later
# iptables -t nat -A POSTROUTING -s "${IP_GUEST%/*}" -o "$MAIN_INTERFACE" -j MASQUERADE -m comment --comment "net-sandbox-rule"
# NOTE translated with iptables-translate
nft add table nat
nft 'add chain nat postrouting { type nat hook postrouting priority 100 ; }'
nft add rule ip nat postrouting oifname "$MAIN_INTERFACE" ip saddr "${IP_GUEST%/*}" counter masquerade comment "net-sandbox-rule"

echo "[5/5] Setting default rules..."
tc qdisc add dev "$VETH_HOST" root netem delay "0ms" loss "0%"
