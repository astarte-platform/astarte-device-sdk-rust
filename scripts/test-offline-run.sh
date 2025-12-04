#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2024 - 2025 SECO Mind Srl
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

CONFIG=""
KEY=""
URL=""
REALM=""

while getopts ":k:c:u:r:" opt; do
  case $opt in
    c)
        CONFIG="$OPTARG"
        ;;
    k)
        KEY="$OPTARG"
        ;;
    u)
        URL="$OPTARG"
        ;;
    r)
        REALM="$OPTARG"
        ;;
    :)
        echo "Error: Option -$OPTARG requires an argument."
        usage
        exit 1
        ;;
    \?)
        echo "Invalid option -$OPTARG" >&2
        usage
        exit 1
        ;;
  esac

  # case $OPTARG in
  #   -*)
  #       echo "Option $opt needs a valid argument"
  #       exit 1
  #       ;;
  # esac
done

STORE_DIR="$(mktemp -d)"

if [ -z "$KEY" ] && [ -z "$CONFIG" ] || { ! [ -z "$KEY" ] && ! [ -z "$CONFIG" ]; }; then
    echo "Provide one argument between KEY and CONFIG"
    exit 1
elif ! [ -z "$KEY" ]; then
    if [ -z "$URL" ]; then
        URL="http://api.astarte.localhost"
    fi
    if [ -z "$REALM" ]; then
        REALM="test"
    fi

    echo "Using '$URL' as default url"
    echo "Using '$REALM' as default realm"

    PAIRING_TOKEN="$(astartectl utils gen-jwt pairing -u "$URL" -k "$KEY")"

    echo "Creating new device in realm '$REALM'"
    DEVICE_ID="$(astartectl utils device-id generate-random)"
    CREDENTIAL_SECRET=$(astartectl pairing agent register "$DEVICE_ID" -t "$PAIRING_TOKEN" --compact-output)

    echo "Syncing interfacing by using the passed key"
    astartectl realm-management interfaces sync -y \
        -u "$URL" \
        -r "$REALM" \
        -k "$KEY" \
        examples/retention/interfaces/*.json

    CONFIG="$STORE_DIR/device-local-offline.json"
    echo "Storing temp config in '$CONFIG'"
    tee -a "$CONFIG" << END
    {
        "realm": "$REALM",
        "device_id": "$DEVICE_ID",
        "credentials_secret": "$CREDENTIAL_SECRET",
        "pairing_url": "$PAIRING_URL"
    }
END
elif ! [ -z "$CONFIG" ]; then
    if ! [ -z "$URL" ] || ! [ -z "$REALM" ]; then
        echo "URL and REALM options are only allowed when using the KEY option by itself"
        echo "URL and REALM should be read from the configuration file"
        exit 1
    fi
fi

sudo ./scripts/offline/network_namespace/create.sh
# set offline by dropping all packets
sudo ./scripts/offline/network_namespace/set_rules.sh 0ms 100%

cargo b --example retention -F "derive"

sudo ./scripts/offline/network_namespace/run.sh \
  ./target/debug/examples/retention -c "$CONFIG" > "$STORE_DIR/offline_log.out" 2>&1 &

RUNNING_EXAMPLE_PID=$!

sleep 15

echo "Setting dropped packets to 0%"
# set online by setting 0% dropped packets
sudo ./scripts/offline/network_namespace/set_rules.sh 0ms 0%

sleep 35

echo "Killing running example"
kill $RUNNING_EXAMPLE_PID || {
    echo "Process exited early"
    sudo ./scripts/offline/network_namespace/destroy.sh
    exit 1
}

echo "===== example stdout ====="
cat "$STORE_DIR/offline_log.out"
echo "===== end stdout ====="

sudo ./scripts/offline/network_namespace/destroy.sh
