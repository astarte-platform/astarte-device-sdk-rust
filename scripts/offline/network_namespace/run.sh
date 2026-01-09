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

# Run the application
echo "Executing application in the sandbox..."
echo "Executable:     $@"
echo "--- Application Output ---"
ip netns exec "$NAMESPACE" "$@"
echo "--- End of Output ---"

echo "✅ Application finished."
echo "👉 Run './destroy.sh' to clean up the environment."
