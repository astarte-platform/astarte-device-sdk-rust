#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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

set -exEuo pipefail

# Check if the crate can be compiled with only the files that will be packaged when publishing

# List files in a package
listPackage() {
    cargo package -l -p "$1" | xargs -I '{}' echo "$1/{}"
}

pkgsFiles=$(
    cat <(cargo package -l -p "astarte-device-sdk") \
        <(listPackage "astarte-device-sdk-derive") \
        <(listPackage "astarte-device-sdk-mock") \
        <(listPackage "e2e-test") |
        sort
)
localFiles=$(
    git ls-files | sort
)

# List files unique to localFiles and not present in pkgsFiles
toRemove=$(comm -23 <(echo "$localFiles") <(echo "$pkgsFiles"))

echo "$toRemove" | xargs --no-run-if-empty rm -v

cargo check --workspace --all-features --locked
