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

set -eEuo pipefail

# Trap -e errors
trap 'echo "Exit status $? at line $LINENO from: $BASH_COMMAND"' ERR

version=$1
changelog=$2

if [[ ! "$version" =~ ^v([1-9][0-9]+|[0-9])\.([1-9][0-9]+|[0-9])\.([1-9][0-9]+|[0-9])$ ]]; then
    echo "The version must be in the form v1.2.3" >&2
    exit 1
fi

echo "Creating release for $version"

if ! git tag | grep -q -F "$version"; then
    git tag -m '' "$version"

    git push --tags upstream "$version"
else
    echo "Tag already exists"
fi

notes="Astarte Device SDK Rust $version release.

## CHANGELOG

$changelog
"

echo "Heres the notes: "
echo
echo "$notes"
echo

read -r -p "Are you sure? [y/n] " response
if [[ $response != 'y' ]]; then
    echo "repspond with y to continue"
    echo "exiting"
    exit 1
fi

gh release create "$version" \
    --repo 'astarte-platform/astarte-device-sdk-rust' \
    --verify-tag \
    --draft \
    --title="$version" \
    --notes="$notes"
