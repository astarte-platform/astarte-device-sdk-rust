#!/usr/bin/env bash

# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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

set -exEuo pipefail

file=$1

tmpout=$(mktemp)

# shellcheck disable=SC2016
sed 's/^```no_run$/```rust/' "$file" > "$tmpout"

# shellcheck disable=SC2016
to_delete=$(grep -zo '```rust[^`]\+```' "$tmpout" |
  tr '\0' '\n' |
  grep '^# ')

echo "$to_delete" | while read -r line; do
   # shellcheck disable=SC2001
   line=$(echo "$line" | sed 's|/|\\/|g')
   sed -i "/$line/d" "$tmpout"
done

cat "$tmpout"
