// This file is part of Astarte.
//
// Copyright 2023-2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use darling::FromMeta;

/// The different possible ways to change case of fields in a struct.
#[derive(Debug, Copy, Clone, PartialEq, FromMeta)]
pub enum RenameRule {
    /// Rename to "lowercase" style.
    #[darling(rename = "lowercase")]
    Lower,
    /// Rename to "UPPERCASE" style.
    #[darling(rename = "UPPERCASE")]
    Upper,
    /// Rename to "PascalCase" style.
    #[darling(rename = "PascalCase")]
    Pascal,
    /// Rename to "camelCase" style.
    #[darling(rename = "camelCase")]
    Camel,
    /// Rename to "snake_case" style.
    #[darling(rename = "snake_case")]
    Snake,
    /// Rename to "SCREAMING_SNAKE_CASE" style.
    #[darling(rename = "SCREAMING_SNAKE_CASE")]
    ScreamingSnake,
    /// Rename to "kebab-case" style.
    #[darling(rename = "kebab-case")]
    Kebab,
    /// Rename to "SCREAMING-KEBAB-CASE" style.
    #[darling(rename = "SCREAMING-KEBAB-CASE")]
    ScreamingKebab,
}

impl RenameRule {
    /// Apply a renaming rule to a struct field, returning the version expected in the source.
    pub fn apply_to_field(&self, field: &str) -> String {
        match self {
            RenameRule::Lower | RenameRule::Snake => field.to_ascii_lowercase(),
            RenameRule::Upper => field.to_ascii_uppercase(),
            RenameRule::Pascal => {
                let mut pascal = String::new();
                let mut capitalize = true;

                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(ch);
                    }
                }

                pascal
            }
            RenameRule::Camel => {
                let pascal = RenameRule::Pascal.apply_to_field(field);

                pascal[..1].to_ascii_lowercase() + &pascal[1..]
            }
            RenameRule::ScreamingSnake => field.to_ascii_uppercase(),
            RenameRule::Kebab => field.replace('_', "-"),
            RenameRule::ScreamingKebab => RenameRule::ScreamingSnake
                .apply_to_field(field)
                .replace('_', "-"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rename_fields() {
        for &(original, upper, pascal, camel, screaming, kebab, screaming_kebab) in &[
            (
                "outcome", "OUTCOME", "Outcome", "outcome", "OUTCOME", "outcome", "OUTCOME",
            ),
            (
                "very_tasty",
                "VERY_TASTY",
                "VeryTasty",
                "veryTasty",
                "VERY_TASTY",
                "very-tasty",
                "VERY-TASTY",
            ),
            ("a", "A", "A", "a", "A", "a", "A"),
            ("z42", "Z42", "Z42", "z42", "Z42", "z42", "Z42"),
        ] {
            assert_eq!(RenameRule::Upper.apply_to_field(original), upper);
            assert_eq!(RenameRule::Pascal.apply_to_field(original), pascal);
            assert_eq!(RenameRule::Camel.apply_to_field(original), camel);
            assert_eq!(RenameRule::Snake.apply_to_field(original), original);
            assert_eq!(
                RenameRule::ScreamingSnake.apply_to_field(original),
                screaming
            );
            assert_eq!(RenameRule::Kebab.apply_to_field(original), kebab);
            assert_eq!(
                RenameRule::ScreamingKebab.apply_to_field(original),
                screaming_kebab
            );
        }
    }
}
