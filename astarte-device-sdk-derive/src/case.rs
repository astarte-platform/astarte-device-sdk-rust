/*
 * This file uses source code from the files /serde_derive/src/internals/case.rs, from
 * https://github.com/serde-rs/serde, copyright Erick Tryzelaar and David Tolnay, licensed under the
 * Apache 2.0 license.
 *
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
};

/// The different possible ways to change case of fields in a struct.
#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub enum RenameRule {
    /// Do not rename.
    #[default]
    None,
    /// Rename to "lowercase" style.
    LowerCase,
    /// Rename to "UPPERCASE" style.
    UpperCase,
    /// Rename to "PascalCase" style.
    PascalCase,
    /// Rename to "camelCase" style.
    CamelCase,
    /// Rename to "snake_case" style.
    SnakeCase,
    /// Rename to "SCREAMING_SNAKE_CASE" style.
    ScreamingSnakeCase,
    /// Rename to "kebab-case" style.
    KebabCase,
    /// Rename to "SCREAMING-KEBAB-CASE" style.
    ScreamingKebabCase,
}

static RENAME_RULES: &[(&str, RenameRule)] = &[
    ("lowercase", RenameRule::LowerCase),
    ("UPPERCASE", RenameRule::UpperCase),
    ("PascalCase", RenameRule::PascalCase),
    ("camelCase", RenameRule::CamelCase),
    ("snake_case", RenameRule::SnakeCase),
    ("SCREAMING_SNAKE_CASE", RenameRule::ScreamingSnakeCase),
    ("kebab-case", RenameRule::KebabCase),
    ("SCREAMING-KEBAB-CASE", RenameRule::ScreamingKebabCase),
];

impl RenameRule {
    /// Obrain a rename rule from a str
    pub fn from_str(rename_all_str: &str) -> Result<Self, ParseError<'_>> {
        for (name, rule) in RENAME_RULES {
            if rename_all_str == *name {
                return Ok(*rule);
            }
        }
        Err(ParseError {
            unknown: rename_all_str,
        })
    }

    /// Apply a renaming rule to a struct field, returning the version expected in the source.
    pub fn apply_to_field<'a>(&self, field: &'a str) -> Cow<'a, str> {
        match *self {
            RenameRule::None => Cow::Borrowed(field),
            RenameRule::LowerCase | RenameRule::SnakeCase => field.to_ascii_lowercase().into(),
            RenameRule::UpperCase => field.to_ascii_uppercase().into(),
            RenameRule::PascalCase => {
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
                Cow::Owned(pascal)
            }
            RenameRule::CamelCase => {
                let pascal = RenameRule::PascalCase.apply_to_field(field);
                Cow::Owned(pascal[..1].to_ascii_lowercase() + &pascal[1..])
            }
            RenameRule::ScreamingSnakeCase => field.to_ascii_uppercase().into(),
            RenameRule::KebabCase => field.replace('_', "-").into(),
            RenameRule::ScreamingKebabCase => RenameRule::ScreamingSnakeCase
                .apply_to_field(field)
                .replace('_', "-")
                .into(),
        }
    }
}

#[derive(Debug)]
pub struct ParseError<'a> {
    unknown: &'a str,
}

impl Display for ParseError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("unknown rename rule `rename_all = ")?;
        Debug::fmt(self.unknown, f)?;
        f.write_str("`, expected one of ")?;
        for (i, (name, _rule)) in RENAME_RULES.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            Debug::fmt(name, f)?;
        }
        Ok(())
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
            assert_eq!(RenameRule::None.apply_to_field(original), original);
            assert_eq!(RenameRule::UpperCase.apply_to_field(original), upper);
            assert_eq!(RenameRule::PascalCase.apply_to_field(original), pascal);
            assert_eq!(RenameRule::CamelCase.apply_to_field(original), camel);
            assert_eq!(RenameRule::SnakeCase.apply_to_field(original), original);
            assert_eq!(
                RenameRule::ScreamingSnakeCase.apply_to_field(original),
                screaming
            );
            assert_eq!(RenameRule::KebabCase.apply_to_field(original), kebab);
            assert_eq!(
                RenameRule::ScreamingKebabCase.apply_to_field(original),
                screaming_kebab
            );
        }
    }
}
