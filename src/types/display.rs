// This file is part of Astarte.
//
// Copyright 2025 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Implements display for the [`AstarteData`].

use std::fmt::Display;

use base64::display::Base64Display;

use super::AstarteData;

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE;

impl Display for AstarteData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AstarteData::Double(float) => float.fmt(f),
            AstarteData::Integer(v) => v.fmt(f),
            AstarteData::Boolean(v) => v.fmt(f),
            AstarteData::LongInteger(v) => v.fmt(f),
            AstarteData::String(v) => v.fmt(f),
            AstarteData::BinaryBlob(items) => Base64Display::new(items, &BASE64).fmt(f),
            AstarteData::DateTime(date_time) => date_time
                .format_with_items(std::iter::once(chrono::format::Item::Fixed(
                    chrono::format::Fixed::RFC3339,
                )))
                .fmt(f),
            AstarteData::DoubleArray(floats) => fmt_iter(floats, f),
            AstarteData::IntegerArray(items) => fmt_iter(items, f),
            AstarteData::BooleanArray(items) => fmt_iter(items, f),
            AstarteData::LongIntegerArray(items) => fmt_iter(items, f),
            AstarteData::StringArray(items) => {
                // Escape the strings
                let iter = items.iter().map(|s| s.escape_debug());

                fmt_iter(iter, f)
            }
            AstarteData::BinaryBlobArray(items) => {
                let iter = items.iter().map(|bytes| Base64Display::new(bytes, &BASE64));

                fmt_iter(iter, f)
            }
            AstarteData::DateTimeArray(date_times) => {
                // Escape the strings
                let iter = date_times.iter().map(|date| {
                    date.format_with_items(std::iter::once(chrono::format::Item::Fixed(
                        chrono::format::Fixed::RFC3339,
                    )))
                });

                fmt_iter(iter, f)
            }
        }
    }
}

fn fmt_iter<I>(iter: I, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
where
    I: IntoIterator,
    I::Item: Display,
{
    if f.alternate() {
        return fmt_iter_alternate(iter, f);
    }

    let mut iter = iter.into_iter();

    let Some(first) = iter.next() else {
        return write!(f, "[]");
    };

    write!(f, "[")?;
    first.fmt(f)?;

    for i in iter {
        write!(f, ", ")?;
        i.fmt(f)?;
    }

    write!(f, "]")
}

fn fmt_iter_alternate<I>(iter: I, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
where
    I: IntoIterator,
    I::Item: Display,
{
    let mut iter = iter.into_iter();

    let Some(first) = iter.next() else {
        return writeln!(f, "[]");
    };

    writeln!(f, "[")?;

    write!(f, "  ")?;
    first.fmt(f)?;
    writeln!(f, ",")?;

    for i in iter {
        write!(f, "  ")?;
        i.fmt(f)?;
        writeln!(f, ",")?;
    }

    writeln!(f, "]")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    use crate::types::test::all_astarte_types;

    #[test]
    fn should_display() {
        let data = all_astarte_types();

        for case in data {
            match case {
                AstarteData::Double(_) => {
                    assert_eq!(format!("{case}"), "12.21");
                }
                AstarteData::Integer(_) => {
                    assert_eq!(format!("{case}"), "12");
                }
                AstarteData::Boolean(_) => {
                    assert_eq!(format!("{case}"), "false");
                }
                AstarteData::LongInteger(_) => {
                    assert_eq!(format!("{case}"), "42");
                }
                AstarteData::String(_) => {
                    assert_eq!(format!("{case}"), "hello");
                }
                AstarteData::BinaryBlob(_) => {
                    assert_eq!(format!("{case}"), "AQIDBA==");
                }
                AstarteData::DateTime(_) => {
                    assert_eq!(format!("{case}"), "2021-07-29T17:46:48+00:00");
                }
                AstarteData::DoubleArray(_) => {
                    assert_eq!(format!("{case}"), "[1.3, 2.6, 3.1, 4]");
                    assert_eq!(format!("{case:#}"), "[\n  1.3,\n  2.6,\n  3.1,\n  4,\n]\n");
                }
                AstarteData::IntegerArray(_) => {
                    assert_eq!(format!("{case}"), "[1, 2, 3, 4]");
                    assert_eq!(format!("{case:#}"), "[\n  1,\n  2,\n  3,\n  4,\n]\n");
                }
                AstarteData::BooleanArray(_) => {
                    assert_eq!(format!("{case}"), "[true, false, true, true]");
                    assert_eq!(
                        format!("{case:#}"),
                        "[\n  true,\n  false,\n  true,\n  true,\n]\n"
                    );
                }
                AstarteData::LongIntegerArray(_) => {
                    assert_eq!(format!("{case}"), "[32, 11, 33, 1]");
                    assert_eq!(format!("{case:#}"), "[\n  32,\n  11,\n  33,\n  1,\n]\n");
                }
                AstarteData::StringArray(_) => {
                    assert_eq!(format!("{case}"), r#"[Hello,  world!]"#);
                    assert_eq!(format!("{case:#}"), "[\n  Hello,\n   world!,\n]\n");
                }
                AstarteData::BinaryBlobArray(_) => {
                    assert_eq!(format!("{case}"), r#"[AQIDBA==, BAQBBA==]"#);
                    assert_eq!(format!("{case:#}"), "[\n  AQIDBA==,\n  BAQBBA==,\n]\n");
                }
                AstarteData::DateTimeArray(_) => {
                    assert_eq!(
                        format!("{case}"),
                        r#"[2021-07-29T17:46:48+00:00, 2021-01-25T13:20:08+00:00]"#
                    );
                    assert_eq!(
                        format!("{case:#}"),
                        "[\n  2021-07-29T17:46:48+00:00,\n  2021-01-25T13:20:08+00:00,\n]\n"
                    );
                }
            }
        }

        // empty array
        let case = AstarteData::IntegerArray([].to_vec());
        assert_eq!(format!("{case}"), "[]");
        assert_eq!(format!("{case:#}"), "[]\n");
    }
}
