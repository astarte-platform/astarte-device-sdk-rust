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
mod tests {}
