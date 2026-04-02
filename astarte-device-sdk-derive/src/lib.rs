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

//! Proc macro helpers for the [Astarte Device SDK](https://crates.io/crates/astarte-device-sdk)

use std::fmt::Debug;

use darling::{FromDeriveInput, FromField};
use proc_macro::TokenStream;

use quote::{quote, quote_spanned};
use syn::{GenericParam, Generics, parse_macro_input, parse_quote};

use crate::{case::RenameRule, event::FromEventDerive};

mod case;
mod event;

/// Handle for the `#[derive(IntoAstarteObject)]` derive macro.
///
/// Uses the `#[astarte_object(..)]` attribute for configuration.
///
/// ### Example
///
/// ```no_compile
/// #[derive(IntoAstarteObject)]
/// #[astarte_object(rename_all = "camelCase")]
/// struct Foo {
///     #[astarte_object(rename = "some")]
///     bar: String
/// }
/// ```
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(astarte_object), supports(struct_named))]
struct ObjectDerive {
    /// Rename the fields in the resulting HashMap, see the [`RenameRule`] variants.
    #[darling(default)]
    rename_all: Option<RenameRule>,
    /// name of the struct
    ident: syn::Ident,
    /// Generic bounds
    generics: syn::Generics,
    /// fields
    data: darling::ast::Data<(), ObjectField>,
}

impl ObjectDerive {
    fn quote(&self) -> darling::Result<proc_macro2::TokenStream> {
        let st_name = &self.ident;

        let Some(fields) = self.data.as_ref().take_struct() else {
            return Err(darling::Error::unsupported_shape_with_expected(
                "enum",
                &"object must be astruct",
            )
            .with_span(&self.ident));
        };

        let mut errors = darling::Error::accumulator();

        let capacity = fields.len();
        let fields = fields.iter()
            .filter_map(|field| {
            errors.handle_in(||
                field.field_name(self.rename_all).ok_or_else(||
                darling::Error::custom( "missing struct fields")
                    .with_span(&self.ident)
            ))
        }).map(|(field_i, field_n)| {
            quote_spanned! {field_i.span() =>
                // TODO *Temporarily* ignore this new lint will be fixed in a new pr
                #[allow(unknown_lints)]
                #[allow(clippy::unnecessary_fallible_conversions)]
                let v: astarte_device_sdk::types::AstarteData = ::std::convert::TryInto::try_into(value.#field_i)?;
                object.insert(#field_n.to_string(), v);
            }
        }).collect::<Vec<_>>();

        errors.finish()?;

        let generics = Self::add_trait_bound(&self.generics);
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        Ok(quote! {
            impl #impl_generics ::std::convert::TryFrom<#st_name #ty_generics> for astarte_device_sdk::aggregate::AstarteObject #where_clause {
                type Error = astarte_device_sdk::error::Error;

                fn try_from(value: #st_name #ty_generics) -> ::std::result::Result<Self, Self::Error> {
                    let mut object = Self::with_capacity(#capacity);
                    #(#fields)*
                    Ok(object)
                }
            }
        })
    }

    pub fn add_trait_bound(generics: &Generics) -> Generics {
        let mut generics = generics.clone();

        for param in &mut generics.params {
            if let GenericParam::Type(type_param) = param {
                type_param.bounds.push(parse_quote!(
                    std::convert::TryInto<astarte_device_sdk::types::AstarteData, Error = astarte_device_sdk::error::Error>
                ));
            }
        }

        generics
    }
}

#[derive(Debug, FromField)]
#[darling(attributes(astarte_object))]
struct ObjectField {
    /// Rename the fields to the given value
    #[darling(default)]
    rename: Option<String>,
    /// Name of the field
    ident: Option<syn::Ident>,
}

impl ObjectField {
    fn field_name(&self, rename_rule: Option<RenameRule>) -> Option<(&syn::Ident, String)> {
        self.ident.as_ref().map(|i| {
            let mut name = i.to_string();

            if let Some(rename) = &self.rename {
                name = rename.clone();
            } else if let Some(rename_rule) = rename_rule {
                name = rename_rule.apply_to_field(&name);
            }

            (i, name)
        })
    }
}

/// Derive macro `#[derive(IntoAstarteObject)]` to implement IntoAstarteObject.
///
/// ### Example
///
/// ```no_compile
/// #[derive(IntoAstarteObject)]
/// struct Foo {
///     bar: String
/// }
/// ```
#[proc_macro_derive(IntoAstarteObject, attributes(astarte_object))]
pub fn astarte_aggregate_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    match ObjectDerive::from_derive_input(&input).and_then(|obj| obj.quote()) {
        Ok(t) => t.into(),
        Err(error) => error.write_errors().into(),
    }
}

/// Derive macro `#[derive(FromEvent)]` to implement the FromEvent trait.
///
/// ### Example
///
/// To derive the trait for an individual.
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(interface = "com.example.Sensor")]
/// enum Sensor {
///     #[mapping(endpoint = "/sensor/luminosity")]
///     Luminosity(i32),
///     #[mapping(endpoint = "/sensor/temerature")]
///     Temperature(Option<f64>),
/// }
/// ```
///
/// To derive the trait it for an object.
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(interface = "com.example.Foo", path = "/obj", aggregation = "object")]
/// struct Foo {
///     bar: String
/// }
/// ```
///
///
/// To derive the trait it for a property.
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(interface = "com.example.Sensor", interface_type = "property", aggregation = "individual")]
/// enum Sensor {
///     #[mapping(endpoint = "/sensor/luminosity")]
///     Luminosity(i32),
///     #[mapping(endpoint = "/sensor/temerature")]
///     Temperature(Option<f64>),
/// }
/// ```
#[proc_macro_derive(FromEvent, attributes(from_event, mapping))]
pub fn from_event_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    let res = FromEventDerive::from_derive_input(&input).and_then(|from_event| from_event.quote());

    // Build the trait implementation
    match res {
        Ok(t) => t.into(),
        Err(err) => err.write_errors().into(),
    }
}
