/*
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

//! Proc macro helpers for the [Astarte Device SDK](https://crates.io/crates/astarte-device-sdk)

mod case;

use std::collections::HashMap;
use std::fmt::Debug;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use quote::quote_spanned;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;
use syn::Attribute;
use syn::Expr;
use syn::GenericParam;
use syn::Generics;
use syn::MetaNameValue;
use syn::Token;

use case::RenameRule;
use syn::parse_quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;

/// Handle for the `#[astarte_aggregate(..)]` attribute.
///
/// ### Example
///
/// ```no_compile
/// #[derive(AstarteAggregate)]
/// #[astarte_aggregate(rename_all = "camelCase")]
/// struct Foo {
///     bar_v: String
/// }
/// ```
#[derive(Debug, Default)]
struct AggregateAttributes {
    /// Rename the fields in the resulting HashMap, see the [`RenameRule`] variants.
    rename_all: Option<RenameRule>,
}

impl AggregateAttributes {
    /// Merge the Astarte attributes from the other struct into self.
    fn merge(self, other: Self) -> Self {
        let rename_all = other.rename_all.or(self.rename_all);

        Self { rename_all }
    }
}

impl Parse for AggregateAttributes {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut vars = parse_name_value_attrs(input)?;

        let rename_all = vars
            .remove("rename_all")
            .map(|expr| {
                parse_str_lit(&expr).and_then(|rename| {
                    RenameRule::from_str(&rename)
                        .map_err(|_| syn::Error::new(expr.span(), "invalid rename rule"))
                })
            })
            .transpose()?;

        Ok(Self { rename_all })
    }
}

/// Parses the content of a [`syn::MetaList`] as a list of [`syn::MetaNameValue`].
///
/// Will convert a list of `#[attr(name = "string",..)]` into an [`HashMap<String, string>`]
fn parse_name_value_attrs(
    input: &syn::parse::ParseBuffer<'_>,
) -> Result<HashMap<String, Expr>, syn::Error> {
    Punctuated::<MetaNameValue, Token![,]>::parse_terminated(input)?
        .into_iter()
        .map(|v| {
            v.path
                .get_ident()
                .ok_or_else(|| {
                    syn::Error::new(v.span(), "expected an identifier like `rename_all`")
                })
                .map(|i| (i.to_string(), v.value))
        })
        .collect::<syn::Result<_>>()
}

/// Parses a [`syn::Lit::Str`] into a [`String`].
fn parse_str_lit(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit),
            ..
        }) => Ok(lit.value()),
        _ => Err(syn::Error::new(
            expr.span(),
            "expression must be a string literal",
        )),
    }
}

/// Handle for the `#[derive(AstarteAggregate)]` derive macro.
///
/// ### Example
///
/// ```no_compile
/// #[derive(AstarteAggregate)]
/// struct Foo {
///     bar: String
/// }
/// ```
struct AggregateDerive {
    name: Ident,
    attrs: AggregateAttributes,
    fields: Vec<Ident>,
    generics: Generics,
}

impl AggregateDerive {
    fn quote(&self) -> proc_macro2::TokenStream {
        let rename_rule = self.attrs.rename_all.unwrap_or_default();

        let name = &self.name;
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let fields = self.fields.iter().map(|i| {
            let name = i.to_string();
            let name = rename_rule.apply_to_field(&name);
            quote_spanned! {i.span() =>
                // TODO *Temporarily* ignore this new lint will be fixed in a new pr
                #[allow(unknown_lints)]
                #[allow(clippy::unnecessary_fallible_conversions)]
                let value: astarte_device_sdk::types::AstarteType = std::convert::TryInto::try_into(self.#i)?;
                result.insert(#name.to_string(), value);
            }
        });

        quote! {
            impl #impl_generics astarte_device_sdk::AstarteAggregate for #name #ty_generics #where_clause {
                fn astarte_aggregate(
                    self,
                ) -> Result<
                    std::collections::HashMap<String, astarte_device_sdk::types::AstarteType>,
                    astarte_device_sdk::error::Error,
                > {
                    let mut result = std::collections::HashMap::new();
                    #(#fields)*
                    Ok(result)
                }
            }
        }
    }

    pub fn add_trait_bound(mut generics: Generics) -> Generics {
        for param in &mut generics.params {
            if let GenericParam::Type(ref mut type_param) = *param {
                type_param.bounds.push(parse_quote!(
                    std::convert::TryInto<astarte_device_sdk::types::AstarteType, Error = astarte_device_sdk::error::Error>
                ));
            }
        }
        generics
    }
}

impl Parse for AggregateDerive {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ast = syn::DeriveInput::parse(input)?;

        // Find all the outer astarte_aggregate attributes and merge them
        let attrs = ast
            .attrs
            .iter()
            .filter_map(|a| parse_attribute_list::<AggregateAttributes>(a, "astarte_aggregate"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .reduce(|first, second| first.merge(second))
            .unwrap_or_default();

        let fields = parse_struct_fields(&ast)?;

        let name = ast.ident;

        let generics = Self::add_trait_bound(ast.generics);

        Ok(Self {
            name,
            attrs,
            fields,
            generics,
        })
    }
}

#[derive(Debug, Default)]
struct FromEventAttrs {
    interface: Option<String>,
    path: Option<String>,
    rename_rule: Option<RenameRule>,
}

impl FromEventAttrs {
    fn merge(self, other: Self) -> Self {
        let interface = other.interface.or(self.interface);
        let path = other.path.or(self.path);
        let rename_all = other.rename_rule.or(self.rename_rule);

        Self {
            interface,
            path,
            rename_rule: rename_all,
        }
    }
}

impl Parse for FromEventAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut vars = parse_name_value_attrs(input)?;

        let interface = vars
            .remove("interface")
            .map(|expr| parse_str_lit(&expr))
            .transpose()?;

        let path = vars
            .remove("path")
            .map(|expr| parse_str_lit(&expr))
            .transpose()?;

        let rename_all = vars
            .remove("rename_all")
            .map(|expr| {
                parse_str_lit(&expr).and_then(|rename| {
                    RenameRule::from_str(&rename)
                        .map_err(|_| syn::Error::new(expr.span(), "invalid rename rule"))
                })
            })
            .transpose()?;

        Ok(Self {
            rename_rule: rename_all,
            interface,
            path,
        })
    }
}

struct FromEventDerive {
    interface: String,
    path: String,
    name: Ident,
    rename_rule: Option<RenameRule>,
    generics: Generics,
    fields: Vec<Ident>,
}

impl FromEventDerive {
    fn add_trait_bound(mut generics: Generics) -> Generics {
        for param in &mut generics.params {
            if let GenericParam::Type(ref mut type_param) = *param {
                type_param.bounds.push(parse_quote!(
                    std::convert::TryFrom<astarte_device_sdk::types::AstarteType, Error =  >
                ));
            }
        }
        generics
    }

    fn quote(&self) -> proc_macro2::TokenStream {
        let rename_rule = self.rename_rule.unwrap_or_default();
        let (impl_generics, ty_generics, where_clause) = &self.generics.split_for_impl();
        let fields_val = self.fields.iter().map(|i| {
            let name = i.to_string();
            let name = rename_rule.apply_to_field(&name);
            quote_spanned! {i.span() =>
                let #i = object
                    .remove(#name)
                    .ok_or(FromEventError::MissingField {
                        interface,
                        base_path,
                        path: #name,
                    })?
                    .try_into()?;
            }
        });
        let fields = self.fields.iter();
        let interface = &self.interface;
        let path = &self.path;
        let name = &self.name;

        quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #name #ty_generics #where_clause {
                type Err = astarte_device_sdk::event::FromEventError;

                fn from_event(event: astarte_device_sdk::AstarteDeviceDataEvent) -> Result<Self, Self::Err> {
                    use astarte_device_sdk::Aggregation;
                    use astarte_device_sdk::event::FromEventError;
                    use astarte_device_sdk::interface::mapping::endpoint::Endpoint;

                    let interface = #interface;
                    let base_path = #path;
                    let endpoint: Endpoint<&str> = Endpoint::try_from(base_path)?;

                    if event.interface != interface {
                        return Err(FromEventError::Interface(event.interface.clone()));
                    }

                    if !endpoint.eq_mapping(&event.path) {
                        return Err(FromEventError::Path {
                            interface,
                            base_path: event.path.clone(),
                        });
                    }

                    let Aggregation::Object(mut object) = event.data else {
                        return Err(FromEventError::Individual {
                            interface,
                            base_path,
                        });
                    };

                    #(#fields_val)*

                    Ok(Self{#(#fields),*})
                }
            }
        }
    }
}

impl Parse for FromEventDerive {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ast = syn::DeriveInput::parse(input)?;

        // Find all the outer astarte_aggregate attributes and merge them
        let attrs = ast
            .attrs
            .iter()
            .filter_map(|a| parse_attribute_list::<FromEventAttrs>(a, "from_event"))
            .collect::<Result<Vec<_>, _>>()?
            // TODO: check for duplicates
            .into_iter()
            .reduce(|first, other| first.merge(other))
            .ok_or_else(|| {
                syn::Error::new(
                    ast.span(),
                    r#"missing attributes #[from_event(interface = "..", path = "..")]"#,
                )
            })?;

        let interface = attrs.interface.ok_or_else(|| {
            syn::Error::new(
                ast.span(),
                r#"missing interface attribute #[from_event(interface = "..")]"#,
            )
        })?;

        let path = attrs.path.ok_or_else(|| {
            syn::Error::new(
                ast.span(),
                r#"missing path attribute #[from_event(path = "..")]"#,
            )
        })?;

        let fields = parse_struct_fields(&ast)?;

        let generics = Self::add_trait_bound(ast.generics);

        Ok(Self {
            interface,
            path,
            rename_rule: attrs.rename_rule,
            name: ast.ident,
            generics,
            fields,
        })
    }
}

/// Parses the fields of a struct
fn parse_struct_fields(ast: &syn::DeriveInput) -> Result<Vec<Ident>, syn::Error> {
    let syn::Data::Struct(ref st) = ast.data else {
        return Err(syn::Error::new(
            ast.span(),
            "AstarteAggregate is only implementable over a struct.",
        ));
    };
    let syn::Fields::Named(ref fields_named) = st.fields else {
        return Err(syn::Error::new(
            ast.span(),
            "AstarteAggregate is only implementable over a named struct.",
        ));
    };
    let fields = fields_named
        .named
        .iter()
        .map(|field| {
            field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new(field.span(), "field is not an ident"))
        })
        .collect::<Result<_, _>>()?;
    Ok(fields)
}

/// Parse the `#[name(..)]` attribute.
///
/// This will skip other attributes or return an error if the attribute parsing failed. We expected
/// the input to be an outer attribute in the form `#[name(foo = "...")]`.
fn parse_attribute_list<T>(attr: &Attribute, name: &str) -> Option<syn::Result<T>>
where
    T: Parse,
{
    let is_attr = attr
        .path()
        .get_ident()
        .map(ToString::to_string)
        .filter(|ident| ident == name)
        .is_some();

    if !is_attr {
        return None;
    }

    // TODO: outer and inner attributes check?
    match &attr.meta {
        // We ignore the path since it can be from another macro or `#[astarte_aggregate]` without
        // parameters, which we still consider valid.
        syn::Meta::Path(_) => None,
        syn::Meta::NameValue(name) => Some(Err(syn::Error::new(
            name.span(),
            "cannot be used as a named value",
        ))),
        syn::Meta::List(list) => Some(syn::parse2::<T>(list.tokens.clone())),
    }
}

/// Handle for the `#[astarte_aggregate(..)]` attribute.
///
/// ### Example
///
/// ```no_compile
/// #[derive(AstarteAggregate)]
/// #[astarte_aggregate(rename_all = "camelCase")]
/// struct Foo {
///     bar_v: String
/// }
/// ```
#[proc_macro_attribute]
pub fn astarte_aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let _ast_attr = parse_macro_input!(attr as AggregateAttributes);
    let ast_item = parse_macro_input!(item as syn::ItemStruct);

    // Do not perform any operation. All the checks and changes are performed by the derive
    // macro.
    TokenStream::from(quote!(#ast_item))
}

/// Derive macro `#[derive(AstarteAggregate)]` to implement AstarteAggregate.
///
/// ### Example
///
/// ```no_compile
/// #[derive(AstarteAggregate)]
/// struct Foo {
///     bar: String
/// }
/// ```
#[proc_macro_derive(AstarteAggregate)]
pub fn astarte_aggregate_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let aggregate = parse_macro_input!(input as AggregateDerive);

    // Build the trait implementation
    aggregate.quote().into()
}

/// Handle for the `#[from_event(..)]` macro attribute.
///
/// ### Example
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(interface = "com.example.Foo", path = "obj")]
/// struct Foo {
///     bar: String
/// }
/// ```
#[proc_macro_attribute]
pub fn from_event(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let _ast_attr = parse_macro_input!(attr as FromEventAttrs);
    let ast_item = parse_macro_input!(item as syn::ItemStruct);

    // Do not perform any operation. All the checks and changes are performed by the derive
    // macro.
    TokenStream::from(quote!(#ast_item))
}

/// Derive macro `#[derive(FromEvent)]` to implement the FromEvent trait.
///
/// ### Example
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(interface = "com.example.Foo", path = "obj")]
/// struct Foo {
///     bar: String
/// }
/// ```
#[proc_macro_derive(FromEvent)]
pub fn from_event_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let from_event = parse_macro_input!(input as FromEventDerive);

    // Build the trait implementation
    from_event.quote().into()
}
