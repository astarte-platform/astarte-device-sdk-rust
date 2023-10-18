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

mod case;

use std::collections::HashMap;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use quote::quote_spanned;
use syn::parse_macro_input;
use syn::Attribute;
use syn::GenericParam;
use syn::Generics;

use case::RenameRule;
use syn::parse_quote;
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
        let rename_all = match other.rename_all {
            Some(_) => other.rename_all,
            None => self.rename_all,
        };

        Self { rename_all }
    }
}

impl syn::parse::Parse for AggregateAttributes {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut vars: HashMap<String, syn::Expr> = syn::punctuated::Punctuated::<
            syn::MetaNameValue,
            syn::Token![,],
        >::parse_terminated(input)?
        .into_iter()
        .map(|v| {
            v.path
                .get_ident()
                .ok_or_else(|| {
                    syn::Error::new(v.path.span(), "expected an ident like `rename_all`")
                })
                .map(|i| (i.to_string(), v.value))
        })
        .collect::<syn::Result<_>>()?;

        let rename_all = match vars.remove("rename_all") {
            Some(expr) => {
                // Cursed expression literal match
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) = expr
                else {
                    return Err(syn::Error::new(
                        expr.span(),
                        "expression must be a string literal",
                    ));
                };

                let Ok(rule) = RenameRule::from_str(&lit.value()) else {
                    return Err(syn::Error::new(lit.span(), "invalid rename rule"));
                };

                Some(rule)
            }
            None => None,
        };

        Ok(Self { rename_all })
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

impl syn::parse::Parse for AggregateDerive {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ast = syn::DeriveInput::parse(input)?;

        // Find all the outer astarte_aggregate attributes and merge them
        let attrs = ast
            .attrs
            .iter()
            .filter_map(parse_astarte_aggregate_attribute)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .reduce(|first, second| first.merge(second))
            .unwrap_or_default();

        // TODO: this can support enums with inner aggregate structs
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

#[proc_macro_derive(AstarteAggregate)]
pub fn astarte_aggregate_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let aggregate = parse_macro_input!(input as AggregateDerive);

    // Build the trait implementation
    aggregate.quote().into()
}

/// Parse the `#[astarte_aggregate(..)]` attribute.
///
/// This will skip other attributes or return an error if the attribute parsing failed. We expected
/// the input to be an outer attribute in the form `astarte_aggregate(rename_all = "...")`.
fn parse_astarte_aggregate_attribute(attr: &Attribute) -> Option<syn::Result<AggregateAttributes>> {
    let is_attr = attr
        .path()
        .get_ident()
        .map(ToString::to_string)
        .filter(|ident| ident == "astarte_aggregate")
        .is_some();

    if !is_attr {
        return None;
    }

    // TODO: outer and inner attributes check?
    match &attr.meta {
        // We ignore the path since it can the from another macro or the `#[astarte_aggregate]` without
        // parameters, which we still consider valid.
        syn::Meta::Path(_) => None,
        syn::Meta::NameValue(name) => Some(Err(syn::Error::new(
            name.span(),
            "cannot be used as a named value, use it as a list `#[astarte_aggregate(..)]`",
        ))),
        //
        syn::Meta::List(list) => Some(syn::parse2::<AggregateAttributes>(list.tokens.clone())),
    }
}
