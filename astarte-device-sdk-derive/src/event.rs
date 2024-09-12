// This file is part of Astarte.
//
// Copyright 2024 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Derives the `FromEvent` trait.

use std::fmt::Debug;

use proc_macro2::Ident;
use quote::{quote, quote_spanned};
use syn::{
    parse::{Parse, ParseStream},
    parse_quote,
    spanned::Spanned,
    Expr, GenericParam, Generics, Variant,
};

use crate::{
    case::RenameRule, parse_attribute_list, parse_bool_lit, parse_name_value_attrs, parse_str_lit,
    parse_struct_fields,
};

#[derive(Debug, Default)]
pub(crate) struct FromEventAttrs {
    interface: Option<String>,
    path: Option<String>,
    rename_rule: Option<RenameRule>,
    // Use an option, so it can be merged if declared multiple times.
    aggregation: Option<Aggregation>,
}

impl FromEventAttrs {
    fn merge(self, other: Self) -> Self {
        let interface = other.interface.or(self.interface);
        let path = other.path.or(self.path);
        let rename_rule = other.rename_rule.or(self.rename_rule);
        let aggregation = other.aggregation.or(self.aggregation);

        Self {
            interface,
            path,
            rename_rule,
            aggregation,
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

        let aggregation = vars
            .remove("aggregation")
            .map(Aggregation::try_from)
            .transpose()?;

        Ok(Self {
            rename_rule: rename_all,
            interface,
            path,
            aggregation,
        })
    }
}

#[derive(Debug, Default)]
enum Aggregation {
    Individual,
    #[default]
    Object,
}

impl TryFrom<Expr> for Aggregation {
    type Error = syn::Error;

    fn try_from(value: Expr) -> Result<Self, Self::Error> {
        parse_str_lit(&value).and_then(|val| match val.as_str() {
            "individual" => Ok(Aggregation::Individual),
            "object" => Ok(Aggregation::Object),
            _ => Err(syn::Error::new(
                value.span(),
                "invalid aggregation, should be: individual or object",
            )),
        })
    }
}

/// Parses the derive for the FromEvent trait
pub(crate) struct FromEventDerive {
    interface: String,
    name: Ident,
    rename_rule: Option<RenameRule>,
    generics: Generics,
    aggregation: FromEventAggregation,
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

    pub(crate) fn quote(&self) -> proc_macro2::TokenStream {
        match &self.aggregation {
            FromEventAggregation::Individual { variants } => self.quote_indv(variants),
            FromEventAggregation::Object { fields, path } => self.quote_obj(path, fields),
        }
    }

    pub(crate) fn quote_obj(&self, path: &str, fields: &[Ident]) -> proc_macro2::TokenStream {
        let rename_rule = self.rename_rule.unwrap_or_default();
        let (impl_generics, ty_generics, where_clause) = &self.generics.split_for_impl();
        let fields_val = fields.iter().map(|i| {
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
        let fields = fields.iter();
        let interface = &self.interface;
        let name = &self.name;

        quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #name #ty_generics #where_clause {
                type Err = astarte_device_sdk::event::FromEventError;

                fn from_event(event: astarte_device_sdk::DeviceEvent) -> Result<Self, Self::Err> {
                    use astarte_device_sdk::Value;
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

                    let Value::Object(mut object) = event.data else {
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

    fn quote_indv(&self, variants: &[IndividualMapping]) -> proc_macro2::TokenStream {
        let (impl_generics, ty_generics, where_clause) = &self.generics.split_for_impl();

        let name = &self.name;
        let interface = self.interface.as_str();

        // Use the same order between endpoints and variants, so we can find the correct endpoint
        // position and then match the index with the corresponding variant.
        let endpoints = variants.iter().map(|v| {
            let endpoint = v.attrs.endpoint.as_str();

            quote! {
                Endpoint::<&str>::try_from(#endpoint)?
            }
        });

        let variants = variants.iter().enumerate().map(|(i, v)| {
            let variant = &v.name;

            if v.attrs.allow_unset {
                quote! {
                    #i => {
                        let individual = match event.data {
                            Value::Individual(individual) => individual,
                            Value::Unset => {
                                return Ok(#name::#variant(None));
                            },
                            Value::Object(_) => {
                                return Err(FromEventError::Object {
                                    interface: INTERFACE,
                                    endpoint: event.path,
                                });
                            }
                        };

                        individual.try_into()
                            .map(|value| #name::#variant(Some(value)))
                            .map_err(FromEventError::from)
                    }
                }
            } else {
                quote! {
                    #i => {
                        let individual = match event.data {
                            Value::Individual(individual) => individual,
                            Value::Unset => {
                                return Err(FromEventError::Unset {
                                    interface: INTERFACE,
                                    endpoint: event.path,
                                });
                            },
                            Value::Object(_) => {
                                return Err(FromEventError::Object {
                                    interface: INTERFACE,
                                    endpoint: event.path,
                                });
                            }
                        };

                        individual.try_into().map(#name::#variant).map_err(FromEventError::from)
                    }
                }
            }
        });

        quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #name #ty_generics #where_clause {
                type Err = astarte_device_sdk::event::FromEventError;

                fn from_event(event: astarte_device_sdk::DeviceEvent) -> Result<Self, Self::Err> {
                    use astarte_device_sdk::Value;
                    use astarte_device_sdk::AstarteType;
                    use astarte_device_sdk::event::FromEventError;
                    use astarte_device_sdk::interface::mapping::endpoint::Endpoint;

                    const INTERFACE: &str = #interface;

                    if event.interface != INTERFACE {
                        return Err(FromEventError::Interface(event.interface));
                    }

                    let endpoints = [ #(#endpoints),* ];

                    let position = endpoints.iter()
                        .position(|e| e.eq_mapping(&event.path))
                        .ok_or_else(|| FromEventError::Path {
                            interface: INTERFACE,
                            base_path: event.path.clone(),
                        })?;

                    match position {
                        #(#variants)*
                        _ => unreachable!("BUG: endpoint found, but outside the range of the variants"),
                    }
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
            .into_iter()
            .reduce(|first, other| first.merge(other))
            .ok_or_else(|| {
                syn::Error::new(
                    ast.span(),
                    r#"missing attributes #[from_event(interface = "..", ...)]"#,
                )
            })?;

        let interface = attrs.interface.ok_or_else(|| {
            syn::Error::new(
                ast.span(),
                r#"missing interface attribute #[from_event(interface = "..")]"#,
            )
        })?;

        let aggregation = match attrs.aggregation.unwrap_or_default() {
            Aggregation::Individual => {
                let variants = FromEventAggregation::parse_enum_variants(&ast)?;

                FromEventAggregation::Individual { variants }
            }
            Aggregation::Object => {
                let path = attrs.path.ok_or_else(|| {
                    syn::Error::new(
                        ast.span(),
                        r#"missing path attribute #[from_event(path = "..")]"#,
                    )
                })?;

                let fields = parse_struct_fields(&ast)?;

                FromEventAggregation::Object { fields, path }
            }
        };

        let generics = Self::add_trait_bound(ast.generics);

        Ok(Self {
            interface,
            rename_rule: attrs.rename_rule,
            name: ast.ident,
            generics,
            aggregation,
        })
    }
}

enum FromEventAggregation {
    Individual { variants: Vec<IndividualMapping> },
    Object { fields: Vec<Ident>, path: String },
}

impl FromEventAggregation {
    /// Parses the variants of the enum
    fn parse_enum_variants(ast: &syn::DeriveInput) -> Result<Vec<IndividualMapping>, syn::Error> {
        let syn::Data::Enum(data) = &ast.data else {
            return Err(syn::Error::new(ast.span(), "an enum is required"));
        };

        data.variants
            .iter()
            .map(IndividualMapping::try_from)
            .collect()
    }
}

/// Enum variant for an individual interface to derive FromEvent.
#[derive(Debug)]
struct IndividualMapping {
    name: Ident,
    attrs: MappingAttr,
}

impl TryFrom<&Variant> for IndividualMapping {
    type Error = syn::Error;

    fn try_from(value: &Variant) -> Result<Self, Self::Error> {
        // NOTE: we could also allow single named fields.
        match &value.fields {
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {}
            _ => {
                return Err(syn::Error::new(
                    value.span(),
                    "the variant must have a single unnamed field",
                ));
            }
        }

        let name = value.ident.clone();

        let attrs = value
            .attrs
            .iter()
            .filter_map(|attr| parse_attribute_list::<MappingAttr>(attr, "mapping"))
            .last()
            .ok_or(syn::Error::new(
                value.span(),
                r#"missing `#[mapping(endpoint = "...")] attribute for variant "#,
            ))??;

        Ok(Self { name, attrs })
    }
}

/// Attributes for the individual event.
///
/// ```no_compile
/// #[derive(FromEvent)]
/// enum Individual {
///     #[mapping(endpoint = "/sensor")]
///     Sensor(i32),
///     #[mapping(endpoint = "/temp", allow_unset = true)]
///     Temperature(Option<f64>),
/// }
/// ```
#[derive(Debug)]
struct MappingAttr {
    /// Endpoint for the enum variant
    endpoint: String,
    /// Allow [`Option`]al values for properties.
    ///
    /// Defaults to false as in the interfaces definition.
    allow_unset: bool,
}

impl Parse for MappingAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut attrs = parse_name_value_attrs(input)?;

        let endpoint = attrs
            .remove("endpoint")
            .ok_or(syn::Error::new(
                input.span(),
                r#"missing endpoint attribute `#[mapping(endpoint = "/path")]`"#,
            ))
            .and_then(|expr| parse_str_lit(&expr))?;

        let allow_unset = attrs
            .get("allow_unset")
            .map(parse_bool_lit)
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            endpoint,
            allow_unset,
        })
    }
}

#[cfg(test)]
mod tests {}
