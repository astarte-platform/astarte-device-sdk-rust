// This file is part of Astarte.
//
// Copyright 2024-2026 SECO Mind Srl
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

//! Derives the `FromEvent` trait.

use std::fmt::Debug;

use darling::{FromDeriveInput, FromField, FromMeta, FromVariant};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{GenericParam, Generics, parse_quote};

use crate::case::RenameRule;

/// Attributes for the individual event.
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(
///     interface = "com.example.Sensor",
///     path = "/sensor",
///     interface_type = "properties",
///     aggregation = "individual"
/// )]
/// enum Individual {
///     #[mapping(endpoint = "/sensor")]
///     Sensor(i32),
///     #[mapping(endpoint = "/temp", allow_unset = true)]
///     Temperature(Option<f64>),
/// }
/// ```
///
/// For objects:
///
/// ```no_compile
/// #[derive(FromEvent)]
/// #[from_event(
///     interface = "com.example.Sensor",
///     path = "/%{sensor_id}",
///     interface_type = "datastream",
///     aggregation = "object"
/// )]
/// struct Object {
///     #[mapping(endpoint = "/sensor")]
///     sensor: i32,
///     #[mapping(endpoint = "/temp")]
///     temperature: f64,
/// }
/// ```
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(from_event), supports(struct_named, enum_newtype))]
pub(crate) struct FromEventDerive {
    /// Name of the interface
    interface: String,
    /// Path of the object datastream
    path: Option<String>,
    /// Aggregation
    #[darling(default)]
    aggregation: Aggregation,
    /// Interface type
    #[darling(default)]
    interface_type: InterfaceType,
    /// Rename the fields in the resulting HashMap, see the [`RenameRule`] variants.
    rename_all: Option<RenameRule>,
    /// Name of the struct
    ident: syn::Ident,
    /// Generics bounds
    generics: syn::Generics,
    /// fields
    data: darling::ast::Data<FromEventVariant, FromEventField>,
}

impl FromEventDerive {
    fn add_trait_bound(&self) -> Generics {
        let mut generics = self.generics.clone();

        for param in &mut generics.params {
            if let GenericParam::Type(ref mut type_param) = *param {
                type_param.bounds.push(parse_quote!(
                    std::convert::TryFrom<astarte_device_sdk::types::AstarteData, Error =  >
                ));
            }
        }
        generics
    }

    pub(crate) fn quote(&self) -> darling::Result<proc_macro2::TokenStream> {
        match (self.interface_type, self.aggregation) {
            (InterfaceType::Datastream, Aggregation::Individual) => {
                let Some(data) = self.data.as_ref().take_enum() else {
                    return Err(darling::Error::unsupported_shape_with_expected(
                        "struct",
                        &"individual should be an enum",
                    )
                    .with_span(&self.ident));
                };

                self.quote_indv(&data)
            }
            (InterfaceType::Datastream, Aggregation::Object) => {
                let Some(data) = self.data.as_ref().take_struct() else {
                    return Err(darling::Error::unsupported_shape_with_expected(
                        "enum",
                        &"object should be a structs",
                    )
                    .with_span(&self.ident));
                };

                self.quote_obj(&data.fields)
            }
            (InterfaceType::Properties, Aggregation::Individual) => {
                let Some(data) = self.data.as_ref().take_enum() else {
                    return Err(darling::Error::unsupported_shape_with_expected(
                        "struct",
                        &"individual should be an enum",
                    )
                    .with_span(&self.ident));
                };

                self.quote_property(&data)
            }
            (InterfaceType::Properties, Aggregation::Object) => {
                Err(darling::Error::custom("object properties are unsupported"))
            }
        }
    }

    pub(crate) fn quote_obj(
        &self,
        fields: &[&FromEventField],
    ) -> darling::Result<proc_macro2::TokenStream> {
        let generics = self.add_trait_bound();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let mut errors = darling::Error::accumulator();

        let fields = fields
            .iter()
            .filter_map(|field| {
                errors.handle_in(|| {
                    field.field_name(self.rename_all).ok_or_else(|| {
                        darling::Error::custom("missing field names").with_span(&self.ident)
                    })
                })
            })
            .collect::<Vec<(&syn::Ident, String)>>();

        let fields_val = fields.iter().map(|(i, name)| {
            quote_spanned! {i.span() =>
                let #i = object
                    .remove(#name)
                    .ok_or_else(||{
                        Error::new(FromEventError::Interface(InterfaceError::MappingRequired))
                            .set_ctx(format!("for interface {} endpoint {}{}", interface, base_path, #name))
                    })?
                    .try_into()
                    .map_kind(FromEventError::Conversion)?;
            }
        });
        let fields = fields.iter().map(|(i, _)| i);
        let interface = &self.interface;
        let st_name = &self.ident;

        let path = errors.handle_in(|| {
            self.path
                .as_ref()
                .ok_or_else(|| darling::Error::missing_field("path").with_span(&self.ident))
        });

        errors.finish()?;

        Ok(quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #st_name #ty_generics #where_clause {
                type Err = astarte_device_sdk::astarte_device_error::Error<astarte_device_sdk::event::FromEventError>;

                fn from_event(event: astarte_device_sdk::DeviceEvent) -> ::std::result::Result<Self, Self::Err> {
                    use astarte_device_sdk::Value;
                    use astarte_device_sdk::astarte_device_error::{Error, WrapError, ResultExt};
                    use astarte_device_sdk::error::InterfaceError;
                    use astarte_device_sdk::event::FromEventError;
                    use astarte_device_sdk::astarte_interfaces::MappingPath;
                    use astarte_device_sdk::astarte_interfaces::mapping::endpoint::Endpoint;
                    use astarte_device_sdk::astarte_interfaces::schema::{Aggregation, InterfaceType};

                    let interface = #interface;
                    let base_path = #path;
                    let endpoint: Endpoint<&str> = Endpoint::try_from(base_path)
                        .wrap_err_msg(FromEventError::Interface(InterfaceError::Invalid), "while parsing endpoint")?;

                    if event.interface != interface {
                        return Err(
                            Error::with(FromEventError::Interface(InterfaceError::Invalid), "for event")
                                .set_ctx(format!("expected {} but got {}", interface, event.interface))
                        );
                    }

                    let path = MappingPath::try_from(event.path.as_str())
                        .wrap_err_with(|_| {
                            Error::with(FromEventError::Interface(InterfaceError::Path), "while parsing event path")
                                .set_ctx(format!("for {interface}{}", event.path))
                        })?;

                    if !endpoint.eq_mapping(&path) {
                        return Err(
                            Error::new(FromEventError::Interface(InterfaceError::ObjectPath))
                                .set_ctx(format!("for {interface}{endpoint}"))
                        );
                    }

                    let mut object = match event.data {
                        Value::Object{data, ..} => data,
                        Value::Individual{..} => {
                            return Err(
                                Error::new(FromEventError::Interface(InterfaceError::Aggregation))
                                    .set_ctx(format!("for {interface}"))
                            );
                        },
                        Value::Property(_) => {
                            return Err(Error::new(FromEventError::Interface(InterfaceError::InterfaceType))
                                    .set_ctx(format!("for {interface}"))
                            );
                        },
                    };

                    #(#fields_val)*

                    Ok(Self{#(#fields),*})
                }
            }
        })
    }

    fn quote_indv(
        &self,
        variants: &[&FromEventVariant],
    ) -> darling::Result<proc_macro2::TokenStream> {
        let generics = self.add_trait_bound();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let name = &self.ident;
        let interface = self.interface.as_str();

        // Use the same order between endpoints and variants, so we can find the correct endpoint
        // position and then match the index with the corresponding variant.
        let endpoints = variants.iter().map(|v| {
            let endpoint = v.endpoint.as_str();

            quote! {
                Endpoint::<&str>::try_from(#endpoint)
                    .wrap_err_with(|_| {
                        Error::with(FromEventError::Interface(InterfaceError::Invalid), "endpoint")
                            .set_ctx(format!("for {INTERFACE} with endpoint {}", #endpoint))
                    })?
            }
        });

        let mut errors = darling::Error::accumulator();

        if self.path.is_some() {
            errors.push(
                darling::Error::custom(
                    "the path is only available for `#[from_event(aggregation = \"object\")]`",
                )
                .with_span(&self.ident),
            );
        }

        for variant in variants {
            if variant.allow_unset.is_some() {
                errors.push(darling::Error::custom(
                    r#"the attribute allow_unset is only usable with `#[from_event(interface_type = "property")]` on the container"#,
                ).with_span(&variant.ident));
            }
        }

        errors.finish()?;

        let variants = variants.iter().enumerate().map(|(i, v)| {
            let variant = &v.ident;

            quote! {
                #i => {
                    let individual = match event.data {
                        Value::Individual{data, ..} => data,
                        Value::Object{..} => {
                            return Err(InterfaceError::aggregation(
                                "from event",
                                INTERFACE,
                                event.path,
                                Aggregation::Individual,
                                Aggregation::Object,
                            )).map_kind(FromEventError::Interface);
                        },
                        Value::Property(_) => {
                            return Err(InterfaceError::interface_type(
                                "from event",
                                INTERFACE,
                                event.path,
                                InterfaceType::Datastream,
                                InterfaceType::Properties,
                            )).map_kind(FromEventError::Interface);
                        },
                    };


                    individual.try_into().map(#name::#variant).map_kind(FromEventError::Conversion)
                }
            }
        });

        Ok(quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #name #ty_generics #where_clause {
                type Err = astarte_device_sdk::astarte_device_error::Error<astarte_device_sdk::event::FromEventError>;

                fn from_event(event: astarte_device_sdk::DeviceEvent) -> ::std::result::Result<Self, Self::Err> {
                    use astarte_device_sdk::{AstarteData, Value};
                    use astarte_device_sdk::astarte_device_error::{WrapError, Error, ResultExt};
                    use astarte_device_sdk::error::InterfaceError;
                    use astarte_device_sdk::event::FromEventError;
                    use astarte_device_sdk::astarte_interfaces::mapping::endpoint::Endpoint;
                    use astarte_device_sdk::astarte_interfaces::schema::{Aggregation, InterfaceType};
                    use astarte_device_sdk::astarte_interfaces::MappingPath;

                    const INTERFACE: &str = #interface;

                    if event.interface != INTERFACE {
                        return Err(
                            Error::with(FromEventError::Interface(InterfaceError::Invalid), "wrong interface")
                                .set_ctx(format!("for {INTERFACE}"))
                        );
                    }

                    let endpoints = [ #(#endpoints),* ];

                    let path = MappingPath::try_from(event.path.as_str())
                        .wrap_err_with(|_| {
                            Error::with(FromEventError::Interface(InterfaceError::Path), "from event")
                                .set_ctx(format!("for {INTERFACE}"))
                        })?;

                    let position = endpoints.iter()
                        .position(|e| e.eq_mapping(&path))
                        .ok_or_else(|| {
                            Error::with(FromEventError::Interface(InterfaceError::MappingNotFound), "from event")
                                .set_ctx(format!("for {INTERFACE}{path}"))
                        })?;

                    match position {
                        #(#variants)*
                        _ => unreachable!("BUG: endpoint found, but outside the range of the variants"),
                    }
                }
            }
        })
    }

    fn quote_property(
        &self,
        variants: &[&FromEventVariant],
    ) -> darling::Result<proc_macro2::TokenStream> {
        let generics = self.add_trait_bound();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let name = &self.ident;
        let interface = self.interface.as_str();

        let mut errors = darling::Error::accumulator();

        if self.path.is_some() {
            errors.push(
                darling::Error::custom(
                    "the path is only available for `#[from_event(aggregation = \"object\")]`",
                )
                .with_span(&self.ident),
            );
        }
        // Use the same order between endpoints and variants, so we can find the correct endpoint
        // position and then match the index with the corresponding variant.
        let endpoints = variants.iter().map(|v| {
            let endpoint = v.endpoint.as_str();

            quote! {
                Endpoint::<&str>::try_from(#endpoint)
                    .wrap_err_with(|_| {
                        Error::with(FromEventError::Interface(InterfaceError::Invalid), "from event endpoint")
                            .set_ctx(format!("for {INTERFACE}"))
                    })?
            }
        });

        let variants = variants.iter().enumerate().map(|(i, v)| {
            let variant = &v.ident;

            let prop_set_case = if v.allow_unset.unwrap_or_default() {
                quote! { Some(value) }
            } else {
                quote! { value }
            };

            let prop_unset = if v.allow_unset.unwrap_or_default() {
                quote! { Ok(#name::#variant(None)) }
            } else {
                quote! {
                    return Err(
                        Error::with(FromEventError::Interface(InterfaceError::Unset), "from event")
                        .set_ctx(format!("for {INTERFACE}{}", event.path))
                    );
                }
            };

            quote! {
                #i => {
                    match event.data {
                        Value::Individual{..} | Value::Object{..} => {
                            return Err(InterfaceError::interface_type(
                                "from event data",
                                INTERFACE,
                                event.path,
                                InterfaceType::Properties,
                                InterfaceType::Datastream,
                            )).map_kind(FromEventError::Interface);
                        },
                        Value::Property(Some(prop)) => {
                            prop.try_into()
                                .map(|value| #name::#variant(#prop_set_case))
                                .map_kind(FromEventError::Conversion)
                        },
                        Value::Property(None) => {
                            #prop_unset
                        },
                    }
                }
            }
        });

        errors.finish()?;

        Ok(quote! {
            impl #impl_generics astarte_device_sdk::FromEvent for #name #ty_generics #where_clause {
                type Err = astarte_device_sdk::astarte_device_error::Error<astarte_device_sdk::event::FromEventError>;

                fn from_event(event: astarte_device_sdk::DeviceEvent) -> ::std::result::Result<Self, Self::Err> {
                    use astarte_device_sdk::astarte_device_error::{Error, WrapError, ResultExt};
                    use astarte_device_sdk::{AstarteData, Value};
                    use astarte_device_sdk::error::{InterfaceError};
                    use astarte_device_sdk::event::FromEventError;
                    use astarte_device_sdk::astarte_interfaces::MappingPath;
                    use astarte_device_sdk::astarte_interfaces::mapping::endpoint::Endpoint;
                    use astarte_device_sdk::astarte_interfaces::schema::{Aggregation, InterfaceType};

                    const INTERFACE: &str = #interface;

                    if event.interface != INTERFACE {
                        return Err(Error::with(FromEventError::Interface(InterfaceError::Invalid), "for event")
                            .set_ctx(format!("expected {INTERFACE} but got {}", event.interface)));
                    }

                    let endpoints = [ #(#endpoints),* ];

                    let path = MappingPath::try_from(event.path.as_str())
                        .wrap_err_with(|_| {
                            Error::with(FromEventError::Interface(InterfaceError::Path), "while parsign event path")
                                .set_ctx(event.path.to_string())
                        })?;

                    let position = endpoints.iter()
                        .position(|e| e.eq_mapping(&path))
                        .ok_or_else(|| {
                            Error::with(FromEventError::Interface(InterfaceError::MappingNotFound), "from event")
                                .set_ctx(format!("for {INTERFACE}{path}"))
                        })?;

                    match position {
                        #(#variants)*
                        _ => unreachable!("BUG: endpoint found, but outside the range of the variants"),
                    }
                }
            }
        })
    }
}

/// Attributes for the individual event.
///
/// ```no_compile
/// struct Object {
///     #[mapping(rename = "type")]
///     sensor_type: i32,
///     temperature: f64,
/// }
/// ```
#[derive(Debug, FromField)]
#[darling(attributes(mapping))]
pub(crate) struct FromEventField {
    /// Rename the filed or variant.
    rename: Option<String>,
    /// Field name
    ident: Option<syn::Ident>,
}

impl FromEventField {
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
#[derive(Debug, FromVariant)]
#[darling(attributes(mapping))]
struct FromEventVariant {
    /// Endpoint for the enum variant
    endpoint: String,
    /// Allow [`Option`]al values for properties.
    ///
    /// Defaults to false as in the interfaces definition. Only available with `interface_type = "properties"`
    #[darling(default)]
    allow_unset: Option<bool>,
    /// variant name
    ident: syn::Ident,
}

#[derive(Debug, Clone, Copy, Default, FromMeta)]
#[darling(rename_all = "lowercase")]
enum Aggregation {
    #[default]
    Individual,
    Object,
}

#[derive(Debug, Clone, Copy, Default, FromMeta)]
#[darling(rename_all = "lowercase")]
enum InterfaceType {
    #[default]
    Datastream,
    Properties,
}
