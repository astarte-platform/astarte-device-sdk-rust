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

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;
use syn::Attribute;

use case::RenameRule;

#[proc_macro_attribute]
pub fn astarte_aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast_attr = parse_macro_input!(attr as syn::AttributeArgs);
    let ast_item = parse_macro_input!(item as syn::ItemStruct);

    impl_astarte_aggregate_attr(ast_attr, ast_item)
}

fn impl_astarte_aggregate_attr(
    _ast_attr: syn::AttributeArgs,
    ast_item: syn::ItemStruct,
) -> TokenStream {
    // Do not perform any operation. All the checks and changes are performed by the derive
    // macro.

    TokenStream::from(quote!(#ast_item))
}

#[proc_macro_derive(AstarteAggregate)]
pub fn astarte_aggregate_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = parse_macro_input!(input as syn::DeriveInput);

    // Build the trait implementation
    impl_astarte_aggregate_derive(ast)
}

fn impl_astarte_aggregate_derive(ast: syn::DeriveInput) -> TokenStream {
    let mut rename_rule = RenameRule::None;
    if let Some(astarte_aggregate_attr) =
        find_astarte_aggregate_in_attributes_list(&ast.attrs).unwrap()
    {
        rename_rule = parse_astarte_aggregate_attribute(astarte_aggregate_attr).unwrap();
    }

    if let syn::Data::Struct(st) = ast.data {
        if let syn::Fields::Named(fields) = st.fields {
            let mut fields_names_renamed = Vec::new();
            let mut fields_names_ident = Vec::new();
            for field in fields.named {
                let ident = field
                    .ident
                    .expect("AstarteAggregate is not implementable over this struct");
                fields_names_renamed.push(rename_rule.apply_to_field(&ident.to_string()));
                fields_names_ident.push(ident);
            }
            let name = &ast.ident;
            let fields_names_renamed_iter = fields_names_renamed.iter();
            let fields_names_ident_iter = fields_names_ident.iter();
            let gen = quote! {
                impl AstarteAggregate for #name {
                    fn astarte_aggregate(
                        self,
                    ) -> Result<
                        std::collections::HashMap<String, astarte_device_sdk::types::AstarteType>,
                        astarte_device_sdk::error::AstarteError,
                    > {
                        let mut result = std::collections::HashMap::new();
                        #(
                            let astype: astarte_device_sdk::types::AstarteType =
                                std::convert::TryInto::try_into(self.#fields_names_ident_iter)?;
                            result.insert(#fields_names_renamed_iter.to_string(), astype);
                        )*
                        Ok(result)
                    }
                }
            };
            gen.into()
        } else {
            panic!("AstarteAggregate is only implementable over a named struct.")
        }
    } else {
        panic!("AstarteAggregate is only implementable over a struct.")
    }
}

fn find_astarte_aggregate_in_attributes_list(
    attrs: &[Attribute],
) -> Result<Option<&Attribute>, String> {
    let astarte_aggregate_list = attrs
        .iter()
        .filter(|e| {
            if let Some(segment) = e.path.segments.last() {
                if segment.ident == "astarte_aggregate" {
                    return true;
                }
            }
            false
        })
        .collect::<Vec<_>>();

    if let [astarte_aggregate_attr] = astarte_aggregate_list[..] {
        Ok(Some(astarte_aggregate_attr))
    } else if astarte_aggregate_list.is_empty() {
        Ok(None)
    } else {
        Err("Duplicated astarte_aggregate attribute.".to_string())
    }
}

fn parse_astarte_aggregate_attribute(attr: &Attribute) -> Result<RenameRule, String> {
    if let Ok(syn::Meta::List(meta_list)) = &attr.parse_meta() {
        let meta_list_nested = &meta_list.nested;
        if meta_list_nested.len() == 1 {
            if let Some(syn::NestedMeta::Meta(syn::Meta::NameValue(meta_name_value))) =
                meta_list_nested.first()
            {
                if let syn::Lit::Str(lit_str) = &meta_name_value.lit {
                    return RenameRule::from_str(&lit_str.value())
                        .map_err(|_| format!("Unrecognize syntax rule {}", lit_str.value()));
                }
            }
        }
    }
    Err("Incorrectly formatted astarte_aggregate attribute.".to_string())
}
