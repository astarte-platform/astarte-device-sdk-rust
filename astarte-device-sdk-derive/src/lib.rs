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

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

#[proc_macro_derive(AstarteAggregate)]
pub fn astarte_aggregate_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = parse_macro_input!(input as syn::DeriveInput);

    // Build the trait implementation
    impl_astarte_aggregate(ast)
}

fn impl_astarte_aggregate(ast: syn::DeriveInput) -> TokenStream {
    if let syn::Data::Struct(st) = ast.data {
        if let syn::Fields::Named(fields) = st.fields {
            let mut fields_names = (Vec::new(), Vec::new());
            for field in fields.named {
                let ident = field
                    .ident
                    .expect("AstarteAggregate is not implementable over this struct");
                fields_names.0.push(ident.to_string());
                fields_names.1.push(ident);
            }
            let name = &ast.ident;
            let fields_names_str_iter = fields_names.0.iter();
            let fields_names_iter = fields_names.1.iter();
            let gen = quote! {
                impl AstarteAggregate for #name {
                    fn astarte_aggregate(
                        self,
                    ) -> Result<
                        std::collections::HashMap<String, astarte_device_sdk::types::AstarteType>,
                        astarte_device_sdk::AstarteError,
                    > {
                        let mut result = std::collections::HashMap::new();
                        #(
                            let astype: astarte_device_sdk::types::AstarteType =
                                std::convert::TryInto::try_into(self.#fields_names_iter)?;
                            result.insert(#fields_names_str_iter.to_string(), astype);
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
