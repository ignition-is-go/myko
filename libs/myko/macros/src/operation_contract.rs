use proc_macro2::TokenStream;
use quote::quote;
use serde_derive_internals::{Ctxt, Derive, ast};

pub fn serde_rename_attr(ctx: &crate::DeriveCtx, attrs: &[syn::Attribute]) -> TokenStream {
    if crate::attrs_have_serde_rename_all(attrs) {
        ctx.serde_attr(&quote!())
    } else {
        ctx.serde_attr(&quote!(rename_all = "camelCase"))
    }
}

pub fn field_metadata_tokens(input: &syn::ItemStruct, krate: &syn::Path) -> TokenStream {
    let input = normalized_definition(input);
    let context = Ctxt::new();
    let container = ast::Container::from_ast(&context, &input, Derive::Deserialize);
    if let Err(error) = context.check() {
        return error.into_compile_error();
    }
    let Some(container) = container else {
        return quote!(&[]);
    };
    if let Some(ty) = container
        .attrs
        .type_from()
        .or_else(|| container.attrs.type_try_from())
    {
        let rust_type = quote!(#ty).to_string();
        return quote!(&[#krate::reflection::OperationArgField {
            name: "", rust_type: #rust_type, presence: #krate::reflection::OperationArgPresence::Required,
            aliases: &[], description: Some("Opaque input converted by Serde"),
            flattened: true, custom_deserializer: true,
            input_schema: || #krate::serde_json::json!({"x-myko-opaque": true}),
        }]);
    }
    let ast::Data::Struct(_, fields) = container.data else {
        return quote!(&[]);
    };
    let entries = fields
        .iter()
        .filter(|field| !field.attrs.skip_deserializing())
        .map(|field| {
            let name = field.attrs.name().deserialize_name();
            let aliases: Vec<_> = field
                .attrs
                .aliases()
                .iter()
                .filter(|alias| alias.as_str() != name)
                .collect();
            let ty = field.ty;
            let rust_type = quote!(#ty).to_string();
            let defaulted =
                !field.attrs.default().is_none() || !container.attrs.default().is_none();
            let optional = defaulted
                || (crate::is_option_type(ty) && field.attrs.deserialize_with().is_none());
            let flattened = field.attrs.flatten() || container.attrs.transparent();
            let custom_deserializer =
                field.attrs.deserialize_with().is_some() || container.attrs.transparent();
            let presence = if defaulted {
                quote!(#krate::reflection::OperationArgPresence::Defaulted)
            } else if optional {
                quote!(#krate::reflection::OperationArgPresence::Optional)
            } else {
                quote!(#krate::reflection::OperationArgPresence::Required)
            };
            let input_schema = if custom_deserializer || flattened {
                quote!(|| #krate::serde_json::json!({"x-myko-opaque": true}))
            } else {
                quote!(|| {
                    use #krate::reflection::ReflectInputSchema as _;
                    (&&#krate::reflection::InputSchema::<#ty>::default()).input_schema()
                })
            };
            let description = crate::extract_doc_comment(&field.original.attrs);
            let description = description
                .as_ref()
                .map_or_else(|| quote!(None), |text| quote!(Some(#text)));
            let conditions = field
                .original
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"));
            quote! {
                #(#conditions)*
                #krate::reflection::OperationArgField {
                    name: #name,
                    rust_type: #rust_type,
                    presence: #presence,
                    aliases: &[#(#aliases),*],
                    description: #description,
                    flattened: #flattened,
                    custom_deserializer: #custom_deserializer,
                    input_schema: #input_schema,
                }
            }
        });
    quote!(&[#(#entries),*])
}

pub fn entity_description_tokens(input: &syn::ItemStruct, krate: &syn::Path) -> TokenStream {
    let name = &input.ident;
    let description = crate::extract_doc_comment(&input.attrs);
    let description = description
        .as_ref()
        .map_or_else(|| quote!(None), |value| quote!(Some(#value)));
    let definition = normalized_definition(input);
    let context = Ctxt::new();
    let container = ast::Container::from_ast(&context, &definition, Derive::Serialize);
    if let Err(error) = context.check() {
        return error.into_compile_error();
    }
    let Some(container) = container else {
        return quote!();
    };
    let ast::Data::Struct(_, fields) = container.data else {
        return quote!();
    };
    let fields = fields.iter().filter(|field| !field.attrs.skip_serializing()).map(|field| {
        let name = field.attrs.name().serialize_name();
        let ty = field.ty;
        let rust_type = quote!(#ty).to_string();
        let flattened = field.attrs.flatten();
        let description = crate::extract_doc_comment(&field.original.attrs);
        let description = description.as_ref().map_or_else(|| quote!(None), |value| quote!(Some(#value)));
        let conditions = field.original.attrs.iter().filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"));
        quote! { #(#conditions)* #krate::reflection::EntityField {
            name: #name, rust_type: #rust_type, description: #description, flattened: #flattened,
        } }
    });
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! { #krate::reflection::EntityDescription {
            entity_type: stringify!(#name), description: #description, fields: &[#(#fields),*],
        } }
    }
}

fn normalized_definition(input: &syn::ItemStruct) -> syn::DeriveInput {
    let mut input = input.clone();
    if !crate::attrs_have_serde_rename_all(&input.attrs) {
        input
            .attrs
            .push(syn::parse_quote!(#[serde(rename_all = "camelCase")]));
    }
    syn::parse_quote!(#input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_metadata_uses_serialized_names_and_skips_private_fields() {
        let input = syn::parse_quote! {
            /// Record for a downstream domain.
            struct Record {
                /// Public display name.
                #[serde(rename(serialize = "displayName", deserialize = "inputName"))]
                name: String,
                #[serde(skip_serializing)]
                secret: String,
            }
        };
        let output = entity_description_tokens(&input, &syn::parse_quote!(myko)).to_string();
        assert!(output.contains("displayName"));
        assert!(output.contains("Public display name."));
        assert!(output.contains("Record for a downstream domain."));
        assert!(!output.contains("inputName"));
        assert!(!output.contains("secret"));
    }

    #[test]
    fn operation_contract_uses_deserialization_names_and_presence() {
        let input = syn::parse_quote! {
            struct Example {
                scene_id: String,
                #[serde(rename(serialize = "storedValue", deserialize = "wireValue"), alias = "legacy")]
                input_value: String,
                #[serde(default)]
                count: u32,
                maybe_value: Option<String>,
                #[serde(skip_deserializing)]
                ignored: String,
                #[serde(flatten)]
                extra: Extra,
            }
        };
        let output = field_metadata_tokens(&input, &syn::parse_quote!(myko)).to_string();
        assert!(output.contains("name : \"sceneId\""));
        assert!(output.contains("name : \"wireValue\""));
        assert!(output.contains("legacy"));
        assert!(!output.contains("storedValue"));
        assert!(!output.contains("ignored"));
        assert!(output.contains("OperationArgPresence :: Defaulted"));
        assert!(output.contains("flattened : true"));
        assert!(output.contains("OperationArgPresence :: Optional"));
    }

    #[test]
    fn operation_contract_preserves_container_defaults_and_custom_option_parsing() {
        let input = syn::parse_quote! {
            #[serde(default)]
            struct Example { scene_id: String }
        };
        let output = field_metadata_tokens(&input, &syn::parse_quote!(myko)).to_string();
        assert!(output.contains("OperationArgPresence :: Defaulted"));
        let input = syn::parse_quote! {
            struct Example {
                #[serde(deserialize_with = "parse_required_option")]
                value: Option<String>
            }
        };
        let output = field_metadata_tokens(&input, &syn::parse_quote!(myko)).to_string();
        assert!(output.contains("OperationArgPresence :: Required"));
    }
    #[test]
    fn operation_contract_marks_converted_input_opaque() {
        let input = syn::parse_quote! {
            #[serde(from = "WireInput")]
            struct Example { internal_count: u32 }
        };
        let output = field_metadata_tokens(&input, &syn::parse_quote!(myko)).to_string();
        assert!(!output.contains("internal_count"));
        assert!(output.contains("flattened : true"));
        assert!(output.contains("custom_deserializer : true"));
    }
}
