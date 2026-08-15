use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ImplItem, ItemImpl, ItemStruct, Path, Token, Type, punctuated::Punctuated};

use crate::myko_path;

pub fn category(input: &ItemStruct) -> TokenStream {
    let krate = myko_path();
    let name = &input.ident;
    quote! {
        #input

        impl #krate::graph::EntityCategory for #name {
            const ID: &'static str = concat!(module_path!(), "::", stringify!(#name));
            const NAME: &'static str = stringify!(#name);
        }

        #krate::submit! {
            #krate::graph::EntityCategoryRegistration {
                id: <#name as #krate::graph::EntityCategory>::ID,
                name: <#name as #krate::graph::EntityCategory>::NAME,
                crate_path: module_path!(),
            }
        }
    }
}

pub fn category_membership(
    categories: &Punctuated<Path, Token![,]>,
    input: &ItemStruct,
) -> TokenStream {
    let krate = myko_path();
    let name = &input.ident;
    let implementations = categories.iter().map(|category| {
        quote! {
            impl #krate::graph::InCategory<#category> for #name {}
            #krate::submit! {
                #krate::graph::ItemCategoryRegistration {
                    item_type: <#name as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                    entity_category_id: <#category as #krate::graph::EntityCategory>::ID,
                    crate_path: module_path!(),
                }
            }
        }
    });
    quote! {
        #input
        #(#implementations)*
    }
}

pub fn edge(mut input: ItemImpl) -> TokenStream {
    let krate = myko_path();
    let edge_type = (*input.self_ty).clone();
    let edge_name = match &edge_type {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.clone()),
        _ => None,
    };
    let has_scope = input
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(associated) if associated.ident == "Scope"));
    let has_validator = input
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(associated) if associated.ident == "Validator"));

    if !has_scope {
        input
            .items
            .push(syn::parse_quote!(type Scope = #krate::graph::NoScope;));
    }
    if !has_validator {
        input
            .items
            .push(syn::parse_quote!(type Validator = #krate::graph::NoEdgeValidator;));
    }

    let validator = if has_validator {
        quote!(Some(#krate::graph::validate_edge::<#edge_type>))
    } else {
        quote!(None)
    };

    let address_aliases = edge_name.map_or_else(TokenStream::new, |edge_name| {
        let a_address = format_ident!("{}AAddress", edge_name);
        let b_address = format_ident!("{}BAddress", edge_name);
        quote! {
            pub type #a_address =
                <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::A as #krate::graph::EndpointSpec>::Value;
            pub type #b_address =
                <<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::TypedEdgeEnds>::B as #krate::graph::EndpointSpec>::Value;
        }
    });

    quote! {
        #input

        #address_aliases

        #krate::submit! {
            #krate::graph::EdgeRegistration {
                edge_type: <#edge_type as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                crate_path: module_path!(),
                shape: <<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::SHAPE,
                pair_policy: <#edge_type as #krate::graph::GraphEdge>::PAIR_POLICY,
                pair_projection: <#edge_type as #krate::graph::GraphEdge>::PAIR_PROJECTION,
                endpoints: &<<#edge_type as #krate::graph::GraphEdge>::Ends as #krate::graph::EdgeEnds>::ENDPOINTS,
                scope_type: <<#edge_type as #krate::graph::GraphEdge>::Scope as #krate::graph::EdgeScope>::scope_type,
                adjacency: <#edge_type as #krate::graph::GraphEdge>::ADJACENCY,
                self_loops: <#edge_type as #krate::graph::GraphEdge>::SELF_LOOPS,
                a_delete: <#edge_type as #krate::graph::GraphEdge>::A_DELETE,
                b_delete: <#edge_type as #krate::graph::GraphEdge>::B_DELETE,
                extract: #krate::graph::extract_edge::<#edge_type>,
                extract_scope: #krate::graph::extract_edge_scope::<#edge_type>,
                validate: #validator,
            }
        }
    }
}
