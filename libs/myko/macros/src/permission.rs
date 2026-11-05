use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    ItemStruct, Path, Token,
    parse::{Parse, ParseStream},
};

pub struct ScopeSpec {
    pub entity: Path,
    pub action: syn::Ident,
}

impl Parse for ScopeSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let entity = input.parse()?;
        input.parse::<Token![,]>()?;
        let action: syn::Ident = input.parse()?;
        let action = match action.to_string().as_str() {
            "read" => syn::Ident::new("Read", action.span()),
            "write" => syn::Ident::new("Write", action.span()),
            "delete" => syn::Ident::new("Delete", action.span()),
            _ => {
                return Err(syn::Error::new(
                    action.span(),
                    "expected read, write, or delete",
                ));
            }
        };
        if !input.is_empty() {
            return Err(input.error("expected #[myko_scope(Entity, action)]"));
        }
        Ok(Self { entity, action })
    }
}

pub fn take_scopes(input: &mut ItemStruct) -> syn::Result<Vec<ScopeSpec>> {
    let mut scopes = Vec::new();
    let mut attrs = Vec::new();
    for attr in std::mem::take(&mut input.attrs) {
        if attr
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "myko_scope")
        {
            scopes.push(attr.parse_args()?);
        } else {
            attrs.push(attr);
        }
    }
    input.attrs = attrs;
    Ok(scopes)
}

pub fn registration(
    kind: &syn::Ident,
    operation: &syn::Ident,
    scopes: &[ScopeSpec],
    krate: &Path,
) -> TokenStream {
    if scopes.is_empty() {
        return TokenStream::new();
    }
    let requirements = scopes.iter().map(|scope| {
        let entity = &scope.entity;
        let action = &scope.action;
        quote! {
            #krate::auth::EntityScope {
                entity_type: <#entity as #krate::item::Eventable>::ENTITY_NAME_STATIC,
                action: #krate::auth::EntityAction::#action,
            }
        }
    });
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::auth::OperationScopeRegistration {
                kind: #krate::auth::OperationKind::#kind,
                operation: stringify!(#operation),
                scopes: &[#(#requirements),*],
            }
        }
    }
}

pub fn read_registration(
    kind: &syn::Ident,
    operation: &syn::Ident,
    entity: &Path,
    krate: &Path,
) -> TokenStream {
    registration(
        kind,
        operation,
        &[ScopeSpec {
            entity: entity.clone(),
            action: syn::parse_quote!(Read),
        }],
        krate,
    )
}

pub fn entity_metadata(
    entity: &syn::Ident,
    relationships: &crate::relationship::RelationshipInfo,
    reports: &[syn::Ident],
    krate: &Path,
) -> TokenStream {
    let entity_type = entity.to_string();
    let report_scopes = reports.iter().map(|operation| {
        read_registration(
            &syn::parse_quote!(Report),
            operation,
            &syn::parse_quote!(#entity),
            krate,
        )
    });
    let inheritance = relationships.belongs_to.iter().map(|relation| {
        let parent = &relation.foreign_type;
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            #krate::submit! { #krate::auth::ScopeInheritanceRegistration { parent_entity: #parent, child_entity: #entity_type } }
        }
    }).chain(relationships.owns_many.iter().map(|relation| {
        let child = &relation.foreign_type;
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            #krate::submit! { #krate::auth::ScopeInheritanceRegistration { parent_entity: #entity_type, child_entity: #child } }
        }
    }));
    quote! { #(#report_scopes)* #(#inheritance)* }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_attribute_parses_and_is_removed_from_command() {
        let mut command: ItemStruct = syn::parse_quote! {
            #[derive(Debug)]
            #[myko_scope(project::Project, write)]
            #[myko_scope(Camera, delete)]
            struct Publish { id: String }
        };
        let parsed = take_scopes(&mut command);
        assert!(parsed.is_ok());
        let Ok(scopes) = parsed else {
            return;
        };
        assert_eq!(scopes.len(), 2);
        assert_eq!(
            scopes.first().map(|scope| scope.action.to_string()),
            Some("Write".into())
        );
        assert_eq!(command.attrs.len(), 1);
        let output = registration(
            &syn::parse_quote!(Command),
            &command.ident,
            &scopes,
            &syn::parse_quote!(myko),
        );
        let parsed = syn::parse2::<syn::File>(output);
        assert!(parsed.is_ok());
        let Ok(file) = parsed else {
            return;
        };
        assert_eq!(file.items.len(), 1);
    }

    #[test]
    fn malformed_scope_attributes_are_rejected() {
        for input in [
            "Project, execute",
            "\"Project\", read",
            "Project",
            "Project, read, write",
        ] {
            assert!(
                syn::parse_str::<ScopeSpec>(input).is_err(),
                "accepted {input}"
            );
        }
    }

    fn registrations(output: TokenStream) -> Vec<syn::ExprStruct> {
        let parsed = syn::parse2::<syn::File>(output);
        assert!(parsed.is_ok());
        let Ok(file) = parsed else {
            return Vec::new();
        };
        file.items
            .into_iter()
            .filter_map(|item| {
                let syn::Item::Macro(item) = item else {
                    return None;
                };
                syn::parse2::<syn::ExprStruct>(item.mac.tokens).ok()
            })
            .collect()
    }

    #[test]
    fn explicit_command_scopes_form_one_registration() {
        let output = crate::command::myko_command_impl(
            crate::command::CommandOptions {
                result_type: None,
                custom_serialize: false,
            },
            syn::parse_quote! {
                #[myko_scope(Project, read)]
                #[myko_scope(Camera, write)]
                struct PublishCamera { id: String }
            },
        );
        let scopes: Vec<_> = registrations(output)
            .into_iter()
            .filter(|entry| {
                entry
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "OperationScopeRegistration")
            })
            .collect();
        assert_eq!(scopes.len(), 1);
        let count = scopes.first().and_then(|registration| {
            registration.fields.iter().find_map(|field| {
                if field.member != syn::Member::Named(syn::parse_quote!(scopes)) {
                    return None;
                }
                let syn::Expr::Reference(reference) = &field.expr else {
                    return None;
                };
                let syn::Expr::Array(array) = reference.expr.as_ref() else {
                    return None;
                };
                Some(array.elems.len())
            })
        });
        assert_eq!(count, Some(2));
    }

    #[test]
    fn malformed_command_scope_is_a_compile_error() {
        let output = crate::command::myko_command_impl(
            crate::command::CommandOptions {
                result_type: None,
                custom_serialize: false,
            },
            syn::parse_quote! {
                #[myko_scope(Camera, admin)]
                struct PublishCamera { id: String }
            },
        );
        let parsed = syn::parse2::<syn::ItemMacro>(output);
        assert!(parsed.is_ok());
        assert_eq!(
            parsed.ok().and_then(|item| item
                .mac
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())),
            Some("compile_error".into())
        );
    }

    #[test]
    fn unannotated_custom_command_does_not_invent_a_scope() {
        let output = crate::command::myko_command_impl(
            crate::command::CommandOptions {
                result_type: None,
                custom_serialize: false,
            },
            syn::parse_quote! { struct PublishCamera { id: String } },
        );
        assert!(!registrations(output).iter().any(|entry| {
            entry
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "OperationScopeRegistration")
        }));
    }
    #[test]
    fn query_scope_attributes_cannot_replace_output_read_permission() {
        let output = crate::query::myko_query_impl(
            &syn::parse_quote!(Camera),
            true,
            syn::parse_quote! {
                #[myko_scope(Project, write)]
                struct ProjectCameras { project_id: String }
            },
        );
        let scopes: Vec<_> = registrations(output)
            .into_iter()
            .filter(|entry| {
                entry
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "OperationScopeRegistration")
            })
            .collect();
        assert_eq!(scopes.len(), 1);
        let count = scopes.first().and_then(|registration| {
            registration.fields.iter().find_map(|field| {
                if field.member != syn::Member::Named(syn::parse_quote!(scopes)) {
                    return None;
                }
                let syn::Expr::Reference(reference) = &field.expr else {
                    return None;
                };
                let syn::Expr::Array(array) = reference.expr.as_ref() else {
                    return None;
                };
                Some(array.elems.len())
            })
        });
        assert_eq!(count, Some(2));
    }
}
