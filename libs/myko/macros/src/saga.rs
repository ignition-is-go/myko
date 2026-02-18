use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

/// Generates a saga registration entry and a typed Saga impl.
///
/// `#[myko_saga]` on a struct generates a `Saga` implementation that delegates
/// business logic to `impl SagaHandler for Struct`.
pub fn myko_saga_impl(attr: TokenStream, input_struct: ItemStruct) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[myko_saga] no longer accepts arguments; use SagaHandler associated types/consts",
        )
        .to_compile_error();
    }

    let struct_name = &input_struct.ident;
    let struct_name_str = struct_name.to_string();
    let krate = crate::myko_rs_path();

    quote! {
        #input_struct

        impl #krate::saga::Saga for #struct_name {
            type State = ();
            type Command = <#struct_name as #krate::saga::SagaHandler>::Command;

            fn name() -> &'static str {
                #struct_name_str
            }

            fn build(
                events: #krate::saga::EventStream,
                ctx: std::sync::Arc<#krate::saga::SagaContext>,
            ) -> #krate::futures::stream::BoxStream<'static, Self::Command> {
                use #krate::futures::StreamExt as _;
                use #krate::saga::SagaStreamExt as _;

                Box::pin(
                    events
                        .of_item_type(<<#struct_name as #krate::saga::SagaHandler>::EventItem as #krate::item::Eventable>::entity_name_static())
                        .of_change_type(<#struct_name as #krate::saga::SagaHandler>::EVENT_TYPE)
                        .filter_map(move |event| {
                            let ctx = ctx.clone();
                            async move {
                                log::debug!(
                                    "{} matched event item_type={} change_type={:?} tx={}",
                                    #struct_name_str,
                                    event.item_type,
                                    event.change_type,
                                    event.tx
                                );
                                let item = match #krate::serde_json::from_value::<<#struct_name as #krate::saga::SagaHandler>::EventItem>(event.item.clone()) {
                                    Ok(item) => item,
                                    Err(err) => {
                                        log::warn!(
                                            "{}: failed to deserialize {} event item: {}",
                                            #struct_name_str,
                                            <<#struct_name as #krate::saga::SagaHandler>::EventItem as #krate::item::Eventable>::entity_name_static(),
                                            err
                                        );
                                        return None;
                                    }
                                };

                                let out = <#struct_name as #krate::saga::SagaHandler>::handle(item, event, ctx);
                                if out.is_some() {
                                    log::debug!("{} emitted command", #struct_name_str);
                                } else {
                                    log::debug!("{} emitted no command", #struct_name_str);
                                }
                                out
                            }
                        }),
                )
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #krate::saga::SagaRegistration {
                saga_id: #struct_name_str,
                create: || std::sync::Arc::new(#struct_name) as std::sync::Arc<dyn #krate::saga::AnySaga>,
            }
        }
    }
}
