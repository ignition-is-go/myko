use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

/// Generates a saga with registration for runtime discovery.
///
/// # Usage
///
/// ```ignore
/// #[myko_saga]
/// pub struct CleanupSaga;
///
/// impl myko_rs::saga::Saga for CleanupSaga {
///     type State = ();
///
///     fn name() -> &'static str {
///         "CleanupSaga"
///     }
///
///     fn build(
///         events: myko_rs::saga::EventStream,
///         ctx: std::sync::Arc<myko_rs::saga::SagaContext>,
///     ) -> myko_rs::saga::CommandStream {
///         use myko_rs::saga::SagaStreamExt;
///         use futures::StreamExt;
///
///         Box::pin(events
///             .of_item_type("LogEntry")
///             .of_change_type(myko_rs::event::MEventType::SET)
///             .filter_map(|event| async move {
///                 // Saga logic here
///                 None
///             }))
///     }
/// }
/// ```
pub fn myko_saga_impl(input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let struct_name_str = struct_name.to_string();

    let expanded = quote! {
        #input_struct

        // Saga registration for runtime discovery
        myko_rs::submit! {
            myko_rs::saga::SagaRegistration {
                saga_id: #struct_name_str,
                create: || std::sync::Arc::new(#struct_name) as std::sync::Arc<dyn myko_rs::saga::AnySaga>,
            }
        }
    };

    expanded
}
