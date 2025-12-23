use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

mod command;
mod item;
mod message_events;
mod partial_matches;
mod query;
mod relationship;
mod report;
mod saga;
mod setter;

#[proc_macro_derive(PartialMatches)]
pub fn derive_partial_matches(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    partial_matches::derive_partial_matches_impl(input).into()
}

/// Implements a number of traits automatically, as well as adds
///
/// `pub id: Arc<str>`
///
/// `pub hash: Arc<str>`
///
/// Derives:
///
/// `Partial, PartialEq, Clone, Serialize, Deserialize, Debug`
///
/// Derives for Partial:
///
/// `Clone, Serialize, Deserialize, Default`
///
/// All fields added manually must implement at least `Clone, Serialize, Deserialize`
#[proc_macro_attribute]
pub fn myko_item(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    item::myko_item_impl(input).into()
}

#[proc_macro_attribute]
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    let query_item_type = parse_macro_input!(attr as syn::Path);
    let input = parse_macro_input!(input as syn::ItemStruct);
    query::myko_query_impl(query_item_type, input).into()
}

/// Generates a reactive report that can depend on queries and other reports.
///
/// # Usage
///
/// ```ignore
/// #[myko_report(Vec<Target>)]
/// pub struct GetParentTargets {
///     pub target_id: String,
///     pub depth: u32,
/// }
///
/// // You must implement the compute method:
/// impl GetParentTargets {
///     pub fn compute(
///         report: std::sync::Arc<Self>,
///         ctx: myko_rs::prelude::ReportContext,
///     ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Vec<Target>> + Send>> {
///         // Use ctx.query() and ctx.report() for reactive dependencies
///         Box::pin(async_stream::stream! {
///             // ... your reactive logic
///         })
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn myko_report(attr: TokenStream, input: TokenStream) -> TokenStream {
    let report_output_type = parse_macro_input!(attr as syn::Path);
    let input = parse_macro_input!(input as syn::ItemStruct);
    report::myko_report_impl(report_output_type, input).into()
}

/// Generates a command with handler struct and registration.
///
/// # Usage
///
/// ```ignore
/// // With return type:
/// #[myko_command(CreateMachineResult)]
/// pub struct CreateMachine {
///     pub name: String,
/// }
///
/// // Without return type (returns ()):
/// #[myko_command]
/// pub struct DeleteMachine {
///     pub machine_id: String,
/// }
///
/// // User must implement the handler execute method:
/// impl CreateMachineHandler {
///     async fn execute(
///         cmd: CreateMachine,
///         ctx: CommandContext,
///     ) -> Result<CreateMachineResult, CommandError> {
///         // Handler logic
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn myko_command(attr: TokenStream, input: TokenStream) -> TokenStream {
    let result_type = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as syn::Path))
    };
    let input = parse_macro_input!(input as syn::ItemStruct);
    command::myko_command_impl(result_type, input).into()
}

/// Derive macro that extracts serde rename values from enum variants
/// and generates MessageEventRegistration inventory submissions.
///
/// # Usage
/// ```ignore
/// #[derive(MessageEvents)]
/// #[serde(tag = "event", content = "data")]
/// pub enum MykoMessage<Commands> {
///     #[serde(rename = "ws:m:query")]
///     Query(WrappedQuery),
///     // ...
/// }
/// ```
#[proc_macro_derive(MessageEvents)]
pub fn derive_message_events(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    message_events::derive_message_events_impl(input).into()
}

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
#[proc_macro_attribute]
pub fn myko_saga(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    saga::myko_saga_impl(input).into()
}

/// Adds standard derives and registers for TypeScript export.
///
/// Use this for report output types to reduce boilerplate.
///
/// # Usage
///
/// ```ignore
/// #[myko_report_output]
/// pub struct ServerStatsOutput {
///     pub server: Option<Server>,
///     pub client_count: usize,
/// }
///
/// // Expands to:
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, myko_rs::TS)]
/// #[serde(rename_all = "camelCase")]
/// pub struct ServerStatsOutput { ... }
/// myko_rs::register_ts_export!(ServerStatsOutput);
/// ```
#[proc_macro_attribute]
pub fn myko_report_output(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    let name = &input.ident;

    // ToValue is implemented via blanket impl for all Serialize types
    let expanded = quote! {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, myko_rs::TS)]
        #[serde(rename_all = "camelCase")]
        #input

        myko_rs::register_ts_export!(#name);
    };

    expanded.into()
}
