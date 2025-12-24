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

/// Marks a struct as a Myko entity, generating queries, reports, commands, and supporting types.
///
/// # Struct Modifications
///
/// Adds two required fields automatically:
/// - `pub id: Arc<str>` - Unique identifier for the entity
/// - `pub hash: Arc<str>` - Hash for change detection (defaults to empty)
///
/// # Derives
///
/// On the entity:
/// - `Partial`, `PartialEq`, `Clone`, `Serialize`, `Deserialize`, `Debug`, `TS`
/// - `Default` (only if `#[ensure_for]` attributes are present)
///
/// On the generated `Partial{Entity}`:
/// - `Clone`, `Serialize`, `Deserialize`, `Debug`, `Default`, `PartialMatches`, `TS`
///
/// # Generated Queries
///
/// | Query | Description |
/// |-------|-------------|
/// | `GetAll{Entity}s` | Returns all entities of this type |
/// | `Get{Entity}sByIds { ids: Vec<Arc<str>> }` | Returns entities matching the given IDs |
/// | `Get{Entity}sByQuery(Partial{Entity})` | Returns entities matching partial field values |
///
/// # Generated Reports
///
/// | Report | Output Type | Description |
/// |--------|-------------|-------------|
/// | `Get{Entity}ById { id: Arc<str> }` | `Option<{Entity}>` | Returns a single entity by ID |
/// | `CountAll{Entity}s` | `{Entity}Count` | Returns total count of all entities |
/// | `Count{Entity}s(Partial{Entity})` | `{Entity}Count` | Returns count matching partial filter |
///
/// # Generated Commands
///
/// | Command | Result Type | Description |
/// |---------|-------------|-------------|
/// | `Delete{Entity} { id: Arc<str> }` | `Delete{Entity}Result` | Deletes a single entity |
/// | `Delete{Entity}s { ids: Vec<Arc<str>> }` | `Delete{Entity}sResult` | Deletes multiple entities |
///
/// # Generated Types
///
/// | Type | Description |
/// |------|-------------|
/// | `Partial{Entity}` | Partial version with all fields optional, for filtering |
/// | `{Entity}Count` | Count result with `count: usize` field |
/// | `Delete{Entity}Result` | Single delete result with `deleted: bool` field |
/// | `Delete{Entity}sResult` | Bulk delete result with `deleted_count: usize` field |
///
/// # Field Attributes
///
/// ## `#[myko_rename]`
/// Generates a `Rename{Entity} { id, name }` command that updates the annotated field.
/// The field is typically named `name` but can be any `String` field.
///
/// ```ignore
/// #[myko_item]
/// pub struct Target {
///     #[myko_rename]
///     pub name: String,
/// }
/// // Generates: RenameTarget { id: Arc<str>, name: Arc<str> }
/// ```
///
/// ## `#[myko_setter]` / `#[myko_setter("CustomName")]`
/// Generates a setter command for the field. Without an argument, generates
/// `Set{Entity}{Field}`. With a string argument, uses that as the command name.
///
/// ```ignore
/// #[myko_item]
/// pub struct Scene {
///     #[myko_setter]
///     pub is_active: bool,
///     #[myko_setter("ToggleSceneVisibility")]
///     pub visible: bool,
/// }
/// // Generates: SetSceneIsActive { id, is_active }
/// // Generates: ToggleSceneVisibility { id, visible }
/// ```
///
/// ## `#[belongs_to(ParentEntity)]`
/// Declares a parent-child relationship. When the parent is deleted, the child
/// is cascade-deleted. The field should contain the parent's ID.
///
/// ```ignore
/// #[myko_item]
/// pub struct Binding {
///     #[belongs_to(Scene)]
///     pub scene_id: String,
/// }
/// // When Scene is deleted, all Bindings with that scene_id are deleted
/// ```
///
/// ## `#[owns_many(ChildEntity)]`
/// Declares ownership of child entities via an ID list. When the parent is deleted,
/// children are deleted. When a child is deleted, its ID is removed from the list.
///
/// ```ignore
/// #[myko_item]
/// pub struct Scene {
///     #[owns_many(BindingNode)]
///     pub node_ids: Vec<String>,
/// }
/// ```
///
/// ## `#[ensure_for(DependencyEntity)]`
/// Auto-creates one entity instance per dependency. Multiple `ensure_for` attributes
/// on different fields create a Cartesian product.
///
/// ```ignore
/// #[myko_item]
/// pub struct BundleStatus {
///     #[ensure_for(Session)]
///     pub session_id: String,
///     #[ensure_for(Bundle)]
///     pub bundle_id: String,
/// }
/// // Creates one BundleStatus per Session×Bundle combination
/// ```
///
/// ## `#[myko_client_id]`
/// Server auto-populates this field with the WebSocket client ID that sent the event.
///
/// ```ignore
/// #[myko_item]
/// pub struct Instance {
///     #[myko_client_id]
///     pub client_id: Option<String>,
/// }
/// ```
///
/// ## `#[searchable]`
/// Marks a field for full-text search indexing.
///
/// ```ignore
/// #[myko_item]
/// pub struct Target {
///     #[searchable]
///     pub name: String,
///     #[searchable]
///     pub description: String,
///     pub internal_id: String,  // not searchable
/// }
/// ```
///
/// ## `#[default_value(expr)]`
/// Sets a default value for the field when auto-creating via `ensure_for`.
///
/// # Requirements
///
/// All manually-added fields must implement `Clone`, `Serialize`, and `Deserialize`.
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
