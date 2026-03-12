use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse_macro_input;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::Token;

mod command;
mod item;
mod message_events;
mod partial_matches;
mod query;
mod relationship;
mod report;
mod saga;
mod setter;
mod view;

/// Returns whether we are compiling inside the myko-rs crate itself.
pub(crate) fn is_myko_rs_crate() -> bool {
    std::env::var("CARGO_PKG_NAME")
        .map(|name| name == "myko-rs")
        .unwrap_or(false)
}

/// Returns the path to use for `myko_rs` depending on the current crate.
/// When compiling myko-rs itself, returns `crate`; otherwise returns `myko_rs`.
pub(crate) fn myko_rs_path() -> syn::Path {
    if is_myko_rs_crate() {
        syn::Path::from(syn::Ident::new("crate", Span::call_site()))
    } else {
        syn::Path::from(syn::Ident::new("myko_rs", Span::call_site()))
    }
}

/// Context for generating serde/partially derive paths in macros.
/// When inside myko-rs, uses direct crate paths. When outside, uses re-exports.
pub(crate) struct DeriveCtx {
    /// Path to myko_rs (either `crate` or `myko_rs`)
    pub krate: syn::Path,
    /// Path for serde derives (either `serde` or `myko_rs::serde`)
    pub serde_path: proc_macro2::TokenStream,
    /// String value for #[serde(crate = "...")] — None when inside myko-rs
    pub serde_crate_attr: Option<String>,
    /// Path for partially derives (either `partially` or `myko_rs::partially`)
    pub partially_path: proc_macro2::TokenStream,
    /// String value for #[partially(crate = "...")] — None when inside myko-rs
    pub partially_crate_attr: Option<String>,
}

impl DeriveCtx {
    pub fn new() -> Self {
        let krate = myko_rs_path();
        if is_myko_rs_crate() {
            Self {
                krate,
                serde_path: quote!(serde),
                serde_crate_attr: None,
                partially_path: quote!(partially),
                partially_crate_attr: None,
            }
        } else {
            let serde_crate_str = "myko_rs::serde".to_string();
            let partially_crate_str = "myko_rs::partially".to_string();
            Self {
                krate,
                serde_path: quote!(myko_rs::serde),
                serde_crate_attr: Some(serde_crate_str),
                partially_path: quote!(myko_rs::partially),
                partially_crate_attr: Some(partially_crate_str),
            }
        }
    }

    /// Generate #[serde(crate = "...", ...rest)] or just #[serde(...rest)]
    pub fn serde_attr(&self, rest: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        match &self.serde_crate_attr {
            Some(crate_str) => {
                if rest.is_empty() {
                    quote!(#[serde(crate = #crate_str)])
                } else {
                    quote!(#[serde(crate = #crate_str, #rest)])
                }
            }
            None => {
                if rest.is_empty() {
                    quote!()
                } else {
                    quote!(#[serde(#rest)])
                }
            }
        }
    }
}

pub(crate) fn take_manual_cache_key_attr(input_struct: &mut syn::ItemStruct) -> bool {
    let mut found = take_marker_attr(input_struct, "myko_manual_cache_key");
    input_struct.attrs.retain(|attr| {
        let is_doc_marker = attr.path().is_ident("doc")
            && attr
                .meta
                .require_name_value()
                .ok()
                .and_then(|nv| match &nv.value {
                    syn::Expr::Lit(expr_lit) => match &expr_lit.lit {
                        syn::Lit::Str(s) => Some(s.value() == "__myko_manual_cache_key"),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap_or(false);
        found |= is_doc_marker;
        !is_doc_marker
    });
    found
}

pub(crate) fn take_non_hash_cache_key_attr(input_struct: &mut syn::ItemStruct) -> bool {
    let mut found = take_marker_attr(input_struct, "myko_non_hash_cache_key");
    input_struct.attrs.retain(|attr| {
        let is_doc_marker = attr.path().is_ident("doc")
            && attr
                .meta
                .require_name_value()
                .ok()
                .and_then(|nv| match &nv.value {
                    syn::Expr::Lit(expr_lit) => match &expr_lit.lit {
                        syn::Lit::Str(s) => Some(s.value() == "__myko_non_hash_cache_key"),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap_or(false);
        found |= is_doc_marker;
        !is_doc_marker
    });
    found
}

fn take_marker_attr(input_struct: &mut syn::ItemStruct, attr_name: &str) -> bool {
    let mut found = false;
    input_struct.attrs.retain(|attr| {
        let matches = attr.path().is_ident(attr_name);
        found |= matches;
        !matches
    });
    found
}

#[proc_macro_attribute]
pub fn myko_manual_cache_key(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::ItemStruct);
    quote! {
        #[doc = "__myko_manual_cache_key"]
        #item
    }
    .into()
}

#[proc_macro_attribute]
pub fn myko_non_hash_cache_key(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::ItemStruct);
    quote! {
        #[doc = "__myko_non_hash_cache_key"]
        #item
    }
    .into()
}

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
/// | `{Entity}Id` | Entity-specific ID wrapper over `Arc<str>` (TypeScript: `string`) |
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
pub fn myko_item(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as item::ItemArgs);
    let input = parse_macro_input!(input as syn::ItemStruct);
    item::myko_item_impl(args, input).into()
}

#[proc_macro_attribute]
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    let query_item_type = parse_macro_input!(attr as syn::Path);
    let input = parse_macro_input!(input as syn::ItemStruct);
    query::myko_query_impl(query_item_type, input).into()
}

/// Defines a reactive view query.
///
/// Preferred stacked syntax:
/// ```ignore
/// #[myko_view]
/// #[view(output = TargetTreeView, root = Target, root_out = target)]
/// #[tree(parent_param = parent_target_id, parent_field = parent_targets, include_offline_param = include_offline)]
/// #[source(Target, key = id)]
/// #[source(TargetStatus, key = target_id)]
/// #[source(Action, key = id)]
/// #[source(Emitter, key = id)]
/// #[join_one(Target.id == TargetStatus.target_id, out = is_online, online = Status::Online)]
/// #[join_many(Target.id == Action.target_id, out = actions)]
/// #[join_many(Target.id == Emitter.target_id, out = emitters)]
/// pub struct GetTargetTreeByParentFiltered {
///     pub parent_target_id: Option<Arc<str>>,
///     pub include_offline: bool,
/// }
/// ```
///
/// Query-style declaration syntax:
/// `#[myko_view(ViewItemType)]`
/// and then implement `myko_rs::prelude::ViewHandler` for the params type with:
/// `fn build_cell(ctx: ViewBuildCellCtx<Self>) -> FilteredViewCellMap`.
#[proc_macro_attribute]
pub fn myko_view(attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    if attr.is_empty() {
        return syn::Error::new(
            input.ident.span(),
            "#[myko_view] requires an item type: #[myko_view(ViewItemType)]",
        )
        .to_compile_error()
        .into();
    }
    let args = parse_macro_input!(attr as view::ViewArgs);
    view::myko_view_impl(args, input).into()
}

/// Marks a struct as a typed view item (id/hash should already be present).
///
/// Adds serde/TS derives, TS export registration, and implements:
/// - `WithId` (from `id`)
/// - `AnyItem`
/// - `Eventable`
#[proc_macro_attribute]
pub fn myko_view_item(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    view::myko_view_item_impl(input).into()
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
    let options = if attr.is_empty() {
        command::CommandOptions {
            result_type: None,
            custom_serialize: false,
        }
    } else {
        parse_macro_input!(attr as CommandArgs).into()
    };
    let input = parse_macro_input!(input as syn::ItemStruct);
    command::myko_command_impl(options, input).into()
}

struct CommandArgs {
    result_type: Option<syn::Path>,
    custom_serialize: bool,
}

impl From<CommandArgs> for command::CommandOptions {
    fn from(value: CommandArgs) -> Self {
        Self {
            result_type: value.result_type,
            custom_serialize: value.custom_serialize,
        }
    }
}

impl Parse for CommandArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let args = Punctuated::<syn::Path, Token![,]>::parse_terminated(input)?;
        let mut result_type = None;
        let mut custom_serialize = false;

        for path in args {
            if path.is_ident("custom_serialize") {
                if custom_serialize {
                    return Err(syn::Error::new(path.span(), "duplicate custom_serialize flag"));
                }
                custom_serialize = true;
                continue;
            }

            if result_type.is_some() {
                return Err(syn::Error::new(path.span(), "expected at most one result type"));
            }

            result_type = Some(path);
        }

        Ok(Self {
            result_type,
            custom_serialize,
        })
    }
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
/// impl myko_rs::saga::SagaHandler for CleanupSaga {
///     type EventItem = myko_rs::entities::client::Client;
///     type Command = HandleClientDisconnected;
///     const EVENT_TYPE: myko_rs::event::MEventType = myko_rs::event::MEventType::DEL;
///
///     fn handle(
///         item: Self::EventItem,
///         event: myko_rs::event::MEvent,
///         ctx: std::sync::Arc<myko_rs::saga::SagaContext>,
///     ) -> Option<Self::Command> {
///         // Saga logic here
///         None
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn myko_saga(attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    saga::myko_saga_impl(attr.into(), input).into()
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
    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    // ToValue is implemented via blanket impl for all Serialize types
    let expanded = quote! {
        #[derive(Debug, Clone, PartialEq, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
        #serde_rename_attr
        #input

        #krate::register_ts_export!(#name);
    };

    expanded.into()
}
