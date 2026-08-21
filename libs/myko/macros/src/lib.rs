use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::{
    Token,
    ext::IdentExt,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
};

mod command;
mod graph;
mod item;
mod message_events;
mod query;
mod relationship;
mod report;
mod saga;
mod setter;
mod view;

/// Declare an open, downstream-defined Myko entity category.
#[proc_macro_attribute]
pub fn myko_category(_attr: TokenStream, input: TokenStream) -> TokenStream {
    graph::category(&parse_macro_input!(input as syn::ItemStruct)).into()
}

/// Add an item type to one or more entity categories.
#[proc_macro_attribute]
pub fn myko_in(attr: TokenStream, input: TokenStream) -> TokenStream {
    let categories =
        parse_macro_input!(attr with Punctuated::<syn::Path, Token![,]>::parse_terminated);
    graph::category_membership(&categories, &parse_macro_input!(input as syn::ItemStruct)).into()
}

/// Register a [`GraphEdge`] implementation without changing its item schema.
#[proc_macro_attribute]
pub fn myko_edge(_attr: TokenStream, input: TokenStream) -> TokenStream {
    graph::edge(parse_macro_input!(input as syn::ItemImpl)).into()
}

/// Returns whether we are compiling inside the myko crate itself.
pub(crate) fn is_myko_crate() -> bool {
    std::env::var("CARGO_PKG_NAME").is_ok_and(|name| name == "myko")
}

/// Returns the path to use for `myko` depending on the current crate.
/// When compiling myko itself, returns `crate`; otherwise returns `myko`.
pub(crate) fn myko_path() -> syn::Path {
    if is_myko_crate() {
        syn::Path::from(syn::Ident::new("crate", Span::call_site()))
    } else {
        syn::Path::from(syn::Ident::new("myko", Span::call_site()))
    }
}

/// Context for generating serde derive paths in macros.
/// When inside myko, uses direct crate paths. When outside, uses re-exports.
pub(crate) struct DeriveCtx {
    /// Path to myko (either `crate` or `myko`)
    pub krate: syn::Path,
    /// Path for serde derives (either `serde` or `myko::serde`)
    pub serde_path: proc_macro2::TokenStream,
    /// String value for #[serde(crate = "...")] — None when inside myko
    pub serde_crate_attr: Option<String>,
    /// String value for ts-rs's `crate` override.
    pub ts_crate: String,
}

impl DeriveCtx {
    pub fn new() -> Self {
        let krate = myko_path();
        if is_myko_crate() {
            Self {
                krate,
                serde_path: quote!(serde),
                serde_crate_attr: None,
                ts_crate: "crate::ts_rs".to_string(),
            }
        } else {
            let serde_crate_str = "myko::serde".to_string();
            Self {
                krate,
                serde_path: quote!(myko::serde),
                serde_crate_attr: Some(serde_crate_str),
                ts_crate: "myko::ts_rs".to_string(),
            }
        }
    }

    /// Generate #[serde(crate = "...", ...rest)] or just #[serde(...rest)]
    pub fn serde_attr(&self, rest: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        self.serde_crate_attr.as_ref().map_or_else(
            || {
                if rest.is_empty() {
                    quote!()
                } else {
                    quote!(#[serde(#rest)])
                }
            },
            |crate_str| {
                if rest.is_empty() {
                    quote!(#[serde(crate = #crate_str)])
                } else {
                    quote!(#[serde(crate = #crate_str, #rest)])
                }
            },
        )
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

/// Noop replacement for `ts_rs::TS` derive — emits no trait impls and
/// declares the `ts` helper attribute so user-written `#[ts(...)]` in
/// entity source doesn't error out when `ts_rs::TS` is absent.
///
/// `myko::TS` routes to this derive when the consuming crate has
/// `codegen-ts` off. When on, `myko::TS` resolves to `ts_rs::TS` instead
/// and full TS impls are generated.
#[proc_macro_derive(TsNoop, attributes(ts))]
pub fn ts_noop_derive(_input: TokenStream) -> TokenStream {
    TokenStream::new()
}

/// No-op retained for call-site compatibility.
///
/// `#[myko_item]`/`#[myko_subtype]` now always emit `#[derive(myko::TS)]`
/// (which resolves to the no-op `TsNoop` derive unless myko's own
/// `codegen-ts` feature is on). Because that derive always claims the `ts`
/// helper-attribute namespace, user-written `#[ts(...)]` attrs are valid
/// as-is and no longer need wrapping in a consumer-side `cfg_attr`.
pub(crate) fn gate_ts_attrs(attrs: &mut [syn::Attribute]) {
    for attr in attrs.iter_mut() {
        if !attr.path().is_ident("myko") {
            continue;
        }
        let Ok(parsed) = attr.parse_args::<ExportOverride>() else {
            continue;
        };
        let mut args = Vec::new();
        if let Some(value) = parsed.type_override {
            args.push(quote!(type = #value));
        }
        if let Some(rename) = parsed.rename {
            args.push(quote!(rename = #rename));
        }
        if parsed.skip {
            args.push(quote!(skip));
        }
        if parsed.nullable {
            args.push(quote!(optional = nullable));
        } else if parsed.optional {
            args.push(quote!(optional));
        }
        *attr = syn::parse_quote!(#[ts(#(#args),*)]);
    }
}

#[derive(Default)]
struct ExportOverride {
    type_override: Option<syn::LitStr>,
    rename: Option<syn::LitStr>,
    optional: bool,
    nullable: bool,
    skip: bool,
}

impl Parse for ExportOverride {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let export = syn::Ident::parse_any(input)?;
        if export != "export" {
            return Err(syn::Error::new_spanned(export, "expected `export(...)`"));
        }
        let content;
        syn::parenthesized!(content in input);
        let mut result = Self::default();
        while !content.is_empty() {
            let key = syn::Ident::parse_any(&content)?;
            if key == "type" {
                content.parse::<Token![=]>()?;
                result.type_override = Some(content.parse()?);
            } else if key == "rename" {
                content.parse::<Token![=]>()?;
                result.rename = Some(content.parse()?);
            } else if key == "optional" {
                result.optional = true;
            } else if key == "nullable" {
                result.nullable = true;
            } else if key == "skip" {
                result.skip = true;
            } else {
                return Err(syn::Error::new_spanned(
                    key,
                    "expected `type`, `rename`, `optional`, `nullable`, or `skip`",
                ));
            }
            if content.is_empty() {
                break;
            }
            content.parse::<Token![,]>()?;
        }
        Ok(result)
    }
}

/// Extract a struct's doc comment (`/// ...` lines, which desugar to
/// `#[doc = "..."]` attrs) as one joined string, or `None` if there isn't
/// one. Call after `take_manual_cache_key_attr`/`take_non_hash_cache_key_attr`
/// have already stripped their internal marker doc attrs, so only genuine
/// user-written doc comments remain.
pub(crate) fn extract_doc_comment(attrs: &[syn::Attribute]) -> Option<String> {
    let lines: Vec<String> = attrs
        .iter()
        .filter_map(|attr| {
            if !attr.path().is_ident("doc") {
                return None;
            }
            let syn::Meta::NameValue(nv) = &attr.meta else {
                return None;
            };
            let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) = &nv.value
            else {
                return None;
            };
            let line = s.value();
            let line = line.trim();
            (!line.is_empty()).then(|| line.to_string())
        })
        .collect();
    (!lines.is_empty()).then(|| lines.join(" "))
}

/// Build a `&[#krate::reflection::OperationArgField]` token stream
/// describing `fields`'s named members — captured directly from the struct
/// definition at macro-expansion time (field name, its Rust type as
/// written, and whether it's `Option<...>`) rather than re-derived from
/// generated ts-rs output. See `myko::reflection` for why.
pub(crate) fn field_metadata_tokens(
    fields: &syn::Fields,
    krate: &syn::Path,
) -> proc_macro2::TokenStream {
    let entries: Vec<_> = match fields {
        syn::Fields::Named(named) => named
            .named
            .iter()
            .filter_map(|f| {
                let name = f.ident.as_ref()?.to_string();
                let ty = &f.ty;
                let rust_type = quote!(#ty).to_string();
                let optional = is_option_type(ty);
                Some(quote! {
                    #krate::reflection::OperationArgField {
                        name: #name,
                        rust_type: #rust_type,
                        optional: #optional,
                    }
                })
            })
            .collect(),
        _ => Vec::new(),
    };
    quote! { &[ #(#entries),* ] }
}

pub(crate) fn operation_metadata_tokens(
    input: &syn::ItemStruct,
    krate: &syn::Path,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let description = extract_doc_comment(&input.attrs);
    let description = description
        .as_ref()
        .map_or_else(|| quote!(None), |value| quote!(Some(#value)));
    (description, field_metadata_tokens(&input.fields, krate))
}

pub(crate) fn gate_field_ts_attrs(fields: &mut syn::Fields) {
    for field in fields {
        prepare_typegen_field(field);
    }
}

/// Apply language-backend metadata owned by Myko. Optional Rust fields are
/// optional and nullable in generated bindings by default, so downstream
/// entity definitions need no duplicate representation annotation.
pub(crate) fn prepare_typegen_field(field: &mut syn::Field) {
    gate_ts_attrs(&mut field.attrs);
    if !is_option_type(&field.ty) {
        return;
    }

    let has_explicit_policy = field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("ts"))
        .any(|attr| {
            let tokens = attr.meta.to_token_stream().to_string();
            tokens.contains("optional") || tokens.contains("skip")
        });
    if !has_explicit_policy {
        field
            .attrs
            .push(syn::parse_quote!(#[ts(optional = nullable)]));
    }
}

fn is_option_type(ty: &syn::Type) -> bool {
    let syn::Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|seg| seg.ident == "Option")
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
/// - `PartialEq`, `Clone`, `Serialize`, `Deserialize`, `Debug`, `TS`
/// - `Default` (only if `#[ensure_for]` attributes are present)
///
/// On the generated `{Entity}Query`:
/// - `Clone`, `Default`, `PartialEq`, `Debug`, `Serialize`, `Deserialize`, `TS`
///
/// # Generated Queries
///
/// | Query | Description |
/// |-------|-------------|
/// | `GetAll{Entity}s` | Returns all entities of this type |
/// | `Get{Entity}sByIds { ids: Vec<Arc<str>> }` | Returns entities matching the given IDs |
/// | `Get{Entity}sByQuery({Entity}Query)` | Returns entities matching the query — every field is `Option<<FieldType as Filterable>::Filter>` (`Eq`/`In`/`Range`/`Contains` depending on the field's type), not a flat value |
///
/// # Generated Reports
///
/// | Report | Output Type | Description |
/// |--------|-------------|-------------|
/// | `Get{Entity}ById { id: Arc<str> }` | `Option<{Entity}>` | Returns a single entity by ID |
/// | `CountAll{Entity}s` | `{Entity}Count` | Returns total count of all entities |
/// | `Count{Entity}s({Entity}Query)` | `{Entity}Count` | Returns count matching the query |
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
/// | `{Entity}Query` | Per-field filter struct, for `Get{Entity}sByQuery`/`Count{Entity}s`/`ctx.query_live(...)` |
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
    item::myko_item_impl(&args, input).into()
}

#[proc_macro_attribute]
/// Define a typed Myko query.
///
/// Queries are included in generated-language bindings by default. Pass
/// `export = false` for Rust-only queries while retaining normal runtime
/// registration and wire behavior.
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as QueryArgs);
    let input = parse_macro_input!(input as syn::ItemStruct);
    query::myko_query_impl(&args.item_type, args.export, input).into()
}

struct QueryArgs {
    item_type: syn::Path,
    export: bool,
}

impl Parse for QueryArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_type = input.parse()?;
        if input.is_empty() {
            return Ok(Self {
                item_type,
                export: true,
            });
        }

        input.parse::<Token![,]>()?;
        let option = syn::Ident::parse_any(input)?;
        if option != "export" {
            return Err(syn::Error::new_spanned(option, "expected `export = false`"));
        }
        input.parse::<Token![=]>()?;
        let export = input.parse::<syn::LitBool>()?.value;
        if !input.is_empty() {
            return Err(input.error("unexpected query options"));
        }
        Ok(Self { item_type, export })
    }
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
/// and then implement `myko::prelude::ViewHandler` for the params type with:
/// `fn build_cell(ctx: ViewBuildArgs<Self>) -> FilteredViewCellMap`.
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
///         ctx: myko::prelude::ReportContext,
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
    report::myko_report_impl(&report_output_type, input).into()
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
                    return Err(syn::Error::new(
                        path.span(),
                        "duplicate custom_serialize flag",
                    ));
                }
                custom_serialize = true;
                continue;
            }

            if result_type.is_some() {
                return Err(syn::Error::new(
                    path.span(),
                    "expected at most one result type",
                ));
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
/// and generates `MessageEventRegistration` inventory submissions.
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
    message_events::derive_message_events_impl(&input).into()
}

/// Generates a saga with registration for runtime discovery.
///
/// # Usage
///
/// ```ignore
/// #[myko_saga]
/// pub struct CleanupSaga;
///
/// impl myko::saga::SagaHandler for CleanupSaga {
///     type EventItem = myko::entities::client::Client;
///     type Command = HandleClientDisconnected;
///     const EVENT_TYPE: myko::event::MEventType = myko::event::MEventType::DEL;
///
///     fn handle(
///         item: Self::EventItem,
///         event: myko::event::MEvent,
///         ctx: std::sync::Arc<myko::saga::SagaContext>,
///     ) -> Option<Self::Command> {
///         // Saga logic here
///         None
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn myko_saga(attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemStruct);
    let attr = attr.into();
    saga::myko_saga_impl(&attr, &input).into()
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
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, myko::TS)]
/// #[serde(rename_all = "camelCase")]
/// pub struct ServerStatsOutput { ... }
/// myko::register_typegen_type!(ServerStatsOutput);
/// ```
#[proc_macro_attribute]
pub fn myko_report_output(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as syn::ItemStruct);
    let name = &input.ident;
    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(&quote!(rename_all = "camelCase"));

    gate_ts_attrs(&mut input.attrs);
    for field in &mut input.fields {
        prepare_typegen_field(field);
    }
    let equal_fields = input
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let member = field.ident.clone().map_or_else(
                || syn::Member::Unnamed(syn::Index::from(index)),
                syn::Member::Named,
            );
            quote! { self.#member == other.#member }
        })
        .reduce(|acc, term| quote! { (#acc) && (#term) })
        .unwrap_or_else(|| quote! { true });

    // ToValue is implemented via blanket impl for all Serialize types
    let expanded = quote! {
        #[derive(Debug, Clone, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
        #[ts(crate = "myko::ts_rs")]
        #serde_rename_attr
        #input

        impl PartialEq for #name {
            fn eq(&self, other: &Self) -> bool { #equal_fields }
        }

        #krate::register_typegen_type!(#name);
    };

    expanded.into()
}

/// Declare a data subtype used by myko entities (field types, payloads,
/// enum variants carried on commands/queries/reports/views).
///
/// Bundles the
/// standard derives + serde camelCase rename + conditional TS export +
/// `register_typegen_type!` so subtype definitions don't repeat 3–4 lines of
/// boilerplate each.
///
/// Default derives: `Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize`.
/// Always added: `#[cfg_attr(feature = "codegen-ts", derive(myko::TS))]`,
/// `#[cfg_attr(feature = "codegen-ts", ts(export))]`, and
/// `#[serde(rename_all = "camelCase")]`. Emits a `register_typegen_type!`
/// call after the item so typegen picks it up when the feature is on.
///
/// Extra derives (e.g. `Default`, `Eq`, `Hash`, `Copy`) can be requested
/// via `derive(...)` — they're appended to the default list.
///
/// `manual(serde)` opts a type out of the default `Serialize`/`Deserialize`
/// derives and camel-case attribute when it owns a custom wire format.
/// Backend representation overrides remain Myko-owned through
/// `export(as = "...")`; downstream crates never implement a generator trait.
///
/// `Debug`/`Clone`/`PartialEq` and any `derive(...)` extras are
/// unaffected by `manual(...)`.
///
/// Also auto-implements `query::Filterable`, so the type can be used as an
/// `#[myko_item]` entity field without a hand-written
/// `impl_filterable_eq!`/`impl_filterable_opaque!` call: deriving both `Eq`
/// and `Ord` gets you `EqFilter` (exact-match/`In` filtering); anything
/// less falls back to `Unfilterable` (the field still compiles, it's just
/// not filterable). A type with its own hand-written `Filterable` impl —
/// e.g. a custom filter whose `matches` uses domain equivalence instead of
/// derived `PartialEq` — opts out with `manual(filterable)`; without it the
/// auto-impl conflicts with the hand-written one.
///
/// # Usage
///
/// ```ignore
/// #[myko_subtype]
/// pub struct UserData {
///     pub id: UserId,
/// }
///
/// #[myko_subtype(derive(Default, Eq))]
/// pub enum NetworkEventType {
///     Added,
///     Removed,
/// }
///
/// #[myko_subtype(derive(Default, Eq, Hash))]
/// pub struct DeviceShareKey {
///     pub device_id: Arc<str>,
///     pub user_id: Arc<str>,
/// }
///
/// // Hand-written Serialize/Deserialize with a Myko-owned opaque binding.
/// #[myko_subtype(derive(Default), manual(serde), export(as = "unknown"))]
/// pub struct BindingValue {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn myko_subtype(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as SubtypeArgs);
    let item: syn::Item = parse_macro_input!(input as syn::Item);
    myko_subtype_expand(args, item).into()
}

struct SubtypeArgs {
    extra_derives: Vec<syn::Path>,
    /// `manual(serde)` — the item has its own hand-written `Serialize`/
    /// `Deserialize` impls (e.g. a custom plain-JSON wire format that
    /// deriving would change); skips those derives AND the default
    /// `#[serde(rename_all = "camelCase")]` (the attribute is a serde
    /// derive-macro helper attr — emitting it with no `#[derive(Serialize/
    /// Deserialize)]` present is a hard compile error, not a no-op).
    manual_serde: bool,
    /// `export(as = "...")` — a Myko-owned opaque/custom wire mapping.
    export_as: Option<syn::LitStr>,
    /// `manual(filterable)` — the item has its own hand-written
    /// `query::Filterable` impl (e.g. a custom filter type whose `matches`
    /// uses domain equivalence instead of derived `PartialEq`); skips the
    /// auto-impl, which would otherwise conflict.
    manual_filterable: bool,
}

impl syn::parse::Parse for SubtypeArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut extra_derives = Vec::new();
        let mut manual_serde = false;
        let mut export_as = None;
        let mut manual_filterable = false;

        // `derive(Foo, Bar)`, `manual(serde, filterable)` — comma-separated.
        // either or both omitted.
        let metas: syn::punctuated::Punctuated<syn::Meta, syn::Token![,]> =
            syn::punctuated::Punctuated::parse_terminated(input)?;

        for meta in metas {
            let syn::Meta::List(list) = &meta else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "expected `derive(...)`, `manual(...)`, or `export(as = \"...\")`",
                ));
            };
            if list.path.is_ident("derive") {
                let punct: syn::punctuated::Punctuated<syn::Path, syn::Token![,]> =
                    list.parse_args_with(syn::punctuated::Punctuated::parse_terminated)?;
                extra_derives.extend(punct);
            } else if list.path.is_ident("export") {
                export_as = Some(list.parse_args_with(|input: ParseStream| {
                    let keyword = syn::Ident::parse_any(input)?;
                    if keyword != "as" {
                        return Err(syn::Error::new_spanned(keyword, "expected `as = \"...\"`"));
                    }
                    input.parse::<Token![=]>()?;
                    input.parse::<syn::LitStr>()
                })?);
            } else if list.path.is_ident("manual") {
                let punct: syn::punctuated::Punctuated<syn::Ident, syn::Token![,]> =
                    list.parse_args_with(syn::punctuated::Punctuated::parse_terminated)?;
                for ident in punct {
                    if ident == "serde" {
                        manual_serde = true;
                    } else if ident == "filterable" {
                        manual_filterable = true;
                    } else {
                        return Err(syn::Error::new_spanned(
                            &ident,
                            "expected `serde` or `filterable` inside `manual(...)`",
                        ));
                    }
                }
            } else {
                return Err(syn::Error::new_spanned(
                    &list.path,
                    "expected `derive(...)`, `manual(...)`, or `export(as = \"...\")`",
                ));
            }
        }

        Ok(Self {
            extra_derives,
            manual_serde,
            export_as,
            manual_filterable,
        })
    }
}

fn subtype_registration(
    krate: &syn::Path,
    name: &syn::Ident,
    has_export_override: bool,
) -> proc_macro2::TokenStream {
    if has_export_override {
        quote!()
    } else {
        quote!(#krate::register_typegen_type!(#name);)
    }
}

fn myko_subtype_expand(args: SubtypeArgs, mut item: syn::Item) -> proc_macro2::TokenStream {
    let SubtypeArgs {
        extra_derives,
        manual_serde,
        export_as,
        manual_filterable,
    } = args;
    let ctx = DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;

    // Common setup: gate user-written `#[ts(...)]` attrs, extract name for
    // the `register_typegen_type!` call. Also normalize visibility expectations
    // to either struct or enum — other shapes aren't meaningful as subtypes.
    //
    // `is_struct` controls whether we default to `#[serde(rename_all = "camelCase")]`.
    // For structs, Rust field names are snake_case and wire is camelCase → we need
    // the rename. For enums, Rust variants are PascalCase (matching the wire form
    // used historically in this codebase) so auto-renaming to camelCase would
    // silently change the serialized representation and break existing stored
    // data. Enums that want a non-default casing must supply their own
    // `#[serde(rename_all = ...)]`.
    let (name, has_rename_all, is_struct) = match &mut item {
        syn::Item::Struct(s) => {
            gate_ts_attrs(&mut s.attrs);
            for field in &mut s.fields {
                prepare_typegen_field(field);
            }
            (s.ident.clone(), attrs_have_serde_rename_all(&s.attrs), true)
        }
        syn::Item::Enum(e) => {
            gate_ts_attrs(&mut e.attrs);
            for variant in &mut e.variants {
                gate_ts_attrs(&mut variant.attrs);
                for field in &mut variant.fields {
                    prepare_typegen_field(field);
                }
            }
            (
                e.ident.clone(),
                attrs_have_serde_rename_all(&e.attrs),
                false,
            )
        }
        other => {
            return syn::Error::new_spanned(
                other,
                "#[myko_subtype] only supports `struct` and `enum` items",
            )
            .to_compile_error();
        }
    };

    let extra_derive_tokens = if extra_derives.is_empty() {
        quote!()
    } else {
        quote!(, #(#extra_derives),*)
    };

    // Only emit the default camelCase rename on structs when the user hasn't
    // already supplied one, and never when `manual(serde)` is set — with no
    // `#[derive(Serialize/Deserialize)]` present, `#[serde(...)]` is an
    // unrecognized attribute (a hard compile error, not a no-op).
    let serde_rename_attr = if is_struct && !has_rename_all && !manual_serde {
        ctx.serde_attr(&quote!(rename_all = "camelCase"))
    } else {
        quote!()
    };

    // `manual(serde)` skips only the wire derives; generated binding metadata
    // remains owned by Myko, including opaque backend representations.
    let serde_derive_tokens = if manual_serde {
        quote!()
    } else {
        quote!(, #serde_path::Serialize, #serde_path::Deserialize)
    };
    let has_export_override = export_as.is_some();
    let ts_derive_tokens = if has_export_override {
        quote!()
    } else {
        quote!(, #krate::TS)
    };
    let ts_export_attr = if has_export_override {
        quote!()
    } else {
        let ts_crate = &ctx.ts_crate;
        quote!(#[ts(crate = #ts_crate, export)])
    };
    let register_export_call = subtype_registration(krate, &name, has_export_override);
    let export_override_impl = export_as.map_or_else(
        || quote!(),
        |wire_type| quote!(#krate::impl_ts_as!(#name, #wire_type);),
    );

    // Every #[myko_subtype] auto-implements Filterable, so a type declared
    // this way is always usable as an entity field without a hand-written
    // impl_filterable_eq!/impl_filterable_opaque! call (the two escape
    // hatches those macros exist for downstream crates that DON'T go
    // through myko_subtype — e.g. a plain third-party or hand-written enum).
    // EqFilter<T>'s CanonicalFilter impl needs T: Ord + Clone (for the
    // In-set sort/dedup step query-cache identity depends on, spec §1), so
    // only pick EqFilter when the consumer actually derived Eq + Ord;
    // otherwise fall back to Unfilterable — same degenerate-but-compiling
    // treatment serde_json::Value and the container blanket impls get.
    let derives_total_order = extra_derives.iter().any(|p| p.is_ident("Ord"))
        && extra_derives.iter().any(|p| p.is_ident("Eq"));
    let filterable_impl = if manual_filterable {
        quote!()
    } else if derives_total_order {
        quote! {
            impl #krate::query::Filterable for #name {
                type Filter = #krate::query::EqFilter<#name>;
            }
        }
    } else {
        quote! {
            impl #krate::query::Filterable for #name {
                type Filter = #krate::query::Unfilterable;
            }
        }
    };

    // `myko::TS` is the no-op `TsNoop` derive unless myko's own `codegen-ts`
    // feature is on, so emit it (and the `ts(export)` attr it claims)
    // unconditionally — no consumer-side feature gate. Concrete declarations
    // register with the active backend; opaque inline mappings do not create files.
    quote! {
        #[derive(Debug, Clone, PartialEq #serde_derive_tokens #ts_derive_tokens #extra_derive_tokens)]
        #ts_export_attr
        #serde_rename_attr
        #item

        #export_override_impl
        #register_export_call

        #filterable_impl
    }
}

/// Returns true if any attribute in the slice is `#[serde(... rename_all = "...")]`.
/// Used by `myko_subtype` to skip its default camelCase rename when the user
/// already wrote a different one (e.g. `snake_case` for enum variants).
fn attrs_have_serde_rename_all(attrs: &[syn::Attribute]) -> bool {
    use quote::ToTokens;
    attrs.iter().any(|a| {
        a.path().is_ident("serde") && a.to_token_stream().to_string().contains("rename_all")
    })
}

#[cfg(test)]
mod tests {
    use quote::ToTokens;

    use super::*;

    #[test]
    fn translates_myko_export_field_overrides() {
        let mut field: syn::Field = syn::parse_quote! {
            #[myko(export(type = "any", optional, nullable, rename = "wireValue"))]
            value: Option<serde_json::Value>
        };

        prepare_typegen_field(&mut field);
        let rendered = field
            .attrs
            .iter()
            .map(|attr| attr.to_token_stream().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(rendered.contains("type = \"any\""));
        assert!(rendered.contains("optional = nullable"));
        assert!(rendered.contains("rename = \"wireValue\""));
    }

    #[test]
    fn optional_fields_default_to_optional_and_nullable_exports() {
        let mut optional: syn::Field = syn::parse_quote!(value: Option<String>);
        prepare_typegen_field(&mut optional);
        let rendered = optional
            .attrs
            .iter()
            .map(|attr| attr.to_token_stream().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(rendered.contains("optional = nullable"));

        let mut required: syn::Field = syn::parse_quote!(value: String);
        prepare_typegen_field(&mut required);
        assert!(required.attrs.is_empty());
    }

    #[test]
    fn subtype_routes_derive_through_myko_and_supports_opaque_export() {
        let normal = myko_subtype_expand(
            syn::parse_quote!(),
            syn::parse_quote!(
                pub struct Normal {
                    value: uuid::Uuid,
                }
            ),
        )
        .to_string();
        assert!(normal.contains("myko :: TS"));
        assert!(normal.contains("crate = \"myko::ts_rs\""));

        let opaque = myko_subtype_expand(
            syn::parse_quote!(export(as = "unknown")),
            syn::parse_quote!(
                pub struct Opaque {
                    value: Vec<u8>,
                }
            ),
        )
        .to_string();
        assert!(opaque.contains("myko :: impl_ts_as ! (Opaque , \"unknown\")"));
        assert!(!opaque.contains(", myko :: TS"));
    }
}
