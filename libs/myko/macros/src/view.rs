use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, ExprLit, ExprPath, Ident, ItemStruct, Lit, Meta, MetaNameValue, Path, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

pub struct ViewArgs {
    pub item_type: Path,
}

impl Parse for ViewArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_type: Path = input.parse()?;
        Ok(Self { item_type })
    }
}

fn parse_path_expr(expr: Expr, key: &str) -> syn::Result<Path> {
    match expr {
        Expr::Path(ExprPath { path, .. }) => Ok(path),
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => s.parse(),
        _ => Err(syn::Error::new_spanned(
            expr,
            format!("expected path for `{key}`"),
        )),
    }
}

fn parse_ident_expr(expr: Expr, key: &str) -> syn::Result<Ident> {
    let path = parse_path_expr(expr, key)?;
    if path.segments.len() == 1 {
        Ok(path.segments.first().expect("checked len").ident.clone())
    } else {
        Err(syn::Error::new_spanned(
            path,
            format!("expected single identifier for `{key}`"),
        ))
    }
}

fn lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
    }
}

fn pluralize(s: &str) -> String {
    if s.ends_with('s') {
        format!("{s}es")
    } else if s.ends_with('y') {
        format!("{}ies", &s[..s.len().saturating_sub(1)])
    } else {
        format!("{s}s")
    }
}

fn single_ident_of_path(path: &Path, context: &str) -> syn::Result<Ident> {
    if path.segments.len() == 1 {
        Ok(path.segments.first().expect("checked len").ident.clone())
    } else {
        Err(syn::Error::new_spanned(
            path,
            format!("{context} must be a single-segment type name"),
        ))
    }
}

fn parse_kv_args(args: Punctuated<Meta, Token![,]>) -> syn::Result<Vec<(String, Expr)>> {
    let mut out = Vec::new();
    for meta in args {
        let Meta::NameValue(MetaNameValue { path, value, .. }) = meta else {
            return Err(syn::Error::new_spanned(meta, "expected key = value"));
        };
        let Some(key_ident) = path.get_ident() else {
            return Err(syn::Error::new_spanned(path, "expected identifier key"));
        };
        out.push((key_ident.to_string(), value));
    }
    Ok(out)
}

#[derive(Clone)]
struct SourceSpec {
    ty: Path,
    key: Ident,
}

#[derive(Clone)]
struct JoinSpec {
    left_ty: Path,
    left_field: Ident,
    right_ty: Path,
    right_field: Ident,
    out: Option<Ident>,
    online: Option<Path>,
    where_field: Option<Ident>,
    where_eq: Option<Path>,
}

struct TreeSpec {
    parent_param: Ident,
    parent_field: Ident,
    include_offline_param: Ident,
}

struct ViewSpec {
    output: Path,
    root: Option<Path>,
    root_out: Option<Ident>,
}

struct JoinedViewArgs {
    item_type: Path,
    root_type: Path,
    root_out: Ident,
    tree: Option<TreeSpec>,
    join_one: Vec<JoinSpec>,
    join_many: Vec<JoinSpec>,
}

fn parse_source_attr(attr: &syn::Attribute) -> syn::Result<SourceSpec> {
    struct SourceArgs {
        ty: Path,
        key: Ident,
    }
    impl Parse for SourceArgs {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let ty: Path = input.parse()?;
            let mut key = None;
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
                for (k, v) in parse_kv_args(metas)? {
                    if k == "key" {
                        key = Some(parse_ident_expr(v, "key")?);
                    }
                }
            }
            Ok(Self {
                ty,
                key: key.unwrap_or_else(|| Ident::new("id", proc_macro2::Span::call_site())),
            })
        }
    }
    let parsed = attr.parse_args::<SourceArgs>()?;
    Ok(SourceSpec {
        ty: parsed.ty,
        key: parsed.key,
    })
}

fn parse_join_attr(attr: &syn::Attribute) -> syn::Result<JoinSpec> {
    struct JoinArgs {
        left_ty: Path,
        left_field: Ident,
        right_ty: Path,
        right_field: Ident,
        out: Option<Ident>,
        online: Option<Path>,
        where_field: Option<Ident>,
        where_eq: Option<Path>,
    }
    impl Parse for JoinArgs {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let left_ty: Path = input.parse()?;
            input.parse::<Token![.]>()?;
            let left_field: Ident = input.parse()?;
            input.parse::<Token![==]>()?;
            let right_ty: Path = input.parse()?;
            input.parse::<Token![.]>()?;
            let right_field: Ident = input.parse()?;

            let mut out = None;
            let mut online = None;
            let mut where_field = None;
            let mut where_eq = None;
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
                let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
                for (k, v) in parse_kv_args(metas)? {
                    match k.as_str() {
                        "out" => out = Some(parse_ident_expr(v, "out")?),
                        "online" => online = Some(parse_path_expr(v, "online")?),
                        "where_field" => where_field = Some(parse_ident_expr(v, "where_field")?),
                        "where_eq" => where_eq = Some(parse_path_expr(v, "where_eq")?),
                        _ => {}
                    }
                }
            }
            Ok(Self {
                left_ty,
                left_field,
                right_ty,
                right_field,
                out,
                online,
                where_field,
                where_eq,
            })
        }
    }
    let parsed = attr.parse_args::<JoinArgs>()?;
    Ok(JoinSpec {
        left_ty: parsed.left_ty,
        left_field: parsed.left_field,
        right_ty: parsed.right_ty,
        right_field: parsed.right_field,
        out: parsed.out,
        online: parsed.online,
        where_field: parsed.where_field,
        where_eq: parsed.where_eq,
    })
}

fn parse_view_attr(attr: &syn::Attribute) -> syn::Result<ViewSpec> {
    let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut output = None;
    let mut root = None;
    let mut root_out = None;
    for (k, v) in parse_kv_args(metas)? {
        match k.as_str() {
            "output" => output = Some(parse_path_expr(v, "output")?),
            "root" => root = Some(parse_path_expr(v, "root")?),
            "root_out" => root_out = Some(parse_ident_expr(v, "root_out")?),
            _ => {}
        }
    }
    Ok(ViewSpec {
        output: output.ok_or_else(|| syn::Error::new(attr.span(), "missing view(output = ...)"))?,
        root,
        root_out,
    })
}

fn parse_tree_attr(attr: &syn::Attribute) -> syn::Result<TreeSpec> {
    let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut parent_param = None;
    let mut parent_field = None;
    let mut include_offline_param = None;
    for (k, v) in parse_kv_args(metas)? {
        match k.as_str() {
            "parent_param" => parent_param = Some(parse_ident_expr(v, "parent_param")?),
            "parent_field" => parent_field = Some(parse_ident_expr(v, "parent_field")?),
            "include_offline_param" => {
                include_offline_param = Some(parse_ident_expr(v, "include_offline_param")?)
            }
            _ => {}
        }
    }
    Ok(TreeSpec {
        parent_param: parent_param
            .unwrap_or_else(|| Ident::new("parent_target_id", proc_macro2::Span::call_site())),
        parent_field: parent_field
            .unwrap_or_else(|| Ident::new("parent_targets", proc_macro2::Span::call_site())),
        include_offline_param: include_offline_param
            .unwrap_or_else(|| Ident::new("include_offline", proc_macro2::Span::call_site())),
    })
}

pub fn myko_view_item_impl(input_struct: ItemStruct) -> TokenStream {
    let name = &input_struct.ident;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    quote! {
        #[derive(Debug, Clone, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
        #serde_rename_attr
        #input_struct

        #krate::register_ts_export!(#name);

        impl #krate::prelude::WithId for #name {
            fn id(&self) -> std::sync::Arc<str> {
                self.id.clone()
            }
        }

        impl #krate::prelude::AnyItem for #name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn entity_type(&self) -> &'static str {
                stringify!(#name)
            }
        }

        impl #krate::prelude::Eventable for #name {
            fn entity_name_static() -> &'static str {
                stringify!(#name)
            }
        }
    }
}

pub fn myko_view_impl(args: ViewArgs, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let item_type = args.item_type;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);

    let derives = if is_empty {
        quote! {
            #[derive(Clone, Debug, Default, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    } else {
        quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    };

    let view_registration = quote! {
        #krate::prelude::ViewRegistration {
            view_id: stringify!(#struct_name),
            view_item_type: stringify!(#item_type),
            crate_name: module_path!(),
            parse: <#struct_name as #krate::view::ViewFactory>::parse,
            cell_factory: <#struct_name as #krate::view::ViewFactory>::cell_factory,
        }
    };

    quote! {
        #derives
        #input_struct

        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #view_registration
        }

        #krate::register_ts_export!(#struct_name);

        impl #krate::prelude::ViewId for #struct_name {
            fn view_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewIdStatic for #struct_name {
            fn view_id_static() -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewItemType for #struct_name {
            type Item = #item_type;

            fn view_item_type(&self) -> std::sync::Arc<str> {
                Self::view_item_type_static()
            }

            fn view_item_type_static() -> std::sync::Arc<str> {
                stringify!(#item_type).into()
            }
        }

    }
}

pub fn myko_view_declarative_impl(mut input_struct: ItemStruct) -> TokenStream {
    let mut view_spec: Option<ViewSpec> = None;
    let mut tree_spec: Option<TreeSpec> = None;
    let mut sources: Vec<SourceSpec> = Vec::new();
    let mut join_one: Option<JoinSpec> = None;
    let mut join_many: Vec<JoinSpec> = Vec::new();
    let mut keep_attrs = Vec::new();

    for attr in &input_struct.attrs {
        let Some(attr_name) = attr.path().get_ident().map(|i| i.to_string()) else {
            keep_attrs.push(attr.clone());
            continue;
        };

        match attr_name.as_str() {
            "view" => match parse_view_attr(attr) {
                Ok(v) => view_spec = Some(v),
                Err(e) => return e.to_compile_error(),
            },
            "tree" => match parse_tree_attr(attr) {
                Ok(t) => tree_spec = Some(t),
                Err(e) => return e.to_compile_error(),
            },
            "source" => match parse_source_attr(attr) {
                Ok(s) => sources.push(s),
                Err(e) => return e.to_compile_error(),
            },
            "join_one" => match parse_join_attr(attr) {
                Ok(j) => join_one = Some(j),
                Err(e) => return e.to_compile_error(),
            },
            "join_many" => match parse_join_attr(attr) {
                Ok(j) => join_many.push(j),
                Err(e) => return e.to_compile_error(),
            },
            _ => keep_attrs.push(attr.clone()),
        }
    }
    input_struct.attrs = keep_attrs;

    let Some(view) = view_spec else {
        return syn::Error::new(
            input_struct.ident.span(),
            "missing #[view(output = ..., ...)] attribute",
        )
        .to_compile_error();
    };

    let root_type = if let Some(r) = view.root.clone() {
        r
    } else if let Some(first) = sources.first() {
        first.ty.clone()
    } else {
        return syn::Error::new(
            input_struct.ident.span(),
            "missing #[source(...)] attributes",
        )
        .to_compile_error();
    };
    let root_type_ident = match single_ident_of_path(&root_type, "root type") {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };

    let root_out = view.root_out.unwrap_or_else(|| {
        Ident::new(
            &lower_first(&root_type_ident.to_string()),
            root_type_ident.span(),
        )
    });

    let join_one = join_one
        .into_iter()
        .map(|mut j| {
            if j.out.is_none() {
                let ty_ident = single_ident_of_path(&j.right_ty, "join_one type")?;
                j.out = Some(Ident::new(
                    &lower_first(&ty_ident.to_string()),
                    ty_ident.span(),
                ));
            }
            if j.online.is_some() && j.where_field.is_none() {
                j.where_field = Some(Ident::new("status", proc_macro2::Span::call_site()));
            }
            if j.where_eq.is_none() {
                j.where_eq = j.online.clone();
            }
            Ok::<JoinSpec, syn::Error>(j)
        })
        .collect::<Result<Vec<_>, _>>();
    let join_one = match join_one {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };

    let join_many = join_many
        .into_iter()
        .map(|mut j| {
            if j.out.is_none() {
                let ty_ident = single_ident_of_path(&j.right_ty, "join_many type")?;
                j.out = Some(Ident::new(
                    &lower_first(&pluralize(&ty_ident.to_string())),
                    ty_ident.span(),
                ));
            }
            Ok::<JoinSpec, syn::Error>(j)
        })
        .collect::<Result<Vec<_>, _>>();
    let join_many = match join_many {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };

    let args = JoinedViewArgs {
        item_type: view.output,
        root_type,
        root_out,
        tree: tree_spec,
        join_one,
        join_many,
    };

    render_joined_view(args, input_struct)
}

fn render_joined_view(args: JoinedViewArgs, input_struct: ItemStruct) -> TokenStream {
    let struct_name = &input_struct.ident;
    let ctx = crate::DeriveCtx::new();
    let krate = &ctx.krate;
    let serde_path = &ctx.serde_path;
    let serde_rename_attr = ctx.serde_attr(quote!(rename_all = "camelCase"));

    let item_type = args.item_type;
    let root_type = args.root_type;
    let root_out = args.root_out;
    let tree = args.tree;
    let join_one = args.join_one;
    let join_many = args.join_many;

    let online_gate_out = join_one
        .iter()
        .find(|j| j.online.is_some())
        .and_then(|j| j.out.clone());

    let join_one_map_idents: Vec<Ident> = join_one
        .iter()
        .enumerate()
        .map(|(idx, _)| format_ident!("join_one_counts_{idx}"))
        .collect();
    let join_one_map_defs = join_one_map_idents.iter().map(|map_ident| {
        quote! {
            let #map_ident = std::sync::Arc::new(::dashmap::DashMap::<std::sync::Arc<str>, usize>::new());
        }
    });
    let join_one_store_defs = join_one.iter().enumerate().map(|(idx, j)| {
        let store_ident = format_ident!("join_one_store_{idx}");
        let right_ty = &j.right_ty;
        quote! {
            let #store_ident = view_ctx.registry().get_or_create(stringify!(#right_ty));
        }
    });
    let join_one_row_values = join_one.iter().enumerate().map(|(idx, j)| {
        let map_ident = format_ident!("join_one_counts_{idx}");
        let out = j.out.clone().expect("join_one out set");
        quote! {
            let #out = #map_ident
                .get(target_id)
                .map(|count| *count.value() > 0)
                .unwrap_or(false);
        }
    });
    let join_one_row_fields = join_one.iter().map(|j| {
        let out = j.out.clone().expect("join_one out set");
        quote! { #out: #out, }
    });
    let join_one_state_parts = join_one.iter().map(|j| {
        let out = j.out.clone().expect("join_one out set");
        quote! { format!("{}={}", stringify!(#out), #out) }
    });

    let join_many_map_idents: Vec<Ident> = join_many
        .iter()
        .enumerate()
        .map(|(idx, _)| format_ident!("join_many_by_target_{idx}"))
        .collect();
    let join_many_map_defs = join_many_map_idents.iter().zip(join_many.iter()).map(|(map_ident, j)| {
        let right_ty = &j.right_ty;
        quote! {
            let #map_ident = std::sync::Arc::new(
                ::dashmap::DashMap::<std::sync::Arc<str>, std::collections::BTreeMap<std::sync::Arc<str>, #right_ty>>::new()
            );
        }
    });
    let join_many_store_defs = join_many.iter().enumerate().map(|(idx, j)| {
        let store_ident = format_ident!("join_many_store_{idx}");
        let right_ty = &j.right_ty;
        quote! {
            let #store_ident = view_ctx.registry().get_or_create(stringify!(#right_ty));
        }
    });
    let join_many_row_values = join_many.iter().enumerate().map(|(idx, j)| {
        let map_ident = format_ident!("join_many_by_target_{idx}");
        let out = j.out.clone().expect("join_many out set");
        let right_ty = &j.right_ty;
        quote! {
            let #out = #map_ident
                .get(target_id)
                .map(|group| group.value().values().cloned().collect::<Vec<#right_ty>>())
                .unwrap_or_default();
        }
    });
    let join_many_row_fields = join_many.iter().map(|j| {
        let out = j.out.clone().expect("join_many out set");
        quote! { #out: #out, }
    });
    let join_many_state_parts = join_many.iter().map(|j| {
        let out = j.out.clone().expect("join_many out set");
        quote! { format!("{}={}", stringify!(#out), #out.len()) }
    });

    let tree_setup = if let Some(tree) = &tree {
        let parent_param = &tree.parent_param;
        let include_offline_param = &tree.include_offline_param;
        quote! {
            let parent_param = view.#parent_param.clone();
            let parent_param_for_root = parent_param.clone();
            let include_offline = view.#include_offline_param;
        }
    } else {
        quote! {}
    };

    let row_matches_expr = if let Some(tree) = &tree {
        let root_parent_field = &tree.parent_field;
        if let Some(out) = online_gate_out.clone() {
            quote! {
                let parent_matches = match &parent_param {
                    Some(parent_id) => root
                        .#root_parent_field
                        .iter()
                        .any(|parent| parent == parent_id.as_ref()),
                    None => root.#root_parent_field.is_empty(),
                };
                parent_matches && (include_offline || #out)
            }
        } else {
            quote! {
                match &parent_param {
                    Some(parent_id) => root
                        .#root_parent_field
                        .iter()
                        .any(|parent| parent == parent_id.as_ref()),
                    None => root.#root_parent_field.is_empty(),
                }
            }
        }
    } else {
        quote! { true }
    };

    let root_parent_match_log = if let Some(tree) = &tree {
        let root_parent_field = &tree.parent_field;
        quote! {
            let parent_match_count = match &parent_param_for_root {
                Some(parent_id) => roots
                    .iter()
                    .filter(|entry| {
                        entry
                            .value()
                            .#root_parent_field
                            .iter()
                            .any(|parent| parent == parent_id.as_ref())
                    })
                    .count(),
                None => roots
                    .iter()
                    .filter(|entry| entry.value().#root_parent_field.is_empty())
                    .count(),
            };
            log::trace!(
                            target: "myko_rs::core::view::builder",
                "[{}] root parent_match_count={}",
                stringify!(#struct_name),
                parent_match_count
            );
        }
    } else {
        quote! {}
    };

    let build_view_log = if let Some(tree) = &tree {
        let parent_param = &tree.parent_param;
        let include_offline_param = &tree.include_offline_param;
        quote! {
            log::trace!(
                            target: "myko_rs::core::view::builder",
                "[{}] build_view start parent={:?} include_offline={}",
                stringify!(#struct_name),
                view.#parent_param,
                view.#include_offline_param
            );
        }
    } else {
        quote! {
            log::trace!(
                            target: "myko_rs::core::view::builder",
                "[{}] build_view start",
                stringify!(#struct_name)
            );
        }
    };

    let join_one_guards = join_one.iter().enumerate().map(|(idx, j)| {
        let guard_ident = format_ident!("join_one_guard_{idx}");
        let map_ident = format_ident!("join_one_counts_{idx}");
        let store_ident = format_ident!("join_one_store_{idx}");
        let right_ty = &j.right_ty;
        let right_field = &j.right_field;
        let qualifies_item = if let (Some(where_field), Some(where_eq)) = (&j.where_field, &j.where_eq) {
            quote! { item.#where_field == #where_eq }
        } else {
            quote! { true }
        };
        let qualifies_old = if let (Some(where_field), Some(where_eq)) = (&j.where_field, &j.where_eq) {
            quote! { old_item.#where_field == #where_eq }
        } else {
            quote! { true }
        };
        let qualifies_new = if let (Some(where_field), Some(where_eq)) = (&j.where_field, &j.where_eq) {
            quote! { new_item.#where_field == #where_eq }
        } else {
            quote! { true }
        };
        quote! {
            let #guard_ident = {
                let #map_ident = #map_ident.clone();
                let roots = roots.clone();
                let recompute_row = recompute_row.clone();
                #store_ident.subscribe_diffs(move |diff| match diff {
                    #krate::hypha::MapDiff::Initial { entries } => {
                        log::trace!(
                            target: "myko_rs::core::view::builder",
                            "[{}] join_one[{}] initial entries={}",
                            stringify!(#struct_name),
                            #idx,
                            entries.len()
                        );
                        #map_ident.clear();
                        for (_, item_any) in entries {
                            if let Some(item) = item_any.as_any().downcast_ref::<#right_ty>()
                                && (#qualifies_item)
                            {
                                let target_id: std::sync::Arc<str> =
                                    std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                let next = #map_ident
                                    .get(&target_id)
                                    .map(|count| count.value().saturating_add(1))
                                    .unwrap_or(1);
                                #map_ident.insert(target_id, next);
                            }
                        }
                        let impacted: Vec<std::sync::Arc<str>> =
                            #map_ident.iter().map(|entry| entry.key().clone()).collect();
                        let overlap = impacted
                            .iter()
                            .filter(|id| roots.contains_key(*id))
                            .count();
                        let joined_sample: Vec<String> = impacted
                            .iter()
                            .take(8)
                            .map(|id| id.to_string())
                            .collect();
                        let non_overlap_sample: Vec<String> = impacted
                            .iter()
                            .filter(|id| !roots.contains_key(*id))
                            .take(8)
                            .map(|id| id.to_string())
                            .collect();
                        let root_sample: Vec<String> = roots
                            .iter()
                            .take(8)
                            .map(|entry| entry.key().to_string())
                            .collect();
                        log::trace!(
                            target: "myko_rs::core::view::builder",
                            "[{}] join_one[{}] matched_targets={} overlap_with_roots={} joined_sample={:?} non_overlap_sample={:?} root_sample={:?}",
                            stringify!(#struct_name),
                            #idx,
                            impacted.len(),
                            overlap,
                            joined_sample,
                            non_overlap_sample,
                            root_sample
                        );
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                    #krate::hypha::MapDiff::Insert { value, .. } => {
                        if let Some(item) = value.as_any().downcast_ref::<#right_ty>()
                            && (#qualifies_item)
                        {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                            let next = #map_ident
                                .get(&target_id)
                                .map(|count| count.value().saturating_add(1))
                                .unwrap_or(1);
                            #map_ident.insert(target_id.clone(), next);
                            recompute_row(&target_id);
                        }
                    }
                    #krate::hypha::MapDiff::Update { old_value, new_value, .. } => {
                        let mut impacted = Vec::new();
                        if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>()
                            && (#qualifies_old)
                        {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                            if let Some(mut count) = #map_ident.get_mut(&target_id) {
                                if *count.value() <= 1 {
                                    drop(count);
                                    #map_ident.remove(&target_id);
                                } else {
                                    *count.value_mut() -= 1;
                                }
                            }
                            impacted.push(target_id);
                        }
                        if let Some(new_item) = new_value.as_any().downcast_ref::<#right_ty>()
                            && (#qualifies_new)
                        {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(new_item.#right_field.clone());
                            let next = #map_ident
                                .get(&target_id)
                                .map(|count| count.value().saturating_add(1))
                                .unwrap_or(1);
                            #map_ident.insert(target_id.clone(), next);
                            impacted.push(target_id);
                        }
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                    #krate::hypha::MapDiff::Remove { old_value, .. } => {
                        if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>()
                            && (#qualifies_old)
                        {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                            if let Some(mut count) = #map_ident.get_mut(&target_id) {
                                if *count.value() <= 1 {
                                    drop(count);
                                    #map_ident.remove(&target_id);
                                } else {
                                    *count.value_mut() -= 1;
                                }
                            }
                            recompute_row(&target_id);
                        }
                    }
                    #krate::hypha::MapDiff::Batch { changes } => {
                        let mut impacted = std::collections::BTreeSet::<std::sync::Arc<str>>::new();
                        for change in changes {
                            match change {
                                #krate::hypha::MapDiff::Initial { entries } => {
                                    #map_ident.clear();
                                    for (_, item_any) in entries {
                                        if let Some(item) = item_any.as_any().downcast_ref::<#right_ty>()
                                            && (#qualifies_item)
                                        {
                                            let target_id: std::sync::Arc<str> =
                                                std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                            let next = #map_ident
                                                .get(&target_id)
                                                .map(|count| count.value().saturating_add(1))
                                                .unwrap_or(1);
                                            #map_ident.insert(target_id, next);
                                        }
                                    }
                                    for entry in #map_ident.iter() {
                                        impacted.insert(entry.key().clone());
                                    }
                                }
                                #krate::hypha::MapDiff::Insert { value, .. } => {
                                    if let Some(item) = value.as_any().downcast_ref::<#right_ty>()
                                        && (#qualifies_item)
                                    {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                        let next = #map_ident
                                            .get(&target_id)
                                            .map(|count| count.value().saturating_add(1))
                                            .unwrap_or(1);
                                        #map_ident.insert(target_id.clone(), next);
                                        impacted.insert(target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Update { old_value, new_value, .. } => {
                                    if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>()
                                        && (#qualifies_old)
                                    {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                                        if let Some(mut count) = #map_ident.get_mut(&target_id) {
                                            if *count.value() <= 1 {
                                                drop(count);
                                                #map_ident.remove(&target_id);
                                            } else {
                                                *count.value_mut() -= 1;
                                            }
                                        }
                                        impacted.insert(target_id);
                                    }
                                    if let Some(new_item) = new_value.as_any().downcast_ref::<#right_ty>()
                                        && (#qualifies_new)
                                    {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(new_item.#right_field.clone());
                                        let next = #map_ident
                                            .get(&target_id)
                                            .map(|count| count.value().saturating_add(1))
                                            .unwrap_or(1);
                                        #map_ident.insert(target_id.clone(), next);
                                        impacted.insert(target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Remove { old_value, .. } => {
                                    if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>()
                                        && (#qualifies_old)
                                    {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                                        if let Some(mut count) = #map_ident.get_mut(&target_id) {
                                            if *count.value() <= 1 {
                                                drop(count);
                                                #map_ident.remove(&target_id);
                                            } else {
                                                *count.value_mut() -= 1;
                                            }
                                        }
                                        impacted.insert(target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Batch { .. } => {}
                            }
                        }
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                })
            };
            output.own_guard(#guard_ident);
        }
    });

    let join_many_guards = join_many.iter().enumerate().map(|(idx, j)| {
        let guard_ident = format_ident!("join_many_guard_{idx}");
        let map_ident = format_ident!("join_many_by_target_{idx}");
        let store_ident = format_ident!("join_many_store_{idx}");
        let right_ty = &j.right_ty;
        let right_field = &j.right_field;
        quote! {
            let #guard_ident = {
                let #map_ident = #map_ident.clone();
                let roots = roots.clone();
                let recompute_row = recompute_row.clone();
                #store_ident.subscribe_diffs(move |diff| match diff {
                    #krate::hypha::MapDiff::Initial { entries } => {
                        log::trace!(
                            target: "myko_rs::core::view::builder",
                            "[{}] join_many[{}] initial entries={}",
                            stringify!(#struct_name),
                            #idx,
                            entries.len()
                        );
                        #map_ident.clear();
                        for (_, item_any) in entries {
                            if let Some(item) = item_any.as_any().downcast_ref::<#right_ty>() {
                                let target_id: std::sync::Arc<str> =
                                    std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                let item_id = item.id.clone();
                                if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                    group.insert(item_id, item.clone());
                                } else {
                                    let mut group = std::collections::BTreeMap::new();
                                    group.insert(item_id, item.clone());
                                    #map_ident.insert(target_id.clone(), group);
                                }
                            }
                        }
                        let impacted: Vec<std::sync::Arc<str>> =
                            #map_ident.iter().map(|entry| entry.key().clone()).collect();
                        let overlap = impacted
                            .iter()
                            .filter(|id| roots.contains_key(*id))
                            .count();
                        let joined_sample: Vec<String> = impacted
                            .iter()
                            .take(8)
                            .map(|id| id.to_string())
                            .collect();
                        let non_overlap_sample: Vec<String> = impacted
                            .iter()
                            .filter(|id| !roots.contains_key(*id))
                            .take(8)
                            .map(|id| id.to_string())
                            .collect();
                        let root_sample: Vec<String> = roots
                            .iter()
                            .take(8)
                            .map(|entry| entry.key().to_string())
                            .collect();
                        log::trace!(
                            target: "myko_rs::core::view::builder",
                            "[{}] join_many[{}] matched_targets={} overlap_with_roots={} joined_sample={:?} non_overlap_sample={:?} root_sample={:?}",
                            stringify!(#struct_name),
                            #idx,
                            impacted.len(),
                            overlap,
                            joined_sample,
                            non_overlap_sample,
                            root_sample
                        );
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                    #krate::hypha::MapDiff::Insert { value, .. } => {
                        if let Some(item) = value.as_any().downcast_ref::<#right_ty>() {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                            let item_id = item.id.clone();
                            if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                group.insert(item_id, item.clone());
                            } else {
                                let mut group = std::collections::BTreeMap::new();
                                group.insert(item_id, item.clone());
                                #map_ident.insert(target_id.clone(), group);
                            }
                            recompute_row(&target_id);
                        }
                    }
                    #krate::hypha::MapDiff::Update { old_value, new_value, .. } => {
                        let mut impacted = Vec::new();
                        if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>() {
                            let old_target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                            if let Some(mut group) = #map_ident.get_mut(&old_target_id) {
                                group.remove(&old_item.id);
                                let is_empty = group.is_empty();
                                drop(group);
                                if is_empty {
                                    #map_ident.remove(&old_target_id);
                                }
                            }
                            impacted.push(old_target_id);
                        }
                        if let Some(new_item) = new_value.as_any().downcast_ref::<#right_ty>() {
                            let new_target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(new_item.#right_field.clone());
                            let new_item_id = new_item.id.clone();
                            if let Some(mut group) = #map_ident.get_mut(&new_target_id) {
                                group.insert(new_item_id, new_item.clone());
                            } else {
                                let mut group = std::collections::BTreeMap::new();
                                group.insert(new_item_id, new_item.clone());
                                #map_ident.insert(new_target_id.clone(), group);
                            }
                            impacted.push(new_target_id);
                        }
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                    #krate::hypha::MapDiff::Remove { old_value, .. } => {
                        if let Some(item) = old_value.as_any().downcast_ref::<#right_ty>() {
                            let target_id: std::sync::Arc<str> =
                                std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                            if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                group.remove(&item.id);
                                let is_empty = group.is_empty();
                                drop(group);
                                if is_empty {
                                    #map_ident.remove(&target_id);
                                }
                            }
                            recompute_row(&target_id);
                        }
                    }
                    #krate::hypha::MapDiff::Batch { changes } => {
                        let mut impacted = std::collections::BTreeSet::<std::sync::Arc<str>>::new();
                        for change in changes {
                            match change {
                                #krate::hypha::MapDiff::Initial { entries } => {
                                    #map_ident.clear();
                                    for (_, item_any) in entries {
                                        if let Some(item) = item_any.as_any().downcast_ref::<#right_ty>() {
                                            let target_id: std::sync::Arc<str> =
                                                std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                            let item_id = item.id.clone();
                                            if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                                group.insert(item_id, item.clone());
                                            } else {
                                                let mut group = std::collections::BTreeMap::new();
                                                group.insert(item_id, item.clone());
                                                #map_ident.insert(target_id.clone(), group);
                                            }
                                        }
                                    }
                                    for entry in #map_ident.iter() {
                                        impacted.insert(entry.key().clone());
                                    }
                                }
                                #krate::hypha::MapDiff::Insert { value, .. } => {
                                    if let Some(item) = value.as_any().downcast_ref::<#right_ty>() {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                        let item_id = item.id.clone();
                                        if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                            group.insert(item_id, item.clone());
                                        } else {
                                            let mut group = std::collections::BTreeMap::new();
                                            group.insert(item_id, item.clone());
                                            #map_ident.insert(target_id.clone(), group);
                                        }
                                        impacted.insert(target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Update { old_value, new_value, .. } => {
                                    if let Some(old_item) = old_value.as_any().downcast_ref::<#right_ty>() {
                                        let old_target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(old_item.#right_field.clone());
                                        if let Some(mut group) = #map_ident.get_mut(&old_target_id) {
                                            group.remove(&old_item.id);
                                            let is_empty = group.is_empty();
                                            drop(group);
                                            if is_empty {
                                                #map_ident.remove(&old_target_id);
                                            }
                                        }
                                        impacted.insert(old_target_id);
                                    }
                                    if let Some(new_item) = new_value.as_any().downcast_ref::<#right_ty>() {
                                        let new_target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(new_item.#right_field.clone());
                                        let new_item_id = new_item.id.clone();
                                        if let Some(mut group) = #map_ident.get_mut(&new_target_id) {
                                            group.insert(new_item_id, new_item.clone());
                                        } else {
                                            let mut group = std::collections::BTreeMap::new();
                                            group.insert(new_item_id, new_item.clone());
                                            #map_ident.insert(new_target_id.clone(), group);
                                        }
                                        impacted.insert(new_target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Remove { old_value, .. } => {
                                    if let Some(item) = old_value.as_any().downcast_ref::<#right_ty>() {
                                        let target_id: std::sync::Arc<str> =
                                            std::convert::Into::<std::sync::Arc<str>>::into(item.#right_field.clone());
                                        if let Some(mut group) = #map_ident.get_mut(&target_id) {
                                            group.remove(&item.id);
                                            let is_empty = group.is_empty();
                                            drop(group);
                                            if is_empty {
                                                #map_ident.remove(&target_id);
                                            }
                                        }
                                        impacted.insert(target_id);
                                    }
                                }
                                #krate::hypha::MapDiff::Batch { .. } => {}
                            }
                        }
                        for id in impacted {
                            recompute_row(&id);
                        }
                    }
                })
            };
            output.own_guard(#guard_ident);
        }
    });

    let is_empty = matches!(&input_struct.fields, syn::Fields::Named(f) if f.named.is_empty())
        || matches!(&input_struct.fields, syn::Fields::Unit);
    let derives = if is_empty {
        quote! {
            #[derive(Clone, Debug, Default, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    } else {
        quote! {
            #[derive(Clone, Debug, #serde_path::Serialize, #serde_path::Deserialize, #krate::TS)]
            #serde_rename_attr
        }
    };

    let view_registration = quote! {
        #krate::prelude::ViewRegistration {
            view_id: stringify!(#struct_name),
            view_item_type: stringify!(#item_type),
            crate_name: module_path!(),
            parse: <#struct_name as #krate::view::ViewFactory>::parse,
            cell_factory: <#struct_name as #krate::view::ViewFactory>::cell_factory,
        }
    };

    quote! {
        #derives
        #input_struct

        #[cfg(not(target_arch = "wasm32"))]
        #krate::submit! {
            #view_registration
        }

        #krate::register_ts_export!(#struct_name);

        impl #krate::prelude::ViewId for #struct_name {
            fn view_id(&self) -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewIdStatic for #struct_name {
            fn view_id_static() -> std::sync::Arc<str> {
                stringify!(#struct_name).into()
            }
        }

        impl #krate::prelude::ViewItemType for #struct_name {
            type Item = #item_type;

            fn view_item_type(&self) -> std::sync::Arc<str> {
                Self::view_item_type_static()
            }

            fn view_item_type_static() -> std::sync::Arc<str> {
                stringify!(#item_type).into()
            }
        }

        impl #krate::prelude::ViewHandler for #struct_name {
            fn build_cell(
                ctx: #krate::prelude::ViewBuildCellCtx<Self>,
            ) -> #krate::prelude::TypedViewCellMap<Self::Item> {
                let view = ctx.view.as_ref();
                let view_ctx = &ctx.view_context;
                #build_view_log

                let output =
                    #krate::hypha::CellMap::<std::sync::Arc<str>, #item_type>::new()
                        .with_name(stringify!(#struct_name));
                let roots = std::sync::Arc::new(
                    ::dashmap::DashMap::<std::sync::Arc<str>, #root_type>::new()
                );
                #(#join_one_map_defs)*
                #(#join_many_map_defs)*

                let root_store = view_ctx.registry().get_or_create(stringify!(#root_type));
                #(#join_one_store_defs)*
                #(#join_many_store_defs)*
                #tree_setup

                let recompute_row = {
                    let output = output.clone();
                    let roots = roots.clone();
                    #(let #join_one_map_idents = #join_one_map_idents.clone();)*
                    #(let #join_many_map_idents = #join_many_map_idents.clone();)*
                    move |target_id: &std::sync::Arc<str>| {
                        let Some(root_ref) = roots.get(target_id) else {
                            output.remove(target_id);
                            return;
                        };
                        let root = root_ref.value().clone();
                        drop(root_ref);

                        #(#join_one_row_values)*
                        let join_one_state_parts: Vec<String> = vec![#(#join_one_state_parts),*];
                        let join_one_state = join_one_state_parts.join(", ");
                        let matches = {
                            #row_matches_expr
                        };
                        if !matches {
                            log::trace!(
                                target: "myko_rs::core::view::builder",
                                "[{}] filtered target={} join_one=[{}]",
                                stringify!(#struct_name),
                                target_id,
                                join_one_state
                            );
                            output.remove(target_id);
                            return;
                        }

                        #(#join_many_row_values)*
                        let join_many_state_parts: Vec<String> = vec![#(#join_many_state_parts),*];
                        let join_many_state = join_many_state_parts.join(", ");

                        let row = #item_type {
                            id: root.id.clone(),
                            hash: root.hash.clone(),
                            #root_out: root,
                            #(#join_many_row_fields)*
                            #(#join_one_row_fields)*
                        };
                        output.insert(target_id.clone(), row);
                        log::trace!(
                            target: "myko_rs::core::view::builder",
                            "[{}] row upsert target={} join_one=[{}] join_many=[{}]",
                            stringify!(#struct_name),
                            target_id,
                            join_one_state,
                            join_many_state
                        );
                    }
                };

                let root_guard = {
                    let roots = roots.clone();
                    let output = output.clone();
                    let recompute_row = recompute_row.clone();
                    root_store.subscribe_diffs(move |diff| match diff {
                        #krate::hypha::MapDiff::Initial { entries } => {
                            log::trace!(
                            target: "myko_rs::core::view::builder",
                                "[{}] root initial entries={}",
                                stringify!(#struct_name),
                                entries.len()
                            );
                            roots.clear();
                            for (_id, item_any) in entries {
                                if let Some(root) = item_any.as_any().downcast_ref::<#root_type>() {
                                    roots.insert(root.id.clone(), root.clone());
                                }
                            }
                            let root_id_sample: Vec<String> = roots
                                .iter()
                                .take(12)
                                .map(|entry| entry.key().to_string())
                                .collect();
                            log::trace!(
                            target: "myko_rs::core::view::builder",
                                "[{}] root sample={:?}",
                                stringify!(#struct_name),
                                root_id_sample
                            );
                            #root_parent_match_log
                            let ids: Vec<std::sync::Arc<str>> =
                                roots.iter().map(|entry| entry.key().clone()).collect();
                            for id in ids {
                                recompute_row(&id);
                            }
                        }
                        #krate::hypha::MapDiff::Insert { value, .. }
                        | #krate::hypha::MapDiff::Update {
                            old_value: _,
                            key: _,
                            new_value: value,
                            ..
                        } => {
                            if let Some(root) = value.as_any().downcast_ref::<#root_type>() {
                                let target_id = root.id.clone();
                                roots.insert(target_id.clone(), root.clone());
                                recompute_row(&target_id);
                            }
                        }
                        #krate::hypha::MapDiff::Remove { key, old_value } => {
                            if let Some(root) = old_value.as_any().downcast_ref::<#root_type>() {
                                let target_id = root.id.clone();
                                log::trace!(
                                    target: "myko_rs::core::view::builder",
                                    "[{}] root remove key={} target_id={}",
                                    stringify!(#struct_name),
                                    key,
                                    target_id
                                );
                                roots.remove(&target_id);
                                output.remove(&target_id);
                            } else {
                                log::trace!(
                                    target: "myko_rs::core::view::builder",
                                    "[{}] root remove key={} (fallback)",
                                    stringify!(#struct_name),
                                    key
                                );
                                roots.remove(key);
                                output.remove(key);
                            }
                        }
                        #krate::hypha::MapDiff::Batch { changes } => {
                            let mut impacted = std::collections::BTreeSet::<std::sync::Arc<str>>::new();
                            for change in changes {
                                match change {
                                    #krate::hypha::MapDiff::Initial { entries } => {
                                        let previous_ids: Vec<std::sync::Arc<str>> =
                                            roots.iter().map(|entry| entry.key().clone()).collect();
                                        roots.clear();
                                        for (_id, item_any) in entries {
                                            if let Some(root) = item_any.as_any().downcast_ref::<#root_type>() {
                                                roots.insert(root.id.clone(), root.clone());
                                            }
                                        }
                                        for id in previous_ids {
                                            impacted.insert(id);
                                        }
                                        for entry in roots.iter() {
                                            impacted.insert(entry.key().clone());
                                        }
                                    }
                                    #krate::hypha::MapDiff::Insert { value, .. }
                                    | #krate::hypha::MapDiff::Update {
                                        old_value: _,
                                        key: _,
                                        new_value: value,
                                        ..
                                    } => {
                                        if let Some(root) = value.as_any().downcast_ref::<#root_type>() {
                                            let target_id = root.id.clone();
                                            roots.insert(target_id.clone(), root.clone());
                                            impacted.insert(target_id);
                                        }
                                    }
                                    #krate::hypha::MapDiff::Remove { key, old_value } => {
                                        if let Some(root) = old_value.as_any().downcast_ref::<#root_type>() {
                                            let target_id = root.id.clone();
                                            roots.remove(&target_id);
                                            impacted.insert(target_id);
                                        } else {
                                            roots.remove(key);
                                            impacted.insert(key.clone());
                                        }
                                    }
                                    #krate::hypha::MapDiff::Batch { .. } => {}
                                }
                            }
                            for id in impacted {
                                recompute_row(&id);
                            }
                        }
                    })
                };
                output.own_guard(root_guard);
                #(#join_one_guards)*
                #(#join_many_guards)*

                output.lock()
            }
        }
    }
}
