# Render a server's permissions in Leptos

The permission UI consumes the connected server's catalog and standard permission
records. Your app does not supply an entity list or compile its application entity
types into the permission components.

The server describes its generated entity scopes, inheritance relationships, and
operation requirements. It also reports the caller's policy permissions for administration operations.
The UI uses those descriptions to render application-specific permissions.

## Connect the components

Add the browser components without server dependencies.

```toml
[dependencies]
myko-permissions-leptos = "6.11.6"
myko-leptos = "6.11.6"
leptos = { version = "0.8", features = ["csr"] }
```

Provide your authenticated `MykoClient` in Leptos context before calling
`use_permissions()`. Set its credential provider before its address so the first
connection authenticates. The app continues to own login and token renewal.

```rust,ignore
use leptos::prelude::*;
use myko_permissions_leptos::{
    PermissionExplorer, PermissionMutationStatus, PrincipalAccessInspector, use_permissions,
};

#[component]
fn PermissionsPage() -> impl IntoView {
    let permissions = use_permissions();
    view! {
        <PermissionExplorer structure=permissions.structure/>
        <PrincipalAccessInspector inspection=permissions.inspection/>
        <PermissionMutationStatus state=Signal::from(permissions.mutation)/>
    }
}
```

Set `permissions.selected_principal` to a selected ID from the discovered
permission document to request its local access explanation.

For each discovered role, render a `RoleEditor` with `structure`, `role`, and
`on_save=permissions.save_role`. Render `GroupMembershipEditor` with `structure`,
a discovered `group_id`, and `on_save=permissions.save_membership`. Omit `on_save`
for read-only editors. Display `permissions.mutation` alongside those controls to
show pending, saved, error, and uncertain outcomes.

The native server links `myko-permissions` with its `server` feature and attaches
`Permissions` as described in [Persisted permissions](permissions.md). Its
`GetPermissionStructure` and `InspectPermissionPrincipal` reports use the ordinary
Myko connection. The Leptos crate depends on the browser-safe contract with server
features disabled.

## Supply your own markup and styles

`use_permissions()` owns the connection and mutation callbacks without rendering
DOM. The matrix helpers and editor controllers are headless too. They do not
insert elements, require CSS classes, or choose your app's controls.

`permission_matrix(&structure.entities)` returns one row per entity with optional
`read`, `write`, and `delete` scope cells. `inspection_matrix(&inspection.scopes)`
groups the server's access explanations into the same layout. A missing cell means
the server did not describe that action. It does not mean Deny or Inherit.

Use `use_role_editor` and `use_group_membership_editor` to keep the default editing
behavior while rendering your own forms. For example, connect a headless role
editor to the existing mutation callback.

```rust,ignore
let editor = use_role_editor(
    permissions.structure,
    Signal::stored(role_id),
    Some(permissions.save_role),
);
```

The controller's `rows()`, `role()`, and `rule(scope)` methods expose the draft.
Bind your own controls to `set_rule` and `remove_obsolete`. Your app chooses every
element and class, including the save button. No reason input is required.

```rust,ignore
let error = RwSignal::new(None::<String>);
view! {
    <button class="app-primary-button"
        disabled=move || !editor.can_submit()
        on:click=move |_| error.set(editor.submit().err().map(|value| value.to_string()))>
        "Save role"
    </button>
    <p role="alert">{move || error.get()}</p>
}
```

The controller checks the current revision, command capability, and target before
calling `on_save`. It supplies an audit description automatically. Apps can set
`editor.reason` programmatically to add context, but a blank value uses the default
description. If the policy revision changes, reload the editor before submitting
again. Denied, disconnected, unsupported, and deleted-target
states block edits.

The default `PermissionExplorer` and `RoleEditor` arrange entity rows in an
expanded inheritance tree, with aligned Read, Write, and Delete columns.
`PrincipalAccessInspector` keeps the same columns for reported decisions. These
views use the headless APIs and ship no stylesheet. Apply app CSS to their semantic HTML, or
replace their markup entirely. Obsolete role scopes remain available for cleanup
without inventing additional action columns.

## Build users and roles pages

Use the headless administration helpers for application-styled lists and pickers:

- `use_permission_users` searches human users by name or role;
  `use_permission_roles` and `use_permission_groups` list roles and groups alphabetically.
- `use_grantee_roles` returns direct and inherited assignments for a user or group,
  including provenance, active state, and an optional removable direct binding ID.
- `use_assignable_roles`, `use_role_users`, and `use_assignable_users` drive role
  and user pickers. Inherited roles may also be assigned directly. Removing a
  direct binding does not remove a group-inherited assignment.
- `use_permission_reference` supplies entity and operation metadata for searchable
  scope browsers. `permission_explanation_text` formats server-computed decisions.
- `use_role_rule_values` indexes draft rules once; `use_role_permission_rows`
  computes a filtered hierarchy only when entity metadata or search changes.

Create one batch action at page scope so its pending state and completion outlive
individual list rows. It uses the latest advertised policy revision and reports
errors through `permissions.mutation`.

```rust,ignore
let (can_edit, apply) = use_permission_changes(permissions, "Access administration");
let grantee = Signal::stored(PermissionGrantee::Principal(principal_id));
let assigned = use_grantee_roles(permissions, grantee);
let available = use_assignable_roles(permissions, grantee);
// In an assignment action:
apply.run(vec![assign_role(role_id, grantee.get_untracked())]);
// In a remove action, use a direct binding ID returned in assigned:
apply.run(vec![PermissionChange::DeleteRoleBinding(binding_id)]);
```

`create_role(name)` and `create_group(name)` construct enabled records without
additional grants; submit them through the same action. These helpers provide no
local authorization bypass. The server validates each change and its references.

`use_permission_list_search(rows, search, text)` adds trimmed, case-insensitive
search to any list or picker. Apps supply the searchable text for each row.
Filtering preserves the source rows and editor drafts, and follows live updates.

All list helpers return memos so equal data stays stable across unrelated policy
revisions. Apps own table markup, buttons, popovers, navigation, and styling. Keep
large permission grids behind an explicit selection or inspection action.

## Arrange entities as a tree

Call `permission_tree(&permission_matrix(&structure.entities))` for headless tree
rows. Each row includes its depth, display parent, ancestors, and whether it has
displayed children. Your app can choose indentation, branch labels, and optional
collapse behavior. `visible_permission_tree(&rows, &collapsed_entities)` filters
display descendants without changing permission values or editor drafts.

For the default components, put indentation in your app stylesheet.

```css
.myko-permission-tree-label,
tr[data-depth] th[scope="row"] small {
  display: block;
  padding-inline-start: calc(var(--myko-permission-depth, 0) * 1.25rem);
}
```

The default tree shows each entity once. Multiple-parent entities retain all real
parents in their metadata, even though one branch determines their display
position. The display parent does not change authorization precedence. Cycles and
unavailable parent entities do not hide rows or cause endless recursion.

## Inspect access

The entity explorer groups each display parent above its displayed descendants
and keeps separate
Read, Write, and Delete columns, with associated operations and parent metadata. Relationships form a graph. An entity can
have several parents, and a cycle must not cause recursive rendering to hang.

The principal inspector shows server-computed local grant decisions. Explanations
identify the rules and inherited scopes that determine each result. The browser
does not calculate authorization independently.

Local grant inspection does not prove that an account can authenticate, that a
future token contains the required OAuth scopes, or that a delegated token permits
the operation. Row-level policies remain outside this operation-permission model.

## Edit permissions

Role rules have three settings. **Allow** grants the scope, **Deny** rejects it,
and **Inherit** removes that role's explicit rule. Other roles and overrides still
participate in the effective result.

Membership edits change which groups contribute rules to a principal. Both role
and membership edits use the existing protected administration commands with an
expected policy revision and an automatically supplied audit description. Users
do not need to explain each edit. The server rechecks authorization,
references, and the last effective administrator before committing.

A stale edit fails instead of overwriting a newer policy. Refresh the structure
before preparing another edit. With `MykoAuthProvider::with_live_permissions()`,
policy edits reauthorize operations without disconnecting the socket. Without
that mode, edits can revoke sessions before their success response arrives.
Reconnect and inspect the accepted policy before retrying an uncertain edit.

## Keep inspection authorized

The standard full structure contains every permission record type. The server
requires read permission for all seven permission entity types, including explicit
child denials. A denied request does not return the permission document.
Identity-sensitive inspection uses caller-specific report caching. Direct handler
calls repeat authorization checks.

Client capabilities describe policy discovery permission. They exclude transport
ceilings and checks that depend on command arguments. If your app uses a read-only
gateway, omit editor `on_save` callbacks. Capabilities do not
replace server enforcement, and a concurrent policy change can invalidate them.
A server using a custom authorizer must explicitly expose supported inspection
and editing. Unsupported inspection must not appear as an empty permission store.

See [Persisted permissions](permissions.md) for server setup, bootstrap, and
host-only recovery.

## Verify the components

Run the focused suite through Flux.

```sh
cargo flux run verify-permissions-ui
```

Build the disposable browser demo with `cargo flux run build-permissions-ui-demo`.
Serve `target/permissions-ui-demo` with a local static file server. The demo edits
browser memory only. Its fixed access explanation illustrates the component's
rendering and does not claim to evaluate live policy. The integration tests cover
server-computed explanations and authenticated report transport separately.
