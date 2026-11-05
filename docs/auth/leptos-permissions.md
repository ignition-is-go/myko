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

## Inspect access

The entity explorer shows read, write, and delete scopes with their associated
operations and parent relationships. Relationships form a graph. An entity can
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
expected policy revision and an audit reason. The server rechecks authorization,
references, and the last effective administrator before committing.

A stale edit fails instead of overwriting a newer policy. Refresh the structure
before preparing another edit. Permission changes revoke existing sessions and can
disconnect the editing client before its success response arrives. Reconnect and
inspect the accepted policy before retrying an uncertain edit.

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
