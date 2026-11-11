# Persisted permissions

`myko-permissions` supplies permission entities, protected administration commands,
and a local `ScopePolicy`. Apps can use it with any `Authenticator`, including
`myko-auth0`, or keep their own policy implementation.

## Connect an app

Create the policy before the server, then attach it after server initialization.
The fallible hook attaches after normal event replay and aborts startup if the
permission entities are invalid. Configure the normal Myko persister (for example
Postgres) on the server; permissions require no separate database or journal.

```rust,ignore
use std::sync::Arc;
use myko_permissions::{CredentialScopeMode, Permissions};
use myko_server::MykoServer;

let permissions = Arc::new(Permissions::from_entities(CredentialScopeMode::LocalGrants));
let provider = Arc::new(permissions.auth_provider(authenticator)
    .with_user_discovery()
    .with_live_permissions()
    .build());
let initialize = permissions.clone();
let server = MykoServer::builder()
    .with_auth_provider(Some(provider))
    .try_after_init(move |server| {
        initialize.attach(Arc::new(server.ctx()))?;
        Ok(())
    })
    .build();
server.run().await?;
```

Entity-backed authorization reads a materialized reactive value built from normal
queries over permission principals, groups, memberships, roles, bindings and
overrides. Edits use the server's normal mixed mutation batch, so the same
persister, event log, replay and subscription machinery apply. There is no polling
thread or separately loaded policy snapshot on this path. Local revision checks
serialize administration; multi-server write concurrency follows Myko's ordinary
semantics and is not strengthened by this package.

For an existing standalone policy, call `permissions.import_from(&legacy_storage)`
after attaching and before accepting connections. It imports grants only into an
empty authority, preserves entity IDs and assignments, and leaves the source
untouched. Once entity state exists, the old store is never consulted. File and
memory storage adapters remain available for standalone/operator workflows and
legacy consumers; they are not needed by an entity-backed application.

`authenticator` verifies credentials and supplies an issuer, subject, principal
kind, and expiry. Users, services, and agents share the permission model. The
standard policy rejects delegated identity chains; apps needing delegation must
supply a policy that interprets its verified constraints.

Choose the credential mode explicitly. `LocalGrants` uses the verified token for
identity and uses local permissions for grants. `TokenScopes` additionally requires
the verified token to contain each candidate scope used to grant access. A token
cannot supply a local grant by itself.

Authentication does not call Auth0 for every command. Authorization reads an
in-memory snapshot. A background observer refreshes storage every 100 milliseconds;
permission administration also loads the latest accepted revision before editing.
Structure and principal-inspection reports follow the authority projection and
update on each accepted policy revision without requiring a new subscription.
By default, a policy change invalidates existing sessions. Applications can opt
into `MykoAuthProvider::with_live_permissions()` to keep connections across role
and grant edits. The WebSocket transport rechecks subscriptions on policy changes
and before delivering queued query, view, or report data. Subscriptions that lose
access stop and receive a typed error; permitted subscriptions stay active.
New operations immediately use the latest grants. A denied subscription can be
requested again with a fresh transaction after access is restored. Credential
expiry, logout, and explicit session revocation still close the connection.
MCP query, view, and report reads are reauthorized before returning results.
Custom transports enabling this option must enforce equivalent delivery checks.

## Bootstrap and recover

An empty store denies access. It never makes the first client an administrator.
Applications may opt in to email-based administrator reconciliation by wrapping
an existing authenticator. The application owns configuration loading; Myko does
not read environment variables or select a default email.

```rust,ignore
use myko_permissions::AdminEmail;

let provider = permissions.auth_provider(authenticator)
    .with_admin_email(config.admin_email.map(AdminEmail::new).transpose()?)
    .with_user_discovery()
    .with_live_permissions()
    .build();
```

The wrapped authenticator must supply a trusted `verified_email` string claim.
Only non-delegated user identities with that verified address qualify. Expiry and
credential liveness are checked before any grant is written. On each matching
sign-in, the helper atomically bootstraps or reconciles an enabled principal,
a normal persisted `Admin` role with allow rules for every registered entity's
read/write/delete scopes, and that principal's role binding. Password and OTP
identities with the same trusted verified email can receive the same role.
Unchanged sign-ins do not write another revision. Other people can receive this
role through normal administration. Explicit overrides retain their normal
precedence; this is a role assignment, not an authorization bypass.

Removing the configuration stops automatic grants and restoration, but does not
remove persisted roles or bindings. Legacy bootstrap role IDs and assignments
are retained when the helper adopts them. Updates are audited and use storage
compare-and-swap retries. The helper refreshes the attached runtime before
returning the identity, so its new access is visible on admission.

### Discover signed-in users

Add `.with_user_discovery()` to `PermissionAuthenticator` to record authenticated,
non-delegated users in an initialized authority. Discovery does not bootstrap an
empty authority, grant roles, or re-enable an existing principal. The trusted
`verified_email` claim supplies the display name when present; otherwise the
provider subject is used. Concurrent admission is deduplicated by issuer and
subject, with storage compare-and-swap retries. The attached runtime publishes
the committed principal before authentication returns.

### Enforce administration separately from application policy

Applications adopting permissions in stages can compose their existing policy:

```rust,ignore
let authorizer = permissions.administration_authorizer(application_authorizer);
```

The same selection is available on the provider builder through
`.with_application_policy(application_authorizer)`. Without that explicit
fallback, the builder uses `ScopedAuthorizer` for all registered entity scopes.
`myko_auth::AuthenticatedAccess` is an opt-in fallback allowing authenticated
application operations and explicitly public anonymous operations; it is not a
role-enforcing policy.

This enforces persisted grants on operations whose registered requirements touch
permission entities, and on raw events for those entities. All other requests
are delegated to the supplied application authorizer, including anonymous public
operations and command results. Anonymous callers cannot access administration
even if the fallback is permissive. Changes to either policy's revision are
visible to transport authorization checks. Use `ScopedAuthorizer` directly when
persisted grants should govern all application entity operations.

For host-only initialization without email reconciliation, run the operator CLI
before first startup, using the exact verified
issuer, subject, and principal kind.

```sh
cargo run -p myko-permissions --bin myko-permissions -- bootstrap \
  --store /var/lib/app/permissions/policy.jsonl \
  --issuer https://tenant.auth0.com/ --subject 'auth0|operator-id' \
  --kind user --operator deployment-admin --reason 'Initial administrator'
```

Use `recover` with the same options to restore a selected administrator in an
initialized store. Recovery works without application credentials. It enables the
selected principal, removes its memberships, direct role bindings, and direct
overrides, then installs a full permission administration role. Repeated recovery
reuses that recovery role and binding.

Host access to the private journal is the recovery authority. The operator label
is an audit label supplied by that operator, not a separately authenticated user.
Keep this path available outside the app's normal authorization configuration.

`show --store PATH` prints the accepted snapshot. `replace` accepts a
`PermissionDocument` JSON file with `--document PATH --expected-revision N`, plus
`--operator` and `--reason`. Replacement normally preserves an administrator.
Only host-only replacement accepts `--allow-lockout` for deliberate lockout.
Bootstrap, recovery, and replacement are library functions too. They are never
registered as WebSocket or MCP commands.

## Administer permissions

The canonical model contains principals, groups, memberships, roles, role bindings,
and overrides. Principals match verified issuer, subject, and user/service/agent
kind. Bind roles to a principal or group. Permission records belong to their authority;
role and grantee references do not introduce ownership or permission inheritance.
Principal management does not implicitly authorize role assignment.
Rules hold a generated scope such as
`Project:write` and an `Allow` or `Deny` effect.

For one entity scope, principal overrides take precedence over group overrides,
which take precedence over role rules. Deny wins ties within a tier. If the scope
is unset, normal entity relationship inheritance applies. Thus `Project:write`
can grant `Camera:write`, while an explicit camera rule overrides inheritance.
These are operation permissions. They do not isolate Project A's rows from
Project B's rows. A future row policy can use the authorization request context.

Use `UpsertPermissionPrincipal`, `UpsertPermissionGroup`,
`UpsertPermissionMembership`, `UpsertPermissionRole`,
`UpsertPermissionRoleBinding`, and `UpsertPermissionOverride`. Each has a matching
`RemovePermission…` command. Every command takes `expected_revision` and `reason`,
plus its row or ID. The default Leptos editors supply an audit description
automatically without asking users to type one. `ApplyPermissionChanges` accepts a
`changes` vector for an atomic edit involving several rows. Deletes do not cascade; remove references
in the same batch.

Each command requires its generated entity write or delete grant. Batch edits
require authority write and delete, plus every affected entity grant. All edits
are checked again against the latest policy, even for in-process command calls.
The app does not maintain a second command-to-scope list.

Ordinary edits must leave at least one enabled principal with effective read,
write, and delete grants for every permission entity. This includes grants through
groups, roles, inheritance, and overrides. The guard proves local permission
reachability. It cannot prove that the corresponding external account still
exists or can authenticate; host recovery handles that failure.

Direct client SET/DEL events and generated deletion commands cannot mutate
permission entities, even for administrators. Use the protected commands.
Without live permission mode, a successful edit can invalidate the caller's
session before its response is sent. Reconnect and inspect the revision and change ID before retrying an edit.

## Persistence and reactive stores

One `PermissionAuthority` snapshot is authoritative. The ordinary Myko entity
stores are reactive read projections of its typed rows. A whole snapshot makes
an administration edit and its last-administrator check one conditional commit.
The snapshot records revision, change ID, actor, reason, mode, and full document.

`PermissionStorage` requires an atomic, durable compare-and-swap. The supplied
`FilePermissionStorage` uses a private append-only journal and operating-system
file locks. Accepted writes synchronize the file and, on Unix, its directory.
Use a local filesystem with working locks. Existing files must be private;
symlinks, shared permissions, and hardlinks are rejected. Interrupted trailing
records are discarded; corruption in committed records fails closed.

The journal retains complete historical snapshots, so protect old permission
information too. Loading scans its history. `history()` rejects more than 4096
records; this limit does not cap journal growth. Each record is limited to 16 MiB.
Plan a storage adapter or an operational retention strategy for large deployments.
`MemoryPermissionStorage` is explicitly ephemeral and suits tests.

This storage does not automatically use Myko's ordinary event persister or
PostgreSQL configuration. A database adapter must implement a real atomic durable
conditional commit. Merely enqueueing Myko events does not satisfy the contract.
Storage failures deny authorization until a valid snapshot can be loaded again.

See [Render a server's permissions in Leptos](leptos-permissions.md) for reusable
components that discover entity scopes and permission records from the server.

## Entity scopes and helper types

Generated read, write, and delete scopes belong to `#[myko_item]` entities.
`#[myko_view_item]` computed rows and `#[myko_subtype]` helper types do not own
permission scopes. A computed view can declare `#[myko_scope(SourceEntity, read)]`
for each source entity it exposes. These explicit mappings use real entity scopes
instead of a synthetic scope for the output row. A view over an actual entity
retains its default entity-read mapping. Unmapped computed views remain denied
by scope-based authorization unless the application's authorizer handles them.
