# Authentication and local authorization

Myko accepts app-owned authentication and authorization through `MykoAuthProvider`.
Authentication establishes an identity once per WebSocket connection or HTTP request.
Authorization reads current app policy locally for each operation.

## Provider composition

`myko-auth` defines `Authenticator`, `Authorizer`, `AuthSession`, and the operation model.
`myko-auth0` implements authentication and optional machine credential acquisition.
Apps can use `myko-permissions` for persisted principals, groups, roles, and grants,
or implement `ScopePolicy` using their own permission store.
`ScopedAuthorizer` resolves generated operation scopes and inheritance before calling that policy.
`Authorizer` remains available for apps that need a custom operation policy.
See [Persisted permissions](permissions.md) for protected administration, startup,
bootstrap, and host-only recovery. Custom policies retain ownership of their store.

Compose the provider before starting a server.

```rust,ignore
let authentication = Arc::new(myko_auth0::Auth0Authenticator::new(
    "https://tenant.auth0.com/",
    "https://api.example.com/",
)?);
let authorization = Arc::new(myko_auth::ScopedAuthorizer::new(app_scope_policy));
let provider = Arc::new(myko_auth::MykoAuthProvider::new(authentication, authorization));
let server = myko_server::MykoServer::builder()
    .with_auth_provider(provider)
    .build();
```

An app that owns its listener uses `router::route_connection_with_auth`.
Its `GatewayAccess::ReadOnly` ceiling applies even when the provider permits a mutation.
Without a configured provider, existing trusted entry points retain their prior behavior.
Expose only the authenticated entry points to untrusted clients.

### Application user references

`myko::identity::UserId` is a shared newtype for stable application profile IDs.
The host authentication provider supplies its value; Myko does not generate it
from an email, a connection ID, or a permission principal. It serializes as a
string and supports typed equality filters and generated TypeScript bindings.
It carries no credentials or grants. `PermissionPrincipalId` remains a separate
type for policy records, so deleting a permission principal does not change an
application user's durable identity.

## Generated operation scopes

Use `ScopedAuthorizer` when the app grants permissions by entity instead of matching command names.
Myko generates `Entity:read`, `Entity:write`, and `Entity:delete` for each registered entity.
Scope names preserve the reflected entity name, such as `Camera:write`.

Generated queries, count reports, and get-by-ID reports require read permission.
Generated setter and rename commands require write permission.
Generated delete commands require delete permission.
Graph connect and ensure commands require write permission for the edge entity.
Graph synchronization requires both write and delete because it can remove omitted edges.
Graph read operations require edge read permission and any returned concrete endpoint type's read permission.
Traversal with concrete endpoint types requires read permission for both endpoint types.
Polymorphic traversal without statically known endpoint types remains unmapped and is denied by `ScopedAuthorizer`.
Direct client `SET` events require write permission and `DEL` events require delete permission.
A policy can further restrict direct events through the original `AuthorizationRequest`.

Wrap the app's scope policy before constructing the provider.

```rust,ignore
let authorization = Arc::new(myko_auth::ScopedAuthorizer::new(app_scope_policy));
let provider = Arc::new(myko_auth::MykoAuthProvider::new(authentication, authorization));
```

The app implements `ScopePolicy::decision` using its local user, group, and role grants.
The method receives the identity, an `EntityScope`, and the original request.
Return `Allow` for an explicit grant, `Deny` for an explicit override, and `Unset` when no setting exists.
Returning `Deny` for every missing entry prevents inheritance.
The app must honor any credential and delegation limits as part of its policy.
The adapter does not infer the meaning of OAuth scope strings.

`belongs_to` and `owns_many` declarations generate the entity-type inheritance graph.
A `Project:write` grant therefore applies to a child `Camera:write` requirement unless a more specific setting overrides it.
Inheritance follows the same action only. Write never implies read or delete.
An explicit camera setting wins over project defaults.
The closest ancestor with a setting wins when the child is unset.
If equally close parents conflict, denial wins.
Unknown requirements and cycles without a grant deny access.
`ensure_for` dependencies and arbitrary graph references do not establish permission inheritance.
Optional and multiple ownership relationships participate by entity type.

For example, an app's explicit grant table can contain these entries.

| Scope | Setting | Effect |
| --- | --- | --- |
| `Project:write` | Allow | Supplies write permission to descendant entity types |
| `Camera:write` | Unset | Inherits the project setting |
| `Camera:delete` | Deny | Rejects camera delete operations |

Changing `Camera:write` to `Deny` overrides the inherited write permission.
Changing it to `Allow` grants the operation even when the parent has no operation grant.
These are type-level operation settings. They do not grant access to a specific project instance.

Custom commands and reports declare their requirements beside their definitions.
Put the helper attribute below the enclosing Myko macro.

```rust,ignore
#[myko::myko_command]
#[myko_scope(Camera, write)]
pub struct CalibrateCamera {
    pub id: CameraId,
}
```

Multiple helper attributes require every declared scope.
Generated entity queries and views over actual entities require that entity's
read permission and can declare additional requirements. Computed `view_item`
rows and `myko_subtype` helper types do not acquire entity scopes. Views over
computed rows declare the required source entity scopes explicitly.
Unannotated custom commands and reports are denied by `ScopedAuthorizer`.
Trusted handler code must declare all the authority its operation requires.
The operation layer does not reauthorize every internal handler read or emitted mutation.

Use `PermissionCatalog::registered().scopes()` to enumerate permissions for role configuration.
Use `requirements(kind, operation)` and `parents(entity_type)` to inspect operation requirements and inheritance.
The catalog comes from compiled macro registrations.
Conflicting bindings fail closed rather than selecting one registration.
MCP operation search includes `requiredScopes` for mapped operations.
Discovery and execution resolve the same requirements.

## Future row-level policy

Operation permission is one check. Future row-level policy will add a separate mandatory resource check.
The existing request retains verified identity and operation arguments for that layer.
The entity relationship metadata remains available for resolving specific resource ancestry.

This release does not distinguish cameras in Project A from cameras in Project B.
A `Camera:read` operation grant permits the operation on that entity type.
It does not filter query rows, apply project-instance boundaries, or enforce old and proposed ownership on writes.
An operation-level child override must never bypass a future mandatory row-level boundary.
Do not use type inheritance as a substitute for project-instance isolation.

## Identity and authority

A stable principal consists of issuer, subject, and kind.
Kinds include users, services, and agents.
The socket ID remains a separate, temporary routing identity.
`RequestContext::auth` carries the verified session into commands, queries, views, reports, and child requests.
External in-process MCP execution uses that same authority.

The authenticator supplies verified credential scopes and delegation information.
The local authorizer must enforce those limits together with current app grants.
Myko cannot infer an app's resource scope rules from OAuth scope strings.
Auth0 does not invent a delegation chain from an ordinary user token.
An app that supports delegation must verify the chain and restrict every delegated grant.

Groups can contain user and service principals.
Roles can bundle permissions.
The standard permission store binds a principal or group to an operation role.
Custom policies can additionally interpret project or resource scope.
These relationships can change without an Auth0 request.
The synchronous `Authorizer::authorize` interface makes local evaluation explicit.

## Enforcement

Myko checks native operations before dispatch and commands again immediately before handler execution.
MCP checks operations beneath the sandbox executor.
MCP search and schema resources apply separate discovery decisions.
Knowing an operation name never grants execution permission.
`connection_status` includes the authenticated principal, session expiry, and credential scopes.
It does not claim that credential scopes enumerate current app permissions.

Native JSON and CBOR client events have a distinct `ClientEvent` authorization request.
Myko sanitizes and normalizes the event before asking policy.
A batch must pass every decision before any event in that batch reaches the store or journal.
`Set` means upsert. It does not mean update-only.
The event's source and ownership fields are untrusted input.
Use type and resource constraints that remain valid through application.
Use a command with application-level concurrency control when permission depends on mutable existing state or a create-versus-update distinction.
Batch preflight is an authorization guarantee, not a new transactional store guarantee.
Command results from executors require an explicit `CommandResult` decision.

Operation admission does not filter rows.
A caller-sensitive query must use its verified request identity to restrict results, or policy must reject the query.
Authenticated query, view, and report caches are automatically partitioned by a server-generated session ID.
This prevents a shared default cache from capturing another caller's authority.

## Session lifetime

Myko checks credential expiry, explicit revocation, and the authorizer's revision.
Provider-created sessions also call `Authenticator::validate_identity` with the admitted identity.
The default implementation accepts it, so stateless authenticators need no changes.
Stateful authenticators can check a local session store to detect logout for one credential,
including when another credential belongs to the same principal.
This synchronous hook must not perform network I/O or retain the bearer token in identity claims.
Return `AuthError::Revoked` to invalidate that session permanently.
Other errors deny the current check; `Unavailable` permits later checks to recover.
Transports close invalid sessions, so a client may still need to reconnect after an outage.
Use `MykoAuthProvider::authenticate` to retain this hook.
`AuthSession::new` accepts an already trusted identity without an authenticator liveness check.

Increment `ScopePolicy::revision`, or a custom `Authorizer::revision`, when a policy update can invalidate existing subscriptions.
The revision must be monotonic and published with the changed policy.
A changed revision invalidates the session permanently.
Myko closes the connection and drops its subscriptions.
The client reconnects to establish a new session against current policy.

Idle connections notice invalidation within a 100 ms polling interval.
Active delivery checks session validity after serialization and before sending.
Queued commands recheck immediately before execution.
Already executing handlers and bytes already sent cannot be undone.
An Auth0-side revocation needs an app notification or credential expiry to reach an established local session.

## Client credentials

Native `MykoClient::set_credential_provider` obtains credentials before each connection attempt.
A provider failure prevents the connection from opening without credentials.
`set_bearer_token` accepts a token managed by the app.
Changing the provider reconnects the transport. Configure it before setting the server address.
Unsupported injected transports reject credential configuration.

Native clients send a bearer authorization header.
Browser clients offer public `myko` and an encoded credential subprotocol.
The server selects only public `myko` and never echoes the credential.
Use TLS and redact authorization and WebSocket protocol headers in proxy logs.
Tokens never belong in a connection URL.

### Machine clients with an external authorization server

Myko does not issue machine tokens. A service obtains them from an external OAuth 2.0
authorization server with the client credentials grant (RFC 6749 section 4.4).
The token identifies the client itself; no user is involved.

Enable the native-only `client` feature of `myko-auth` and configure `client::ClientCredentials`.
Protocol requests use the `oauth2` crate, and discovery uses the `openidconnect` crate.

```rust,ignore
use myko_auth::client::{ClientCredentials, TargetResource, TokenEndpoint};

let credentials = ClientCredentials::new(
    TokenEndpoint::Discover("https://login.example.com/".into()),
    client_id,
    client_secret,
)?
.with_target(TargetResource::Resource("https://api.example.com/".into()))
.with_scope("Camera:read");
client.set_credential_provider(Arc::new(credentials));
```

`TokenEndpoint::Discover` reads `token_endpoint` from the issuer's OIDC discovery document on first use.
`TokenEndpoint::Fixed` uses a documented endpoint without discovery.
`TargetResource` selects the RFC 8707 `resource` parameter or the Auth0-style `audience` parameter.
`ClientAuth` selects `client_secret_basic`, the default, or `client_secret_post`.
Endpoints must use HTTPS; plain HTTP is accepted only for loopback development hosts.
The provider does not follow redirects, so the secret goes only to the configured endpoint.
Clones share one token cache, which renews tokens shortly before expiry.

With the `client-credentials` feature, `myko-auth0` provides `Auth0ClientCredentials`,
which is `ClientCredentials` configured for Auth0's `oauth/token` endpoint,
`client_secret_post`, and `audience` parameter.
Auth0 token verification uses cached signing keys and validates RS256, issuer, audience, subject, expiry, and optional not-before.
JWT validation never runs as a network request for each command.

The legacy stdio MCP adapter accepts `McpServer::with_credential_provider` for its remote Myko connection.
Its local discovery uses compiled registrations and client filters.
The protected remote gateway enforces execution permissions.
Per-principal remote discovery uses the server-hosted MCP endpoint.
Full OAuth authorization-server discovery and browser login UI are outside this provider integration.

## Provider boundaries

Authentication and local authorization are separate interfaces. An Auth0 verifier
can therefore be shared without owning application policy. The session helper
centralizes expiry, revocation, and cache identity. The optional
`myko-permissions` crate supplies a standard local policy with app-selected storage.

## Verification

Run the complete focused suite.

```sh
cargo flux run verify-auth
```

The script uses serial Rust tests and runs scope, macro, graph, client, server, WASM, lint, and formatting checks through Flux.

Auth0 tests use signed fixtures and local HTTP servers.
They do not require tenant credentials.
The WASM check compiles browser transport code. It does not prove browser runtime behavior.

## Explicit public operations and remote events

With authentication configured, unmarked operations remain protected. Place
`#[myko_public]` after a command, query, view, or report decorator to allow that
specific operation without a verified identity:

```rust,ignore
#[myko_command(EnrollmentStatus)]
#[myko_public]
pub struct CheckEnrollment { pub installation_id: String }

#[myko_query(ExecutorTarget)]
#[myko_public]
pub struct DiscoverExecutorTargets { pub service_id: String }
```

The same marker works after `#[myko_view(...)]` and `#[myko_report(...)]`.
Discovery and execution use the exact operation kind and ID. Marking a query
public does not expose other queries, generated operations, or its entity events.
Public handlers must validate their arguments and limit the data or effects they
expose to an unauthenticated caller.

Entities opt into client-originated events separately, by change type:

```rust,ignore
#[myko_item]
#[myko_public_events(set)]
pub struct ExecutorPulse { pub value: f64 }
```

Use `#[myko_public_events(set, delete)]` only when anonymous deletion is also
intended. Neither form makes generated queries, setters, reports, or delete
commands public. Normal gateway ceilings, server-owned field checks, and
connection-owned field normalization still apply.

The provider's transport admission accepts anonymous sessions when public
metadata is present. Invalid supplied credentials are rejected, never downgraded
to anonymous access. `authenticate` remains strictly authenticated; transport
routing uses `admit`. `AuthSession::identity()` returns `None` for anonymous
sessions, which remain distinct from trusted internal requests with no session.
WebSocket JSON/CBOR and MCP use the same authorization boundary.

`Authorizer::authorize_anonymous` defaults to the explicit public registrations.
Applications may override it to restrict arguments or entity rows, or define
additional anonymous policy. `with_anonymous_access()` enables anonymous
admission without decorators for such policies; it grants no operations itself.
Authenticated calls continue through the application's authenticated authorizer.
Permission administration still requires a verified identity. Policy revision
changes invalidate anonymous sessions too, and request caches remain partitioned
by external session.

Generated entity queries are selected separately with
`#[myko_public_queries(all, by_ids, by_query)]`. Include only the needed families:
`all` exposes `GetAll<Entity>s`, `by_ids` exposes `Get<Entity>sByIds`, and
`by_query` exposes `Get<Entity>sByQuery`. This never exposes generated reports,
setters, or delete commands.

### Operation errors do not disconnect valid sessions

A rejected command, query, view, or report returns its existing transaction-correlated
`ws:m:*-error` message. The request is not executed or installed as a subscription;
other subscriptions on the socket continue. Read-only gateway denials use the same
path. Rejected entity events (including a mixed authorized/unauthorized batch) have
no effects and are logged; entity events have no request/reply transaction.
Expired, revoked, or otherwise invalid sessions still close the connection.

Native query/view watches expose a separate reactive error cell:

```rust,ignore
let watch = client.watch_query_state(GetAllProjects {});
let items = watch.items(); // existing Vec<Arc<Project>> data shape
let ready = watch.ready(); // an error is not a successful empty snapshot
let error = watch.error(); // Cell<Option<String>>
```

The same `error()` accessor exists on query/view map watches and windowed/cursor
query watches. Shared watches share their error state. An error does not overwrite
the last successful data snapshot or advance its sequence. A subsequent response
clears the error, and a reconnect clears the previous connection's error before
resubscription. Existing data-only methods and success payloads are unchanged;
there is no new `Result` wrapper. TypeScript subscriptions already route these
error messages through their existing observable error channel.

Leptos callers can opt in without changing the existing data-only or loaded APIs:

```rust,ignore
let (rows, loaded, error) = myko_leptos::live_view_state(|| MyView { /* ... */ });
// Render the error/fallback when error.get() is Some, loading while !loaded.get(),
// and the ordinary rows when loaded. An initial denial never marks data loaded.
```

`live_query_state`, `live_view_state`, `live_query_store_state`, and
`live_view_store_state` return `(data, loaded, error)`; windowed query handles expose
`error()` separately. Reactive parameter changes clear the previous error, and
callbacks from a retired subscription cannot restore its rows or denial message.
Raw Rust clients can use `watch_query_raw_state` and `watch_view_raw_state` for the
same data/readiness/error separation; their original data-only methods are retained.

## MCP OAuth resource discovery

An HTTP MCP host can advertise an external OAuth authorization server through
`OAuthResource` and `MykoAuthProvider::with_mcp_oauth`. The resource is the
public canonical MCP endpoint URI, not a socket address inferred from a request.
The host supplies configuration explicitly; Myko never reads an issuer or
resource from environment variables or untrusted forwarding headers.

```rust,ignore
let resource = myko_auth::OAuthResource::new(
    "https://api.example.com/myko/mcp",
    vec!["https://login.example.com/".into()],
)?.with_scopes(vec!["mcp:use".into()])?;
let provider = MykoAuthProvider::new(authentication, authorization)
    .with_mcp_oauth(resource);
```

MCP HTTP initialization, initialized notifications and the static code-mode tool
list are public. Application requests still require authentication and return an
HTTP OAuth challenge before executing. Invalid supplied credentials fail closed,
even for bootstrap. Anonymous optional event-stream probes receive 405, so they do
not start login during discovery. This preserves the configured anonymous policy
for the ordinary Myko WebSocket gateway.
The router exposes RFC 9728 protected-resource metadata at both the root well-known
URI and its resource-path variant. A 401 challenge points to that metadata and
advertises configured scopes. Metadata retrieval itself requires no credential.
HTTPS is required except for explicit loopback HTTP development endpoints.

The authenticator must accept OAuth access tokens for the advertised resource,
validate signature, issuer, audience, expiry and any identity policy, and return
a trusted identity. Local authorization still decides operation access. This API
does not turn Myko into an authorization server, register clients, or implement
login or consent screens. Clients obtain tokens from the advertised provider
using its supported registration mechanism and authorization-code flow with PKCE.

### Provider login adapters

`myko-auth0::login::LoginProvider` handles Auth0 OIDC discovery, bounded code
exchange with PKCE verifier validation, ID-token nonce/authorized-party binding,
and the Auth0 passwordless email-code extension. It returns verified provider
claims. Hosts choose supported methods, render their own login UI, and apply
profile/email-domain admission policy. These provider operations do not require
a hosted login page or prescribe a component library.

`myko-auth0::JwtVerifier` exposes the same key cache and issuer/audience/lifetime
verification used by the Auth0 authenticator, including explicit trusted JWKS
configuration for other OIDC workload issuers. ID-token checks additionally bind
the authorized party and nonce; API-token verification binds its resource audience.

### OAuth handoff with a custom login UI

`MykoAuthProvider::with_oauth_server(Arc<OAuthServer>)` enables a same-listener
OAuth authorization service. Configure its canonical issuer, MCP resource URI,
and host login URL explicitly. HTTPS is required outside loopback development.
It advertises RFC 8414 server metadata and RFC 9728 resource metadata, requires
S256 PKCE and exact registered redirects, and binds authorization and token
requests to the configured resource. Authorization responses include RFC 9207
`iss`. The service issues resource-bound opaque access tokens, not OIDC ID tokens.

```rust,ignore
let oauth = myko_auth::oauth::OAuthServer::new(
    "https://api.example.com/myko/oauth",
    "https://api.example.com/myko/mcp",
    "https://app.example.com/",
)?;
oauth.add_client("desktop", "Desktop client", vec![
    "http://127.0.0.1:8765/callback".into(),
])?;
let provider = MykoAuthProvider::new(authentication, authorization)
    .with_oauth_server(Arc::new(oauth));
```

Public-client dynamic registration is an explicit opt-in via
`with_dynamic_registration()`. Pre-registration works without it. Confidential
clients and client-ID metadata documents are not implemented; metadata does not
advertise those capabilities. Client names are untrusted presentation text.

The authorize endpoint validates the request, then redirects to the host login
page with a short-lived opaque `myko_oauth` query parameter. After login, the UI
uses the `MykoOAuthRequest` command to inspect the request and show explicit
consent, including its client name, callback and scopes. Approval submits the
existing session credential through that command. Myko authenticates it before
issuing a one-use callback code; it never trusts a browser-supplied identity.
The host must retain the handoff parameter across external login redirects.
No approval is automatic, even for previously signed-in users.

This framework command becomes anonymously available when the service is
configured; ordinary operation policy is unchanged. MCP HTTP bootstrap exposes
only static capabilities and tool definitions, never operation search results or
application data. Invalid credentials fail
closed. MCP requires a resource-specific access token. Browser session credentials
remain usable on the normal Myko WebSocket transport, and issued OAuth credentials
can use that same transport and local authorizer.

Access tokens last at most five minutes and never outlive the parent login
session. Refresh tokens rotate; reuse revokes the grant family. Parent logout,
expiry and current local permission checks apply to already admitted OAuth
sessions. Revoking a grant does not log the user out of the browser. Grants do
not introduce offline access or additional entity permissions.

Pending requests, codes, clients and issued tokens are bounded, process-local
state. Restarting the service requires clients to sign in again. Multiple
instances need routing affinity; durable shared OAuth state is not provided.
Only SHA-256 digests of pending requests/codes/tokens are used as lookup keys.

### Authenticated application users in handlers

An authenticator can supply `AuthenticatedIdentity.user_id` as a typed shared
application `UserId`, independently of its provider subject or connection ID.
Every request context exposes `user_id()`, and command handlers use
`ctx.user_id()?`. The accessor returns the verified user and an ordinary error
when no live authenticated application user is available; nested requests retain
it. Hosts choose the user mapping during authentication. This supplies caller
identity only and does not add row-level authorization rules.

### External entity events

Raw client SET/DEL events are denied by default, including gateways without an
auth provider. Opt an entity into those events explicitly:

```rust
#[myko_item]
#[myko_external(SET)]
pub struct RemoteObservation {
    pub value: String,
}
```

Use `#[myko_external(SET, DEL)]` for both operations, or `#[myko_external(DEL)]`
for delete-only ingress. These schema limits apply even to authenticated callers
and cannot be expanded by an authorizer. An authorized command handler may still
perform its own server-side mutations.

The attribute also works on manually implemented `Eventable` types and uses their
declared entity name. Applications do not need inventory registration syntax.
This enables an ingress mechanism; it does not bypass application authorization.
To permit anonymous events with auth configured, also select the changes with
`#[myko_public_events(set)]` or `#[myko_public_events(set, delete)]`.

Myko normalizes event names before checking and rejects an entire mixed batch
before applying any events. Trusted in-process server events and authorized commands remain
available for every entity; reads are unaffected. This is an entity-wide ingress
contract, not a row policy.

Upgrading an app that previously sent raw entity events requires adding the
`myko_external` attribute to the intended remote-write types. An app without
an auth provider still allows its existing commands, queries, views and reports;
raw client events now require this explicit opt-in.
