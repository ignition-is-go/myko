# MCP Endpoint at `/myko/mcp`

**Status:** Draft
**Date:** 2026-05-20

## Goal

Host an MCP (Model Context Protocol) endpoint inside the Myko server, on the
same TCP listener as the existing WebSocket gateway. The endpoint is
content-negotiated across three transports so any MCP client can connect:

- `GET /myko` with `Upgrade: websocket` → existing Myko WS handler (unchanged).
- `POST /myko/mcp` → MCP JSON-RPC request; immediate JSON response (Streamable HTTP).
- `GET  /myko/mcp` with `Upgrade: websocket` → MCP over WebSocket (JSON-RPC text frames, bidi).
- `GET  /myko/mcp` with `Accept: text/event-stream` → SSE stream for server-initiated notifications.
- Everything else → `404`.

MCP currently runs as a separate stdio binary that connects out to Myko over WS.
Hosting it in-process removes the WS round-trip and lets us reuse `CellServerCtx`
directly. The stdio path is kept as-is for editors/clients that need it.

Per-client tool filtering via request headers gives MCP-client configs (e.g. a
Claude Desktop entry) a way to lock down what a given config can call, without
trusting the client itself.

## Non-goals

- No reactive query → SSE push subscriptions in v1. SSE GET is opened and kept
  alive but emits nothing until we wire up MCP `notifications/resources/updated`
  in a follow-up.
- No `Mcp-Session-Id` session tracking — server is stateless, each POST stands
  alone.
- No HTTP-level authentication. Deployments that need auth put a reverse proxy
  in front of the server. Header-based tool filtering is defense-in-depth on
  the config side, not an auth boundary.
- No new HTTP framework (hyper/axum). Hand-rolled HTTP/1.1 front-door is
  sufficient for our request shape and avoids a heavy dep.

## Architecture

```
                          TcpListener (:5155)
                                 │
                          ┌──────┴──────┐
                          │   router    │  parse request line + headers
                          └──┬──┬──┬────┘
                             │  │  │
              GET /myko ─────┘  │  └───── *  → 404
              (Upgrade: WS)     │
                 │              │
                 ▼              ▼
        ws_handler         mcp::http
        (existing)             │
                               ├── POST /myko/mcp ─→ dispatch ─→ JSON
                               ├── GET  /myko/mcp + Upgrade: ws ─→ MCP-WS loop
                               └── GET  /myko/mcp + Accept: SSE ─→ SSE stream
                                       │
                                       ▼
                             mcp::dispatch (in-process)
                             uses CellServerCtx directly
```

### New / modified files

| File | Status | Purpose |
|---|---|---|
| `libs/myko/server/src/router.rs` | new | Read & parse HTTP request line + headers off a `TcpStream`. Decide WS upgrade vs MCP HTTP vs 404. |
| `libs/myko/server/src/mcp/dispatch.rs` | new | Transport-agnostic JSON-RPC handler. `handle_request(req, filter, exec) -> McpResponse`. |
| `libs/myko/server/src/mcp/exec.rs` | new | `Executor` enum: `Client(MykoClient)` (stdio) or `InProcess(CellServerCtx)` (HTTP). Implements `execute_query`, `execute_report`, `execute_command`. |
| `libs/myko/server/src/mcp/filter.rs` | new | `ToolFilter` (allow/deny glob patterns), parsed from `X-Myko-Tools-Allow` / `X-Myko-Tools-Deny`. |
| `libs/myko/server/src/mcp/http.rs` | new | POST handler (JSON dispatch) + GET handler (SSE stream). |
| `libs/myko/server/src/mcp/ws.rs` | new | WebSocket MCP loop: read text frames, dispatch, write text frames. Reuses tungstenite handshake helpers + `WebSocketStream::from_raw_socket`. |
| `libs/myko/server/src/mcp/server.rs` | refactor | `run_stdio` now wraps `dispatch` with `Executor::Client`. Old `RequestHandler` / `tool_executor` / `execute_*` move into `dispatch.rs` + `exec.rs`. |
| `libs/myko/server/src/ws_handler.rs` | minor | Split `handle_connection` so callers can pass an already-buffered prefix (the bytes the router peeked) into the WS handshake. |
| `libs/myko/server/src/lib.rs` | minor | `run_ws_accept_loop` → `run_accept_loop`, dispatches through the router. |
| `libs/myko/server/Cargo.toml` | minor | Add `httparse` (transitive today; pull in directly). `tungstenite` already provides handshake helpers; no new crypto deps. |

### HTTP front-door (router.rs)

`tokio::net::TcpStream::peek` is unreliable across packet boundaries, so we
read with a `BufReader<TcpStream>`:

1. Read lines until empty line (end of headers). Cap at 8 KB to bound risk.
2. Parse method + path + headers with `httparse`.
3. Route:
   - `GET` + path == `/myko` + `Upgrade: websocket` → hand stream + parsed
     request to `ws_handler::handle_connection_after_request`, which writes the
     101 response (computing `Sec-WebSocket-Accept` via
     `tungstenite::handshake::derive_accept_key`) and wraps the stream with
     `WebSocketStream::from_raw_socket(_, Role::Server, _)`.
   - `POST` + path == `/myko/mcp` → read `Content-Length` bytes (cap 1 MB),
     parse JSON-RPC, call `mcp::http::handle_post`.
   - `GET` + path == `/myko/mcp` + `Upgrade: websocket` →
     `mcp::ws::handle_upgrade` (writes 101 + announces `Sec-WebSocket-Protocol: mcp` if requested, wraps stream, runs loop).
   - `GET` + path == `/myko/mcp` + `Accept: text/event-stream` →
     `mcp::http::handle_sse`.
   - Else → write `404 Not Found` and close.

Errors during HTTP parsing → write `400 Bad Request` and close.

### Dispatch core (dispatch.rs)

```rust
pub async fn handle_request(
    req: McpRequest,
    filter: &ToolFilter,
    executor: &Executor,
    server_name: &str,
    server_version: &str,
) -> McpResponse
```

Methods handled: `initialize`, `notifications/initialized`,
`notifications/cancelled`, `tools/list`, `tools/call`, `resources/list`,
`resources/read`. Behavior matches the existing stdio implementation, with
three changes:

- `tools/list` runs the catalog through `filter.allows(&tool_name)`.
- `tools/call` rejects with `McpError { code: -32601, message: "Tool denied by filter" }` if `!filter.allows(name)`.
- Tool execution dispatches through `Executor` rather than calling `MykoClient` directly.

### Executor (exec.rs)

```rust
pub enum Executor {
    Client(Arc<MykoClient>),
    InProcess(Arc<CellServerCtx>),
}

impl Executor {
    pub async fn execute_query(&self, id: &str, args: Value) -> Result<Value, String>;
    pub async fn execute_report(&self, id: &str, args: Value) -> Result<Value, String>;
    pub async fn execute_command(&self, id: &str, args: Value) -> Result<Value, String>;
    pub async fn connection_status(&self) -> Value;
}
```

`Executor::Client` is what `run_stdio` builds today — the existing
`execute_query` / `execute_report` / `execute_command` code in `server.rs`
moves here verbatim.

`Executor::InProcess` paths execute directly against the registry. We follow
the same code paths the WS handler uses today (see `ws_handler.rs:837` for
queries, `:1082` for reports, and command dispatch via `CommandContext`):

- Query: `handler_registry.get_query(query_id)`, build the wrapped query with a
  fresh `tx` and `createdAt`, build a `RequestContext::internal`, run the
  hyphae cell to first value, return items.
- Report: `handler_registry.get_report(report_id)`, run, return value.
- Command: build `CommandContext` with internal `RequestContext`, call
  `command.execute_boxed(cmd_ctx)`, return the response or error.

For v1 we use one-shot execution with a timeout (same 5 s / 10 s caps as
stdio). Reactive subscriptions over SSE are deferred.

### Tool filter (filter.rs)

```rust
pub struct ToolFilter {
    allow: Vec<Pattern>,  // empty = allow all
    deny:  Vec<Pattern>,  // always applied
}

pub fn parse_from_headers(headers: &HeaderMap) -> ToolFilter;
pub fn allows(&self, tool_name: &str) -> bool;
```

Patterns are comma-separated values from headers. Each pattern is a glob:

- `*` matches any tool.
- `<literal>*` is a prefix match (`command:Delete*` matches `command:DeleteFoo`).
- `*<literal>` is a suffix match.
- Otherwise, exact match.

`allows` returns `true` iff (allow is empty OR any allow pattern matches) AND no
deny pattern matches. Deny wins on conflict.

`X-Myko-Tools-Allow: query:*,report:*` + `X-Myko-Tools-Deny: query:Get*Internal`
means: every query and report is callable, except queries named like
`GetSomethingInternal`.

Filter is parsed per-request (stateless POST). For SSE GET and WS connections,
we capture the filter at handshake time and reuse it for every request /
pushed message over that connection's lifetime.

### POST handler (http.rs)

```
POST /myko/mcp HTTP/1.1
Content-Type: application/json
X-Myko-Tools-Allow: query:*,report:*

{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query:GetAllTargets","arguments":{}}}
```

Flow:
1. Read body up to `Content-Length` (cap 1 MB → `413` over limit).
2. Parse `McpRequest`. Parse errors → JSON-RPC error response with `id: null`, code `-32700`.
3. Parse `ToolFilter` from headers.
4. Call `dispatch::handle_request(req, &filter, &executor, ...)`.
5. Write response:
   ```
   HTTP/1.1 200 OK
   Content-Type: application/json
   Content-Length: <len>
   Connection: close

   <json>
   ```

We always set `Connection: close` for simplicity (no keep-alive in v1).

### WebSocket handler (ws.rs)

```
GET /myko/mcp HTTP/1.1
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: ...
Sec-WebSocket-Version: 13
Sec-WebSocket-Protocol: mcp
X-Myko-Tools-Allow: query:*,report:*
```

Flow:
1. Router has already parsed the request. Validate `Sec-WebSocket-Version: 13`
   and `Sec-WebSocket-Key` is present.
2. Compute `Sec-WebSocket-Accept` via
   `tungstenite::handshake::derive_accept_key`.
3. Parse `ToolFilter` from headers; capture for the connection.
4. Write 101 response. Echo `Sec-WebSocket-Protocol: mcp` if the client
   requested it.
5. Wrap stream with
   `WebSocketStream::from_raw_socket(stream, Role::Server, Some(config))`.
6. Loop:
   - On text frame: parse `McpRequest`, call `dispatch::handle_request`, send
     `McpResponse` as text frame. JSON-RPC notifications (no `id`) suppress the
     response.
   - On binary frame: send error frame and continue (MCP-WS is text-only).
   - On close / read error: drop the connection.
7. Server-initiated pushes (future) write text frames containing JSON-RPC
   notifications.

WS uses the same `WS_MAX_MESSAGE_SIZE_BYTES` / `WS_MAX_FRAME_SIZE_BYTES` caps
as the main Myko WS gateway, so a large query result over MCP-WS gets the same
treatment as one over the main gateway.

### SSE handler (http.rs)

```
GET /myko/mcp HTTP/1.1
Accept: text/event-stream
X-Myko-Tools-Allow: query:*

```

Response:
```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

: keepalive

```

Server writes a `:` comment line every 15 s to hold the connection. No actual
notifications emitted in v1. Disconnect on client close (write error).

When notifications are wired up in a follow-up, the SSE stream will receive
filtered MCP notifications (e.g. `notifications/resources/updated` for
reactive query result changes).

### Config

`CellServerBuilder` grows one knob:

```rust
.with_mcp_http(true)  // default: true
```

When `false`, the router still pre-parses HTTP but returns 404 for `/myko/mcp`.
The cost of the HTTP pre-parse on the WS hot path is one line read + one
`Upgrade:` header check — negligible. There is no separate-port option in v1.

## Data flow examples

**Tool call (HTTP):**
1. Client `POST /myko/mcp` with `tools/call` body and `X-Myko-Tools-Allow: query:*`.
2. Router parses, dispatches to `mcp::http::handle_post`.
3. `handle_post` builds `ToolFilter` from headers, calls `dispatch::handle_request`.
4. `dispatch` resolves the method, applies filter, builds the wrapped query.
5. `Executor::InProcess` runs the query against the registry, awaits the hyphae cell.
6. Result → `McpResponse` → JSON → HTTP body.

**WS connection (unchanged from client's POV):**
1. Client `GET /myko` with `Upgrade: websocket`.
2. Router peeks the request line + headers, sees WS upgrade.
3. Hands buffered request + stream to `ws_handler`, which writes the 101 response and wraps the stream as a tungstenite `WebSocketStream`.
4. Existing WS handler logic runs.

## Error handling

- HTTP parse error → `400 Bad Request`, body empty, close.
- Body over 1 MB → `413 Payload Too Large`.
- JSON-RPC parse error → `200 OK` with JSON-RPC error response (code `-32700`).
- Method not found → JSON-RPC error `-32601`.
- Tool denied by filter → JSON-RPC error `-32601`, message `"Tool denied by filter"`.
- Tool execution timeout → JSON-RPC `result` with `isError: true` content (same shape as today).
- Internal panic in executor → caught by Tokio task boundary, returns 500.

## Testing

- Unit: `router` parses correct method/path/headers across line-boundary splits.
- Unit: `ToolFilter` patterns (prefix, suffix, exact, allow/deny precedence).
- Unit: `dispatch` filter application on `tools/list` and `tools/call`.
- Integration: spin up `CellServer`, send `tools/list` over HTTP, assert filter trims output.
- Integration: send `tools/call` for a denied tool, assert error.
- Integration: send a valid query call, assert result matches what WS returns for the same query.
- Integration: open MCP-WS, run `tools/list` + `tools/call`, assert filter is captured at handshake.
- Smoke: existing Myko WS + MCP-WS + HTTP POST all coexist on the same port.

## Migration

- No API breakage: existing WS clients connect to `ws://host:5155/myko` exactly
  as before.
- The standalone stdio binary (`myko::mcp::McpServer::run_stdio`) is unchanged.
- Downstream apps (e.g. rship) get MCP-over-HTTP for free; they can keep the
  stdio binary for editor integrations and add HTTP for in-process callers.

## Deployment (rship-control-plane)

rship-server runs behind Traefik v3.6 in Docker Swarm. The current router for
`rship_server1` already matches `/myko/mcp`:

```yaml
- traefik.http.routers.server.rule=(PathPrefix(`/myko`) || PathPrefix(`/server`))
- traefik.http.services.server.loadbalancer.server.port=5155
```

Because `PathPrefix(/myko)` is a prefix match and `WebSocket` upgrade is
transparent in Traefik, **the bare endpoint works as-is for POST and WS** —
no control-plane changes are strictly required to ship.

However, three changes in `rship-control-plane/stacks/rship.yml` are
recommended so the surface is operable and SSE behaves well in production:

### 1. Dedicated `server-mcp` router (higher priority)

Splitting MCP onto its own router lets ops attach middlewares (rate limit, IP
allowlist, forward-auth) without touching the main Myko WS gateway, and makes
the route visible in the Traefik dashboard.

```yaml
# additions to the rship_server1 deploy.labels list
- traefik.http.routers.server-mcp.rule=PathPrefix(`/myko/mcp`)
- traefik.http.routers.server-mcp.priority=100        # win over /myko prefix
- traefik.http.routers.server-mcp.service=server      # same backend service
- traefik.http.routers.server-mcp.entrypoints=websecure
- traefik.http.routers.server-mcp.tls=true
- traefik.http.routers.server-mcp.tls.certresolver=letsencrypt
- traefik.http.routers.server-mcp.middlewares=strip-protocol-headers
```

The original `server` router stays exactly as it is — it just no longer wins
the `/myko/mcp` path because the more specific router has higher priority.

### 2. SSE-friendly response forwarding

For SSE GET to stream without buffering surprises, add a serversTransport
config with a long response timeout and explicit flush interval:

```yaml
- traefik.http.serversTransports.mcp-sse.responseHeaderTimeout=0s
- traefik.http.serversTransports.mcp-sse.forwardingTimeouts.responseHeaderTimeout=0s
# attach to the server-mcp router's service
- traefik.http.services.server-mcp.loadbalancer.serversTransport=mcp-sse
```

In practice Traefik flushes streaming responses (chunked / SSE) by default and
our 15 s keepalive comments prevent idle timeouts at the entrypoint level
(`respondingTimeouts.idleTimeout` defaults to 180 s). The explicit config is
defense-in-depth so future Traefik upgrades don't silently break SSE.

### 3. Header passthrough sanity

The existing `strip-protocol-headers` middleware strips the `:protocol`
HTTP/2 pseudo-header leak — that's fine for us, and it does not touch our
`X-Myko-Tools-Allow` / `X-Myko-Tools-Deny` headers. No change needed; just
called out so the next reader knows the filter headers reach the backend.

### Required vs. optional

| Change | Required to ship? | Why |
|---|---|---|
| Dedicated `server-mcp` router | No | `/myko` prefix already matches. Add it for operability. |
| SSE serversTransport tuning | No (today) | Traefik 3.6 defaults are fine; add as defense-in-depth. |
| Header passthrough | No | Already works; just documenting it. |

### Process

This spec is the proposal. Once approved, file a corresponding PR against
`rship-control-plane` containing the `stacks/rship.yml` diff above. The
control plane is deployed via Ansible (`plays/rship.yml` → `docker stack
deploy`), so the rollout is one `r.sh` invocation after merge.

No coordination is required between the myko server release and the
control-plane change: the server can ship first (endpoint just sits behind
the existing `/myko` route), and the dedicated router can land later without
downtime.

## Open questions

None blocking. Future work:

- Reactive query → notification pushes over MCP-WS and SSE (depends on a
  MCP-friendly subscription model on top of hyphae cells).
- Session tracking via `Mcp-Session-Id` for HTTP POST if we ever need
  per-session state (WS sessions are implicit per-connection).
- HTTP keep-alive (currently `Connection: close` on every POST).
- TLS termination — currently expected at a reverse proxy.
