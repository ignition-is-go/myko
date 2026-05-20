# MCP Endpoint at `/myko/mcp`

**Status:** Shipped — `feat/mcp-http-endpoint`, PR #15
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
| `libs/myko/server/src/mcp/exec.rs` | new | `Executor` enum: `Client(MykoClient)` (stdio) or `InProcess(CellServerCtx)` (HTTP). Implements `execute_query`, `execute_view`, `execute_report`, `execute_command`. |
| `libs/myko/server/src/mcp/filter.rs` | new | `ClientFilters` — two layers (visibility globs, callability JSON), parsed from the four `X-Myko-Tool-{Visibility,Callable}-{Allow,Deny}` headers (or `MYKO_MCP_TOOL_*` env vars for stdio). |
| `libs/myko/server/src/mcp/http.rs` | new | POST handler (JSON dispatch) + GET handler (SSE stream). |
| `libs/myko/server/src/mcp/ws.rs` | new | WebSocket MCP loop: read text frames, dispatch, write text frames. Reuses tungstenite handshake helpers + `WebSocketStream::from_raw_socket`. |
| `libs/myko/server/src/mcp/server.rs` | refactor | `run_stdio` now wraps `dispatch` with `Executor::Client`. Old `RequestHandler` / `tool_executor` / `execute_*` move into `dispatch.rs` + `exec.rs`. |
| `libs/myko/server/src/ws_handler.rs` | minor | Split `handle_connection` so callers can pass an already-buffered prefix (the bytes the router peeked) into the WS handshake. |
| `libs/myko/server/src/lib.rs` | minor | `run_ws_accept_loop` → `run_accept_loop`, dispatches through the router. |
| `libs/myko/server/Cargo.toml` | minor | Add `httparse` (transitive today; pull in directly). `tungstenite` already provides handshake helpers; no new crypto deps. |

### HTTP front-door (router.rs)

1. `read_request_head` reads chunks off the raw `TcpStream` into a growing
   buffer until `\r\n\r\n` appears, capped at 8 KB. Anything past the
   header terminator (e.g. a POST body that arrived in the same TCP
   segment) is captured as `leftover_body` on the parsed head.
2. Parse method + path + headers with `httparse`.
3. Route:
   - `GET` + path == `/myko` + WS upgrade → `mcp::ws::handle_myko_ws_upgrade`,
     which writes the 101 response (computes `Sec-WebSocket-Accept` via
     `tungstenite::handshake::derive_accept_key`), wraps the stream with
     `WebSocketStream::from_raw_socket(_, Role::Server, _)`, and hands off
     to `WsHandler::handle_upgraded`.
   - `POST` + path == `/myko/mcp` → read up to `Content-Length` bytes
     (continuing from `leftover_body`; cap 1 MB), parse JSON-RPC, call
     `mcp::http::handle_post`.
   - `GET` + path == `/myko/mcp` + WS upgrade →
     `mcp::ws::handle_mcp_ws_upgrade` (echoes `Sec-WebSocket-Protocol: mcp`
     if requested, wraps stream, runs the JSON-RPC text-frame loop).
   - `GET` + path == `/myko/mcp` + `Accept: text/event-stream` →
     `mcp::http::handle_sse`.
   - `GET /myko/mcp` with no upgrade / SSE accept → a small status JSON
     so the URL is friendly in a browser.
   - Else → write `404 Not Found` and close.

Errors during HTTP parsing → write `400 Bad Request` and close.
Every HTTP response path calls `shutdown_cleanly()` before drop.

### Dispatch core (dispatch.rs)

```rust
pub async fn handle_request(
    req: McpRequest,
    filter: &ClientFilters,
    executor: &Executor,
    info: &ServerInfo,
) -> Option<McpResponse>
```

Methods handled: `initialize`, `notifications/initialized`,
`notifications/cancelled`, `tools/list`, `tools/call`, `resources/list`,
`resources/read`. Returns `None` for notifications (no response expected).
Behavior:

- `tools/list` and `resources/list` run each candidate through
  `filter.tool_visible(name)`; denied entries are omitted entirely.
- `tools/call` first checks `tool_visible`; on denial returns an MCP
  **Protocol Error** `{ code: -32602, message: "Unknown tool: <name>" }`,
  matching the spec's example. Then runs `tool_callable(name, args)`;
  on denial returns an MCP **Tool Execution Error** (`result.isError = true`
  with the constraint message as the content text).
- `resources/read` checks `tool_visible` and returns the JSON schema for
  the underlying tool.
- Tool execution dispatches through `Executor` instead of calling
  `MykoClient` directly.

### Executor (exec.rs)

```rust
pub enum Executor {
    Client(Arc<MykoClient>),
    InProcess(Arc<CellServerCtx>),
}

impl Executor {
    pub async fn execute_query(&self, id: &str, args: Value)   -> Result<Value, String>;
    pub async fn execute_view(&self, id: &str, args: Value)    -> Result<Value, String>;
    pub async fn execute_report(&self, id: &str, args: Value)  -> Result<Value, String>;
    pub async fn execute_command(&self, id: &str, args: Value) -> Result<Value, String>;
    pub fn connection_status(&self) -> Value;
}
```

`Executor::Client` is what `run_stdio` builds — it wraps the existing
`MykoClient` raw watchers (`watch_query_raw`, `watch_view_raw`,
`watch_report_raw`, `send_command_raw_result`). A new
`MykoClient::watch_view_raw` was added so the stdio path can call views
too (mirrors `watch_query_raw`; sends `MykoMessage::View` and cancels
with `ViewCancel`).

`Executor::InProcess` paths execute directly against the registry,
following the same code paths the WS handler uses:

- Query / View: `handler_registry.get_query(id)` /
  `handler_registry.get_view(id)`, build the wrapped payload with a fresh
  `tx` and `createdAt`, build a `RequestContext::internal`, run the
  `cell_factory` to produce a `FilteredCellMap`, return its `snapshot()`.
- Report: `handler_registry.get_report(id)`, run, subscribe to capture the
  first emission with a timeout, return value.
- Command: find the `CommandHandlerRegistration`, build `CommandContext`
  with internal `RequestContext`, call `execute_from_value`, return the
  response or error.

For v1 we use one-shot execution with a timeout (same 5 s / 10 s caps as
stdio). Reactive subscriptions over SSE are deferred.

### Client filters (filter.rs)

Per-client filtering has two complementary layers, both client-configured,
mapped to the two error categories defined in the
[MCP 2025-06-18 spec][mcp-tool-errors]:

```rust
pub struct ClientFilters {
    visibility_allow: Vec<Pattern>,
    visibility_deny:  Vec<Pattern>,
    callable_allow:   HashMap<String, HashMap<String, Vec<Value>>>,
    callable_deny:    HashMap<String, HashMap<String, Vec<Value>>>,
}

pub fn from_strings(
    visibility_allow: Option<&str>,
    visibility_deny: Option<&str>,
    callable_allow_json: Option<&str>,
    callable_deny_json: Option<&str>,
) -> Self;

pub fn tool_visible(&self, name: &str) -> bool;
pub fn tool_callable(&self, name: &str, args: &Value) -> Result<(), String>;
```

[mcp-tool-errors]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools#error-handling

**1. Tool visibility** — glob allow/deny over tool names. Patterns are
comma-separated globs: `*` (any), `prefix*`, `*suffix`, exact. Deny wins.
A hidden tool is omitted from `tools/list` / `resources/list`; a
`tools/call` against it returns an MCP **Protocol Error**
`{ "code": -32602, "message": "Unknown tool: …" }` — indistinguishable on
the wire from a tool that doesn't exist.

Sources:
- HTTP/WS: `X-Myko-Tool-Visibility-Allow` and `X-Myko-Tool-Visibility-Deny`
  request headers.
- Stdio: `MYKO_MCP_TOOL_VISIBILITY_ALLOW` / `MYKO_MCP_TOOL_VISIBILITY_DENY`
  env vars.

**2. Tool callability** — per-tool, per-argument JSON value lists.
Failure surfaces as an MCP **Tool Execution Error** (`isError: true`
content carrying a short descriptive message), the spec's "Invalid input
data" category — distinct from a Protocol Error.

JSON shape per header: `{ "<tool>": { "<arg>": [values] } }`. Allow is
positive (the arg must be present on the call and its value must appear
in the list). Deny excludes (if the value appears, the call is rejected).
Deny wins.

Sources:
- HTTP/WS: `X-Myko-Tool-Callable-Allow` and `X-Myko-Tool-Callable-Deny`
  request headers (JSON).
- Stdio: `MYKO_MCP_TOOL_CALLABLE_ALLOW` / `MYKO_MCP_TOOL_CALLABLE_DENY`
  env vars (JSON).

Examples:

- `X-Myko-Tool-Visibility-Allow: query:*,report:*` +
  `X-Myko-Tool-Visibility-Deny: query:Get*Internal` — every query and
  report is callable, except queries named like `GetSomethingInternal`.
- `X-Myko-Tool-Callable-Allow: {"command:RunPlaybook":{"playbook_id":["site","deploy"]}}` —
  when calling `command:RunPlaybook`, `playbook_id` must be `"site"` or
  `"deploy"`.

Filters are parsed per-request (stateless POST). For SSE GET and WS
connections, we capture them at handshake time and reuse for every
request / pushed message over that connection's lifetime.

### POST handler (http.rs)

```
POST /myko/mcp HTTP/1.1
Content-Type: application/json
X-Myko-Tool-Visibility-Allow: query:*,report:*

{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query:GetAllTargets","arguments":{}}}
```

Flow:
1. Read body up to `Content-Length` (cap 1 MB → `413` over limit).
2. Parse `McpRequest`. Parse errors → JSON-RPC error response with `id: null`, code `-32700`.
3. Parse `ClientFilters` from headers.
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
X-Myko-Tool-Visibility-Allow: query:*,report:*
```

Flow:
1. Router has already parsed the request. Validate `Sec-WebSocket-Version: 13`
   and `Sec-WebSocket-Key` is present.
2. Compute `Sec-WebSocket-Accept` via
   `tungstenite::handshake::derive_accept_key`.
3. Parse `ClientFilters` from headers; capture for the connection.
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
X-Myko-Tool-Visibility-Allow: query:*

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

No feature flag. The MCP endpoint is part of the server runtime and is
hosted by every `CellServer`. The HTTP pre-parse on the WS hot path is one
line read + an `Upgrade:` header check — negligible. There is no
separate-port option, and no way to disable the endpoint short of fronting
the server with a proxy that rejects `/myko/mcp`.

Operators who want to lock the endpoint down at the network edge can do so
in Traefik (IP allowlist middleware on the dedicated `server-mcp` router —
see the Deployment section). Operators who want to lock it down for a
specific client send the `X-Myko-Tool-Visibility-Allow` / `X-Myko-Tool-Visibility-Deny`
headers via that client's MCP config.

## Data flow examples

**Tool call (HTTP):**
1. Client `POST /myko/mcp` with `tools/call` body, headers
   `X-Myko-Tool-Visibility-Allow: query:*,command:RunPlaybook` and
   `X-Myko-Tool-Callable-Allow: {"command:RunPlaybook":{"playbook_id":["site"]}}`.
2. Router parses HTTP head, dispatches to `mcp::http::handle_post`.
3. `handle_post` reads the body, builds `ClientFilters` from headers,
   calls `dispatch::handle_request`.
4. `dispatch` checks `tool_visible("command:RunPlaybook")` (passes),
   then `tool_callable("command:RunPlaybook", args)` (passes iff
   `args.playbook_id == "site"`).
5. `Executor::InProcess` runs the command against the registry.
6. Result → `McpResponse` → JSON → HTTP body. `shutdown_cleanly()`
   flushes and drains the socket before drop.

**WS connection (unchanged from client's POV):**
1. Client `GET /myko` with `Upgrade: websocket`.
2. Router parses the request line + headers, sees WS upgrade.
3. Calls `mcp::ws::handle_myko_ws_upgrade`, which performs the WS
   handshake (writes the 101 + `Sec-WebSocket-Accept`), wraps the
   stream as `WebSocketStream::from_raw_socket`, and hands off to
   `WsHandler::handle_upgraded`.
4. Existing WS handler logic runs.

## Error handling

Two MCP error categories ([2025-06-18 spec][mcp-tool-errors]):

- **Protocol Error** — JSON-RPC error response. Used for:
  - HTTP parse error → `400 Bad Request`, body empty, connection drained then closed.
  - Body over 1 MB → `413 Payload Too Large`.
  - JSON-RPC parse error → `200 OK` with JSON-RPC error response (code `-32700`).
  - Unknown JSON-RPC method → JSON-RPC error `-32601` ("Method not found").
  - Tool hidden by visibility filter → JSON-RPC error `-32602`, message
    `"Unknown tool: <name>"` — indistinguishable on the wire from a tool
    that doesn't exist (spec example wording).

- **Tool Execution Error** — JSON-RPC success response carrying
  `result.content` + `result.isError = true`. Used for:
  - Tool denied by callability filter → message is the constraint
    rejection text (e.g. `"argument \`playbook_id\` value not in allowlist"`).
  - Tool execution failure / timeout downstream → message describes the
    failure.

- Connection-level: on every HTTP response path the server calls
  `shutdown_cleanly()` (shutdown the write half, drain the read half for
  up to 250 ms) so HTTP/1.1 keep-alive clients don't see ECONNRESET.
- Internal panic in executor → caught by the Tokio task boundary, logged.

## Testing

Unit coverage in `myko-server` (33 tests on the branch):

- `router` parses correct method/path/headers across line-boundary splits.
- `ClientFilters` visibility patterns (prefix, suffix, exact, allow/deny precedence).
- `ClientFilters` callability constraints (allow/deny per arg, missing arg
  rejected, deny wins, malformed JSON ignored).
- `dispatch` filter application on `tools/list`; `initialize` + unknown
  method responses; notifications produce no response.
- `mcp::http::filter_from_head` round-trips all four header parsers.

End-to-end checks left for manual verification (test plan in the PR):
- `tools/list` over HTTP returns the catalog; filter headers trim it.
- `tools/call` against a query / view / report / command returns items.
- Callability-deny rejects matching calls with `isError: true`.
- Existing `ws://host:5155/myko` clients keep working (no regression).

## Migration

- No API breakage: existing WS clients connect to `ws://host:5155/myko` exactly
  as before.
- The legacy stdio binary (`myko::mcp::McpServer::run_stdio`) keeps working as
  a transitional path while consumers migrate to the in-server endpoint.

## Deployment notes (generic)

The endpoint shares the existing Myko TCP listener, so any reverse proxy that
already forwards `/myko` to the server will also forward `/myko/mcp`. Common
operational considerations:

- **TLS termination** at the proxy; the server speaks plain HTTP/WS.
- **SSE-friendly forwarding**: most proxies stream chunked responses by
  default, but verify there is no idle-connection timeout shorter than the
  15 s SSE keepalive comment we emit.
- **WebSocket upgrade** is transparent in mainstream proxies (Traefik, nginx,
  Envoy, Caddy). No special config needed beyond the usual `Upgrade` /
  `Connection` header forwarding.
- **Tool filter headers**: confirm the proxy doesn't strip
  `X-Myko-Tool-Visibility-Allow` / `X-Myko-Tool-Visibility-Deny`. Most proxies pass arbitrary
  request headers through by default.

For lockdown, attach proxy-level middlewares (IP allowlist, rate limit,
forward-auth) to a dedicated route for `/myko/mcp` so they don't affect the
main Myko WS gateway.

## Open questions

None blocking. Future work:

- Reactive query → notification pushes over MCP-WS and SSE (depends on a
  MCP-friendly subscription model on top of hyphae cells).
- Session tracking via `Mcp-Session-Id` for HTTP POST if we ever need
  per-session state (WS sessions are implicit per-connection).
- HTTP keep-alive (currently `Connection: close` on every POST).
- TLS termination — currently expected at a reverse proxy.
