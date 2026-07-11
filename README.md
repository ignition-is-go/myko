# Myko

**Event-sourcing CQRS framework with reactive queries.** Define entities once in Rust; get commands, events, queries, real-time subscriptions, and cross-language bindings automatically.

License: MIT OR Apache-2.0 · Workspace version: see `Cargo.toml`

---

## Core concept

```
Command ──▶ Handler ──▶ Event ──▶ Store ──▶ Reactive Query ──▶ Client
   │                       │
   │                       └──▶ Saga ──▶ Command
   └── validation
```

- **Commands** mutate state. Synchronous validation, then events.
- **Events** are persisted (Postgres) and replicated.
- **Stores** hold current state, derived from events.
- **Queries** and **reports** are reactive views of stores.
- **Sagas** subscribe to events and emit follow-up commands.
- **Clients** subscribe to queries/reports over WebSocket and receive live diffs.

The framework handles persistence, real-time sync, federation between server peers, and type generation across language bindings.

---

## Workspace layout

```
libs/
├── myko/
│   ├── core/         Canonical Rust framework (entities, queries, commands, sagas, stores)
│   ├── macros/       #[myko_item] proc macro + relationship attributes
│   ├── server/       Tokio-based server runtime: WS gateway, Postgres backend,
│   │                 peer federation, MCP endpoint
│   ├── leptos/       Leptos integration (web UI)
│   ├── ts/           TypeScript port + generated bindings
│   ├── py/, python/  Python bindings
│   ├── cpp/          C++ bindings
│   ├── csharp/       C# bindings
│   ├── ui-svelte/    Svelte UI integration
│   ├── ui-vue/       Vue UI integration
│   └── debug/        Diagnostic tooling
└── autosocket/       Auto-reconnecting WebSocket transport (native + WASM)

docs/superpowers/specs/   Design specs for in-flight features
```

**External dependency:** [hyphae](https://github.com/ignition-is-go/hyphae) (reactive dataflow). Consumed from crates.io (`^0.5.1`).

---

## Quick start

Prerequisites: **Rust** (edition 2024), **Bun** (1.3+), **Postgres** if running the server with durability.

```bash
# Install JS deps (used for type generation, formatting, UI bindings)
bun install

# Type-check the whole workspace
cargo flux run check

# Run all tests
cargo flux run test

# Lint (cargo clippy + biome)
cargo flux run lint

# Regenerate TS bindings from Rust types
cargo flux run gen
```

[`cargo flux`](https://github.com/ignition-is-go/cargo-flux) orchestrates the polyglot workspace. Tasks are defined in `flux.toml`.

---

## Defining entities

```rust
use myko_macros::myko_item;

#[myko_item]
pub struct Target {
    pub name: String,
}
```

Auto-generated for every `myko_item`:

| Item                | Purpose                                |
| ------------------- | -------------------------------------- |
| `id: Arc<str>`      | Stable entity identity (UUID)          |
| `hash: Arc<str>`    | Content hash for change detection      |
| `GetAllTargets`     | Query — every target                   |
| `GetTargetsByIds`   | Query — by id set                      |
| `GetTargetsByQuery` | Query — filtered                       |
| `GetTargetById`     | Query — single                         |
| `CountAllTargets`   | Report — count                         |
| `DeleteTarget`      | Command — delete one                   |
| `DeleteTargets`     | Command — delete many                  |
| `PartialTarget`     | Patch type for partial updates         |

### Relationship attributes

```rust
#[myko_item]
pub struct BindingNode {
    #[belongs_to(Scene)]            // cascade delete: parent DEL → child DEL
    pub scene_id: Arc<str>,

    #[owns_many(BindingEdge)]       // parent DEL → children DEL; child DEL → parent UPDATE
    pub edges: Vec<Arc<str>>,

    #[ensure_for(Project)]          // auto-create one per Project
    pub project_id: Arc<str>,

    #[searchable]                   // index for full-text search
    pub label: String,

    #[myko_client_id]               // auto-populate with connecting WS client id
    pub created_by: Arc<str>,
}
```

---

## Running a server

```rust
use myko_server::{CellServer, postgres::PostgresConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _telemetry = myko_server::telemetry::init_from_env();

    let server = CellServer::builder()
        .with_bind_addr("0.0.0.0:5155".parse()?)
        .with_postgres(PostgresConfig::from_env()?)   // durable event log + LISTEN/NOTIFY
        .build();

    server.run().await
}
```

On startup the server logs:

```
Myko gateway: ws://0.0.0.0:5155/myko | MCP: /myko/mcp (POST + WS + SSE)
```

### Endpoints

| Path        | Method                              | Purpose                                       |
| ----------- | ----------------------------------- | --------------------------------------------- |
| `/myko`     | `GET` + WS upgrade                  | Main Myko gateway (clients, federation peers) |
| `/myko/mcp` | `POST`                              | MCP JSON-RPC (Streamable HTTP)                |
| `/myko/mcp` | `GET` + WS upgrade                  | MCP over WebSocket (subprotocol `mcp`)        |
| `/myko/mcp` | `GET` + `Accept: text/event-stream` | MCP SSE stream (keepalive in v1)              |

All endpoints share a single TCP listener; the front-door router peeks at the HTTP request line + headers and dispatches.

### Environment

| Variable                         | Default               | Description                                |
| -------------------------------- | --------------------- | ------------------------------------------ |
| `MYKO_ADDRESS`                   | `ws://localhost:5155` | Used by the stdio MCP binary and clients   |
| `MYKO_POSTGRES_URL`              | —                     | Postgres connection string                 |
| `MYKO_PORT`                      | `5155`                | Server bind port (when wired through env)  |
| `MYKO_TRACING_ENDPOINT`          | —                     | OTLP/HTTP endpoint for traces+metrics (see `myko_server::telemetry::init_from_env`); unset = local console logging only |
| `MYKO_CCMD_MONITOR`              | `0`                   | Set `1` to log command timing              |
| `MYKO_CCMD_TIMEOUT_MS`           | —                     | Slow-command threshold for warn logs       |
| `MYKO_MEM_PROFILE_INTERVAL_SECS` | `60`                  | Metrics export interval (seconds); only applies when `MYKO_TRACING_ENDPOINT` is set |

---

## Cross-language bindings

> **Rule:** Logic lives in Rust. Cross-language types are *generated*, not duplicated by hand. If a binding needs a type, add it to the Rust side and re-run `cargo flux run gen`.

| Language   | Path                       | Notes                                     |
| ---------- | -------------------------- | ----------------------------------------- |
| TypeScript | `libs/myko/ts/`            | Generated by `cargo flux run gen` (ts-rs) |
| Python     | `libs/myko/py/`, `python/` | pyproject-based                           |
| C++        | `libs/myko/cpp/`           |                                           |
| C#         | `libs/myko/csharp/`        |                                           |
| Leptos     | `libs/myko/leptos/`        | Rust web client integration               |
| Svelte     | `libs/myko/ui-svelte/`     | Wraps the TS client                       |
| Vue        | `libs/myko/ui-vue/`        | Wraps the TS client                       |

---

## Development workflow

### Iteration loop

```bash
# Fast check during edits (preferred over cargo build)
cargo check --target-dir target/claude

# Tests
cargo test -p myko-server --target-dir target/claude

# Single test
cargo test -p myko my_test_name --target-dir target/claude -- --exact --nocapture

# Lint as CI would (strict)
cargo clippy --all-targets --all-features --target-dir target/claude -- -D warnings

# Format
cargo fmt --all          # Rust
bun run format:all       # JS/TS via Prettier
```

**Always pass `--target-dir target/claude`** (or `target/agent` when scripted) so cargo's lockfile doesn't fight whatever IDE / bacon / agent loop is running. `cargo flux` uses `target/claude` by default.

### Bacon (background type checker)

`bacon.toml` is configured. `.bacon-locations` is updated by bacon — **check it before running clippy/check** to fix errors in order, since later errors often resolve when the first is fixed.

### Hot reload

Assume the user is running entities and codegen in hot-reload mode. **Do not** start dev servers, run `cargo flux run gen`, or kick off long-running tasks yourself unless explicitly asked.

### Type generation

Whenever a type that crosses the Rust↔TS boundary changes, run:

```bash
cargo flux run gen
```

This regenerates `libs/myko/ts/src/generated/`. Never hand-edit generated files.

### Conventional commits

```
feat(scope): add new feature
fix(scope):  fix a bug
chore(scope): tooling, dependencies, release plumbing
docs(scope): documentation only
refactor(scope): no behavior change
perf(scope): performance work
test(scope): tests only
```

Scopes match crate or area: `myko`, `myko-server`, `mcp`, `autosocket`, `ts`, `leptos`, `ws`, etc.

### Release

Automated. Pushing to `main` or `dev` stamps a version, tags, creates a GitHub Release, and publishes to crates.io and npm. Canary versions ship from `dev`, stable from `main`.

---

## Performance

See [`libs/myko/core/OPTIMIZATION.md`](libs/myko/core/OPTIMIZATION.md) for strategies, benchmarks, and the rationale behind the cell-based hot path.

Diagnostic features (memory profiles, ingest stats, command timing) are opt-in via env vars and have near-zero overhead when disabled.

---

## MCP — Model Context Protocol

Myko ships an MCP endpoint so AI agents can call your queries / reports / commands as tools. Two modes:

1. **In-server HTTP/WS/SSE** at `/myko/mcp` on the same port as the Myko gateway. Auto-discovered tools, per-client filtering via headers. See [Endpoints](#endpoints) above.
2. **Stdio binary** (`myko::mcp::McpServer::run_stdio`) for editor integrations that prefer stdio MCP. Connects to a running Myko server via `MYKO_ADDRESS`.

### Tool naming

| Prefix              | Source                | Example                              |
| ------------------- | --------------------- | ------------------------------------ |
| `query:`            | `QueryRegistration`   | `query:GetAllTargets`                |
| `view:`             | `ViewRegistration`    | `view:GetTargetTreeByParentFiltered` |
| `report:`           | `ReportRegistration`  | `report:CountAllTargets`             |
| `command:`          | `CommandRegistration` | `command:DeleteTarget`               |
| `connection_status` | built-in              | health check                         |

Each prefix also surfaces as a resource at `myko://schema/<prefix>/<id>` so MCP clients can fetch the input schema separately from calling the tool.

### Per-client filtering

Lock down what an MCP-client config can call without trusting the client itself. Two filter layers, both **client-configured**, composed AND. Both follow an Allow/Deny header pair with the same precedence (deny wins).

**1. Tool visibility** — glob allow/deny over tool names. A hidden tool is omitted from `tools/list` and a `tools/call` against it returns the MCP **Protocol Error** (`-32602`, `"Unknown tool: …"`) — indistinguishable from a tool that doesn't exist.

```
X-Myko-Tool-Visibility-Allow: query:*,report:*
X-Myko-Tool-Visibility-Deny:  command:Delete*
```

Patterns: `*`, `prefix*`, `*suffix`, exact.

**2. Tool callability** — per-tool, per-arg JSON value lists. Failure surfaces as an MCP **Tool Execution Error** (`isError: true` content) — the spec's "Invalid input data" category, distinct from a Protocol Error.

```
X-Myko-Tool-Callable-Allow: {"command:RunPlaybook":{"playbook_id":["site","deploy"]}}
X-Myko-Tool-Callable-Deny:  {"command:Tag":{"namespace":["prod"]}}
```

JSON shape per header: `{ "<tool_name>": { "<arg_path>": [values] } }`. Allow is positive (the arg must be present and its value must be in the list). Deny excludes (the arg's value must not be in the list).

**Stdio transport** has no headers; the same four knobs come from env vars:
- `MYKO_MCP_TOOL_VISIBILITY_ALLOW`
- `MYKO_MCP_TOOL_VISIBILITY_DENY`
- `MYKO_MCP_TOOL_CALLABLE_ALLOW` (JSON)
- `MYKO_MCP_TOOL_CALLABLE_DENY` (JSON)

Visibility applies to `tools/list`, `tools/call`, `resources/list`, `resources/read`. Callability applies only to `tools/call`.

### Connecting a client

**curl sanity check:**

```bash
curl -sS -X POST http://localhost:5155/myko/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | jq
```

**Claude Code (project `.mcp.json`)** — name filter only:

```json
{
  "mcpServers": {
    "myko": {
      "type": "http",
      "url": "http://localhost:5155/myko/mcp",
      "headers": {
        "X-Myko-Tool-Visibility-Allow": "query:*,report:*"
      }
    }
  }
}
```

**With argument allowlist** (e.g. only let the agent run two specific playbooks):

```json
{
  "mcpServers": {
    "myko-restricted": {
      "type": "http",
      "url": "http://localhost:5155/myko/mcp",
      "headers": {
        "X-Myko-Tool-Visibility-Allow": "query:*,report:*,command:RunPlaybook",
        "X-Myko-Tool-Callable-Allow": "{\"command:RunPlaybook\":{\"playbook_id\":[\"site\",\"deploy\"]}}"
      }
    }
  }
}
```

**Claude Desktop / Inspector:** point them at the same URL with the same headers.

Behind a TLS-terminating reverse proxy the public URL is `https://<host>/myko/mcp`.

---

## AI agents

This repo is set up for AI-assisted development with both [Claude Code](https://docs.claude.com/en/docs/claude-code/overview) and other agents.

- [`CLAUDE.md`](CLAUDE.md) — project-specific instructions Claude Code loads automatically.
- [`AGENTS.md`](AGENTS.md) — generic agent guidance (toolchain commands, formatting, conventions).

### The short version for agents

1. **Rust first.** New logic goes in Rust unless you're explicitly working on a legacy TS path. Generate bindings; never duplicate types by hand.
2. **`cargo check`, not `cargo build`.** Use `--target-dir target/claude` (or `target/agent`) always.
3. **Check `.bacon-locations`** before running clippy/check. Fix errors in order.
4. **Don't run `cargo flux run gen` or start dev servers yourself** — assume the user has hot reload running.
5. **No hardcoded field/type name strings.** Use macros and type constructors.
6. **Use real entities in tests.** Don't construct JSON manually.
7. **Comments explain *why*, not *what*.** Initials and tags: `// TODO(ts): ...`, `// NOTE(ts): ...`.
8. **Conventional commits** with scope; never bundle unrelated changes.

### Specs and design docs

Every non-trivial change starts with a spec under `docs/superpowers/specs/<date>-<topic>-design.md`. The spec is the source of truth; implementation plans and PRs reference it.

---

## Architecture references

| Topic                     | Where                                                                                                                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Optimization & benchmarks | [`libs/myko/core/OPTIMIZATION.md`](libs/myko/core/OPTIMIZATION.md)                                                             |
| MCP endpoint spec         | [`docs/superpowers/specs/2026-05-20-mcp-http-endpoint-design.md`](docs/superpowers/specs/2026-05-20-mcp-http-endpoint-design.md) |
| Agent instructions        | [`CLAUDE.md`](CLAUDE.md), [`AGENTS.md`](AGENTS.md)                                                                             |

---

## License

MIT OR Apache-2.0 for the framework crates (`myko`, `myko-macros`, `autosocket`).
The server runtime (`myko-server`) is AGPL-3.0-or-later. See individual crate `Cargo.toml` for specifics.
