# CLAUDE.md

## Overview

**Myko** is an event-sourcing CQRS framework with implementations in multiple languages. The canonical implementation is in Rust, with bindings/ports for TypeScript, Python, C++, C#, Leptos, Svelte, and Vue.

- **Principle**: Logic should live in Rust. Any cross-language duplication (types, validation, serialization) must be **generated**, not manually maintained.

## Commands

```bash
# Rust (primary)
cargo check                           # Fast type check (prefer over build)
cargo build --release
cargo test -- --nocapture
cargo clippy -- -D warnings
cargo fmt

# TypeScript tooling
bun install                           # Install dependencies
bun run format:all                    # Prettier

# Type generation (Rust → TypeScript)
bunx @moonrepo/cli run myko-rs:gen
```

## Cargo

Always use `--target-dir target/claude` for all cargo commands (check, clippy, build, test, etc.) to avoid lock contention with other tools.

Always assume user is running code and type generation in hot reload mode - never run apps or type generation yourself.

## Clippy

Check `.bacon-locations` for current clippy errors before running clippy or cargo check yourself. Bacon keeps this file updated. Always fix errors in order, since errors later in the list may be resolved by fixing the first.

## Architecture

### Myko Framework (`/libs/myko/`)

Event-sourcing CQRS framework. Pattern: **Commands → Events → State → Queries**

**Rust (primary)**:

- **myko-rs** (`libs/myko/rs`): Rust server/client — the canonical implementation
- **myko-macros** (`libs/myko/macros`): `#[myko_item]` generates queries, reports, commands
- **myko-server** (`libs/myko/server`): Server runtime — WebSocket, Kafka, peer federation
- **myko-leptos** (`libs/myko/leptos`): Leptos/web-ui integration

**Other languages** (`libs/myko/ts`, `libs/myko/py`, `libs/myko/cpp`, etc.): Ports/bindings of the framework.

### Autosocket (`/libs/autosocket/`)

Auto-reconnecting WebSocket transport adapters shared by myko clients. Supports native and WASM targets.

### Hyphae (external dependency)

Reactive dataflow framework. Referenced as a path dependency at `../hyphae/hyphae`.

### Commits

[Conventional Commits](https://www.conventionalcommits.org/): `feat(scope):`, `fix(scope):`, `chore(scope):`

### Comments

Include initials: `// TODO(ts): ...` or `// NOTE(ts): ...`

### Formatting

- Rust: `rustfmt`
- JS/TS: `prettier` with `prettier-plugin-organize-imports`
- Lines under 120 chars
- Comments explain _why_, not _what_

### Naming

- Rust: `snake_case` vars/functions, `PascalCase` structs/enums
- JS/TS: `camelCase` vars/functions, `PascalCase` classes/types

## Key Patterns

### Event Sourcing + CQRS

1. Command sent → 2. Handler validates → 3. Events persisted to Kafka → 4. Sagas react → 5. Queries return state

### Myko Item Macro

```rust
#[myko_item]
pub struct Target {
    pub name: String,
    // id: Arc<str> and hash: Arc<str> added automatically
}
```

Auto-generates: `GetAllTargets`, `GetTargetsByIds`, `GetTargetsByQuery`, `CountAllTargets`, `GetTargetById`, `DeleteTarget`, `DeleteTargets`, `PartialTarget`

### Relationship Attributes

```rust
#[belongs_to(Scene)]      // Parent DEL → cascade delete child
#[owns_many(BindingNode)] // Parent DEL → delete children; Child DEL → update parent
#[ensure_for(Project)]    // Auto-create entity per dependency
#[searchable]             // Full-text search indexing
#[myko_client_id]         // Auto-populate with WebSocket client ID
```

## Important Notes

- **Rust First**: New logic belongs in Rust. TypeScript is for bindings and legacy support only.
- **Generated Types**: Never manually maintain duplicate types — generate TS from Rust definitions.
- **Type Generation**: `moon run myko-rs:gen` generates TS bindings.
- **JS Runtime**: Bun, not Node.js.
- **Package Manager**: bun workspaces

### Prefer Existing Patterns

Check for established patterns before suggesting new ones. Project has: MykoLogger, env guards, debug log levels.

### Performance-Conscious Defaults

Diagnostic features: opt-in via env vars, debug log levels, minimal overhead when disabled.

### Rust Guidelines

- Use `cargo check` not `cargo build` for type checking
- No hardcoded field/type name strings
- Use real entities with macros in tests
- Don't construct JSON manually — use type constructors
- New features should be implemented in Rust, not TypeScript

### Cross-Language Code Generation

When types or logic need to exist in multiple languages:

1. Define the canonical version in Rust
2. Use code generation to produce TypeScript equivalents
3. Never manually duplicate — if generation doesn't exist, add it

## Performance

See `libs/myko/rs/OPTIMIZATION.md` for optimization strategies and benchmarks.
