# CLAUDE.md

## Overview

**Rust-first monorepo** with UI in Svelte. TypeScript libraries exist to support the legacy implementation but are being phased out.

This repo contains:

- **Myko**: An event-sourcing CQRS framework (the reusable library)
- **Rship** (Rocketship): A control platform for orchestrating reactive event relationships in multimedia systems (the main product, built on Myko)

**Principle**: Logic should live in Rust. Any cross-language duplication (types, validation, serialization) must be **generated**, not manually maintained.

**Rship Core Concept**: **Services** run **Executors** that connect via WebSocket. Executors publish **Targets** with **Emitters** (state) and **Actions** (commands). **Bindings** define reactive relationships, organized into **Scenes** and **Calendars**.

## Commands

```bash
# Rust (primary)
cargo check                           # Fast type check (prefer over build)
cargo build --release
cargo test -- --nocapture
cargo clippy -- -D warnings
cargo fmt

# UI & TypeScript tooling
pnpm install                          # Install dependencies
pnpm dev --filter @rship/ui           # UI dev server
pnpm build --filter <package>         # Build package
pnpm format:all                       # Prettier

# Type generation (Rust → TypeScript)
pnpm --filter @rship/entities gen     # Generate TS types from Rust entities

# Legacy server (being replaced)
pnpm dev --filter @rship/server       # Bun server (MYKO_PORT=5155)
```

## Cargo

Always use `--target-dir target/claude` for all cargo commands (check, clippy, build, run, test, etc.) to avoid lock contention with other tools.

## Clippy

Check `.bacon-locations` for current clippy errors before running clippy or cargo check yourself. Bacon keeps this file updated. always fix errors in order, since errors later in the list may be resolved by fixing the first

## Architecture

### Myko Framework (`/libs/myko/`)

Event-sourcing CQRS framework. Pattern: **Commands → Events → State → Queries**

**Rust (primary)**:

- **@myko/rs**: Rust server/client with `ractor` actors - the canonical implementation
- **@myko/macros**: `#[myko_item]` generates queries, reports, commands

**TypeScript (legacy/UI support)**:

- **@myko/core**: MItem, MEvent, MCommand, MQuery, MSaga - being superseded by Rust
- **@myko/ws**: WebSocket + MessagePack client
- **@myko/ui-svelte**: Svelte 5 reactive bindings for Myko queries/reports

### Rship Entities (`/libs/entities/`)

Rship domain entities built on Myko, defined in Rust (`/libs/entities/rs/`), with TypeScript types **generated** from Rust definitions.

Entities: Target, Instance, Machine, Emitter, Action, Binding, Scene, Calendar, EventTrack.

BindingNode trees: Expression → Condition → Constraint → Delay → Action

### Rship Executor SDK (`/libs/sdk/`)

Multi-language executor SDK. Executors connect via WebSocket to declare Targets/Emitters/Actions, push Pulses, receive Actions.

### Applications (`/apps/`)

- **server_rs**: Rust server (primary) - `apps/server_rs/`
- **server**: Bun-based legacy server - being replaced by server_rs
- **ui**: Svelte 5 + SvelteKit, Tailwind 4 + daisyUI 5
- **execs**: 15+ executor implementations

## Project Structure

```
/apps/                # Rship applications
├── server_rs/        # Rust server (primary)
├── server/           # Bun server (legacy)
├── ui/               # Svelte 5 UI
├── execs/            # Executors
└── asset_store/      # Asset service

/libs/
├── myko/             # Myko framework
│   ├── rs/           # Rust framework (primary)
│   ├── macros/       # Proc macros for #[myko_item]
│   ├── ui-svelte/    # Svelte reactive bindings
│   ├── core/         # TS core (legacy)
│   └── ws/           # TS WebSocket client
├── entities/         # Rship entities (built on Myko)
│   └── rs/           # Rust entities → generates TS types
├── sdk/              # Rship executor SDK (6 languages)
├── link/             # gRPC RPC layer
└── asset-store/      # S3-compatible storage
```

## Code Style

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

### Actor Model (@myko/rs)

Key actors: EventManager, QueryManager, ReportManager, CommandManager, WebSocketServer, SagaManager, RelationshipManager, PeerManager

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

- **Rust First**: New logic belongs in Rust. TypeScript is for UI and legacy support only.
- **Generated Types**: Never manually maintain duplicate types - generate TS from Rust definitions.
- **Type Generation**: `pnpm --filter @rship/entities gen` generates TS bindings from Rust.
- **JS Runtime**: Bun, not Node.js (for legacy server and tooling).
- **Package Manager**: pnpm workspaces
- **Submodules**: Auto-updated via preinstall hook

## Code Guidelines

### Respect Opt-In Patterns

Use env guards for diagnostic features, even debug-only:

```typescript
if (process.env['MEMORY_MONITOR'] !== 'true') return
```

Guards prevent overhead when disabled, not just log filtering.

### Commit Organization

Separate bug fixes from improvements. Fix all instances of systemic issues in one commit.

### Formatting

Let prettier/rustfmt handle it. Don't manually match formatting.

### Prefer Existing Patterns

Check for established patterns before suggesting new ones. Project has: MykoLogger, env guards, debug log levels.

### Performance-Conscious Defaults

Diagnostic features: opt-in via env vars, debug log levels, minimal overhead when disabled.

### URL Path Design

Use query params for identifiers with special chars (reverse proxies decode `%2F` in paths):

```
GET /asset?key=folder%2Ffile.png  # Correct
GET /assets/folder%2Ffile.png     # Breaks with Traefik
```

### Rust Guidelines

- Use `cargo check` not `cargo build` for type checking
- No hardcoded field/type name strings
- Use real entities with macros in tests
- Don't construct JSON manually - use type constructors
- New features should be implemented in Rust, not TypeScript

### Cross-Language Code Generation

When types or logic need to exist in multiple languages:

1. Define the canonical version in Rust
2. Use code generation to produce TypeScript equivalents
3. Never manually duplicate - if generation doesn't exist, add it

Entity generation: `libs/entities/rs/src/bin/typegen.rs` produces TypeScript types.

## Beads Workflow

This project uses Beads for issue tracking. Follow these rules:

- **No auto-push**: Only sync with remote (`bd sync`) when explicitly asked
- **Local-only by default**: Use `bd sync --flush-only` to save changes to JSONL without git operations
- **Task-scoped commits**: When completing a task, only commit files changed for that specific task
- **Separate concerns**: Commit code changes separately from beads changes

When completing work:

```bash
git add <only-task-files>           # Stage only files for this task
git commit -m "feat(scope): ..."    # Commit code changes
bd sync --flush-only                # Export beads to JSONL (no commit/push)
```

Only run `bd sync` (full sync with push) when explicitly requested.

## Migration Guide

See `libs/myko/rs/MIGRATION.md` for TypeScript to Rust migration details.

## Performance

See `libs/myko/rs/OPTIMIZATION.md` for optimization strategies and benchmarks.
