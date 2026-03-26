# CLAUDE.md

## Overview

**Rust-first monorepo** with UI in Svelte. TypeScript libraries exist to support the legacy implementation but are being phased out.

This repo contains:

- **Hyphae**: A reactive dataflow framework for building complex systems
- **Myko**: An event-sourcing CQRS framework (the reusable library)
- **Rship** (Rocketship): A control platform for orchestrating reactive event relationships in multimedia systems (the main product, built on Myko)
- **Principle**: Logic should live in Rust. Any cross-language duplication (types, validation, serialization) must be **generated**, not manually maintained.

## Commands

```bash
# Rust (primary)
cargo check                           # Fast type check (prefer over build)
cargo build --release
cargo test -- --nocapture
cargo clippy -- -D warnings
cargo fmt

# UI & TypeScript tooling
bun install                           # Install dependencies
bun run --filter @rship/ui dev        # UI dev server
bun run --filter <package> build      # Build package
bun run format:all                    # Prettier

# Type generation (Rust → TypeScript, run from respective directories)
bunx @moonrepo/cli run myko-rs:gen
bunx @moonrepo/cli run entities-rs:gen

# Legacy server (being replaced)
bun run --filter @rship/server dev    # Bun server (MYKO_PORT=5155)
```

## Cargo

Always use `--target-dir target/claude` for all cargo commands (check, clippy, build, test, etc.) to avoid lock contention with other tools.

Always assume user is running code and type generation in hot reload mode - never run apps or type generation yourself.

## Clippy

Check `.bacon-locations` for current clippy errors before running clippy or cargo check yourself. Bacon keeps this file updated. always fix errors in order, since errors later in the list may be resolved by fixing the first

## Architecture

### Myko Framework (`/libs/myko/`)

Event-sourcing CQRS framework. Pattern: **Commands → Events → State → Queries**

**Rust (primary)**:

- **@myko/rs**: Rust server/client with `ractor` actors - the canonical implementation
- **@myko/macros**: `#[myko_item]` generates queries, reports, commands

### Rship Entities (`/libs/entities/`)

Rship domain entities built on Myko, defined in Rust (`/libs/entities/rs/`), with TypeScript types **generated** from Rust definitions.

### Rship Executor SDK (`/libs/sdk/`)

Multi-language executor SDK. Executors connect via WebSocket to declare Targets/Emitters/Actions, push Pulses, receive Actions.

### Applications (`/apps/`)

- **rship_server**: Rust server (primary) - `apps/server/`
- **ui**: Svelte 5 + SvelteKit, Tailwind 4 + daisyUI 5
- **execs**: 15+ executor implementations

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

- **Rust First**: New logic belongs in Rust. TypeScript is for UI and legacy support only.
- **Generated Types**: Never manually maintain duplicate types - generate TS from Rust definitions.
- **Type Generation**: `moon run myko-rs:gen` and `moon run entities-rs:gen` generate TS bindings.
- **JS Runtime**: Bun, not Node.js (for legacy server and tooling).
- **Package Manager**: bun workspaces
- **Submodules**: Auto-updated via preinstall hook

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

## Migration Guide

See `libs/myko/rs/MIGRATION.md` for TypeScript to Rust migration details.

## Performance

See `libs/myko/rs/OPTIMIZATION.md` for optimization strategies and benchmarks.
