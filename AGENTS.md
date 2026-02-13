# Agent Instructions

This file is the canonical agent guide for this repository.

## Overview

Rust-first monorepo with UI in Svelte.
TypeScript libraries exist for legacy paths but are being phased out.

Primary components:
- **Hypha**: Reactive dataflow framework for complex systems
- **Myko**: Event-sourcing CQRS framework
- **Rship**: Control platform built on Myko
- **Core principle**: Business logic belongs in Rust; cross-language types should be generated, not hand-maintained

## Issue Tracking (Beads)

This project uses **bd** (beads) for issue tracking.
Run `bd onboard` once, then `bd ready` / `bd show <id>` when starting work.

### Quick Reference

```bash
bd ready                              # Find available work
bd show <id>                          # View issue details
bd update <id> --status in_progress   # Claim work
bd create "Title" --type task --priority 2
bd close <id>                         # Complete work
bd sync --flush-only                  # Persist beads locally only
```

### Beads Workflow Rules

- **No auto-push**: Only sync/push remote when the user explicitly asks
- **Local by default**: Use `bd sync --flush-only` during normal task completion
- **Task-scoped commits**: Commit only files changed for the current task
- **Separate concerns**: Keep code commits separate from beads persistence actions

### Completing Work

```bash
git add <only-task-files>
git commit -m "feat(scope): ..."
bd close <id>
bd sync --flush-only
```

Do not run `git push` or full `bd sync` unless explicitly requested.

## Commands

```bash
# Rust (primary)
cargo check --target-dir target/agent
cargo test --target-dir target/agent -- --nocapture
cargo clippy --target-dir target/agent -- -D warnings
cargo fmt --all

# UI / TypeScript
pnpm install
pnpm dev --filter @rship/ui
pnpm build --filter <package>
pnpm format:all

# Type generation (run inside relevant crate)
cargo make gen
```

## Cargo / Clippy Guidance

- Prefer `cargo check` for validation instead of full builds during iteration
- Always use `--target-dir target/agent` for cargo commands to avoid lock contention with other tooling
- Check `.bacon-locations` before running broad clippy/check commands; fix reported errors in order
- Assume user-controlled hot reload workflows are active: do not start long-running apps unless asked

## Architecture Notes

### Myko (`libs/myko/`)
Event-sourcing CQRS flow: **Commands -> Events -> State -> Queries**

### Entities (`libs/entities/`)
Canonical entity definitions live in Rust (`libs/entities/rs/`).
TypeScript bindings are generated from Rust.

### Applications (`apps/`)
- `apps/server_rs/`: Primary Rust server
- `apps/ui/`: SvelteKit UI
- `apps/execs/`: Executor implementations

## Engineering Rules

- **Rust first**: New domain/backend logic should be implemented in Rust
- **Generated types**: Never manually duplicate cross-language types
- **No stringly typing**: Avoid hardcoded field/type names in Rust
- **Use existing patterns**: Reuse Myko logging, env guards, and debug-level conventions
- **Comments explain why**: Keep comments short and intention-focused

## Style

- Rust: `snake_case` functions/vars, `PascalCase` types
- TS/JS: `camelCase` functions/vars, `PascalCase` types/classes
- Max line length: 120 chars
- Formatting: `rustfmt` and `prettier` where applicable
- Commits: Conventional commits (`feat(scope): ...`, `fix(scope): ...`, `chore(scope): ...`)

## Migration References

- `libs/myko/rs/MIGRATION.md`
- `libs/myko/rs/OPTIMIZATION.md`
