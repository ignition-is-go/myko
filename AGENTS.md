# Agent Instructions

Myko event-sourcing CQRS framework. Rust-first with multi-language bindings (TS, Python, C++, C#, Leptos, Svelte, Vue). Generate cross-language types from Rust instead of duplicating them by hand.

## Rule Sources

- This `AGENTS.md` is the primary repo-specific guide.

## Quick Context

- Core: Myko (event-sourcing/CQRS framework), Autosocket (WebSocket transport), Hyphae (external reactive dataflow dep)
- Important paths: `libs/myko/core/`, `libs/myko/macros/`, `libs/myko/server/`, `libs/myko/leptos/`, `libs/autosocket/`

## Core Rules

- Rust first unless you are working inside an established legacy TS path.
- Reuse existing Myko patterns for registration, logging, env loading, and server wiring.
- All formatting and testing should go through `cargo flux` tasks, not direct formatter/test runner commands.
- Comments should explain why, not what.
- Use TODO/NOTE sparingly and include initials or an issue tag, e.g. `TODO(ts): ...`.

## Workspace Commands

```bash
bun install
cargo flux run check
```

- Prefer `cargo flux run <task>` over calling underlying package scripts directly.

## Rust Commands

```bash
cargo check --target-dir target/agent
cargo test --target-dir target/agent -- --nocapture
cargo clippy --target-dir target/agent -- -D warnings
cargo fmt --all
```

- Always use `--target-dir target/agent` to avoid lock contention.
- Prefer `cargo check` during iteration.
- Check `.bacon-locations` before broad Rust validation.

## Type Generation

Run inside the relevant crate when changing exported Rust types:

```bash
cargo flux run gen
```

## Single-Test Commands

### Rust

```bash
# Single crate
cargo test -p myko --target-dir target/agent -- --nocapture

# Single inline/unit test by exact name
cargo test -p myko my_test_name --target-dir target/agent -- --exact --nocapture
```

## Validation Guidance

- Rust-only change: usually run `cargo check -p <crate> --target-dir target/agent`.
- Rust behavior change: prefer `cargo flux run test`; if you need narrower coverage, run `cargo test -p <crate>` directly.
- Do not start long-running dev servers unless the user asks.

## Formatting And Imports

### Rust

- `rustfmt.toml` uses edition `2024`.
- Import groups are `std`, external crates, then local crate/modules.
- Imports are reordered automatically and merged at crate granularity.

### TypeScript

- Root Prettier rules: 2 spaces, single quotes, no semicolons, trailing commas, width 80.
- `prettier-plugin-organize-imports` sorts imports automatically.
- Biome lints JS/TS; Prettier formats it.

## Naming And File Conventions

- Rust: `snake_case` functions/modules/variables, `PascalCase` types/traits, `SCREAMING_SNAKE_CASE` constants.
- TS: `camelCase` functions/variables, `PascalCase` classes/types.
- Keep TS tests near the code they cover; Rust tests may be inline or under `tests/`.

## Types And Architecture

- Canonical domain types live in Rust under `libs/myko/core/`.
- Generate TS bindings from Rust exports instead of duplicating shapes manually.
- Prefer explicit types on public APIs and non-obvious return values.

## Error Handling

### Rust

- `anyhow` is the common application-level error type.
- Prefer `anyhow::Result<T>` or a local crate result alias if one exists.
- Add `.context(...)` / `.with_context(...)` for IO, parsing, subprocess, and network failures.
- Use `bail!` / `anyhow!` for clear early exits.

## Testing Conventions

- Rust uses `#[test]`, `#[tokio::test]`, inline test modules, and integration tests.
- Favor narrow, behavior-focused tests over broad end-to-end setups.

## Commit Expectations

- Use conventional commits: `feat(scope): ...`, `fix(scope): ...`, `chore(scope): ...`.
- Keep commits task-scoped; do not bundle unrelated files.
- Do not push or create remote PR side effects unless explicitly asked.

## Useful References

- `libs/myko/core/OPTIMIZATION.md`
