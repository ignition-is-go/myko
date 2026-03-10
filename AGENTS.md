# Agent Instructions

Rust-first monorepo with Svelte/SvelteKit UI. Prefer Rust for new backend/domain behavior, and generate cross-language types from Rust instead of duplicating them by hand.

## Rule Sources

- This `AGENTS.md` is the primary repo-specific guide.
- No `.cursor/rules/`, `.cursorrules`, or `.github/copilot-instructions.md` files were present when this file was updated.

## Quick Context

- Core systems: Hypha (reactive dataflow), Myko (event-sourcing/CQRS), Rship (platform on Myko).
- Important paths: `apps/server/`, `apps/ui/`, `apps/execs/`, `libs/myko/rs/`, `libs/entities/rs/`, `libs/sdk/rs/`, `libs/sdk/ts/`, `tools/`.
- This repo uses `bd`; close finished local work with `bd close <id>` then `bd sync --flush-only`.

## Core Rules

- Rust first unless you are working inside an established legacy TS path.
- Never hand-maintain TS mirrors of Rust entity types.
- Reuse existing Myko patterns for registration, logging, env loading, and server wiring.
- All formatting and testing should go through Moon tasks, not direct formatter/test runner commands.
- Comments should explain why, not what.
- Use TODO/NOTE sparingly and include initials or an issue tag, e.g. `TODO(ts): ...`.

## Workspace Commands

```bash
moon run root:install
moon run root:check
```

- Prefer `moon run <project>:<task>` over calling underlying package scripts directly.
- Use Moon for formatting/testing; if a needed formatter or test flow is not exposed yet, add/update the Moon task instead of standardizing on the raw command.

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

## UI Commands

```bash
moon run ui:dev
moon run ui:build
moon run ui:check
```

## Type Generation

Run inside the relevant crate when changing exported Rust types:

```bash
moon run myko-rs:gen
moon run entities-rs:gen
```

## Single-Test Commands

### Rust

```bash
# Single crate
cargo test -p rship-server --target-dir target/agent -- --nocapture

# Single inline/unit test by exact name
cargo test -p rship-server my_test_name --target-dir target/agent -- --exact --nocapture

# Single integration test file
cargo test -p rship-entities-engine --test cue_engine_test --target-dir target/agent -- --nocapture
```

### Vitest

```bash
# Use a Moon task when available.
# If targeted Vitest execution is needed and no Moon task exists,
# add/update the Moon task rather than relying on raw bun commands.
```

### Playwright

```bash
# Use a Moon task when available.
# If targeted Playwright execution is needed and no Moon task exists,
# add/update the Moon task rather than relying on raw bun commands.
```

- `apps/ui/vitest.config.ts` includes `src/**/*.{test,spec}.{js,ts}`.
- `apps/ui/playwright.config.ts` defaults to Chromium.
- `libs/sdk/ts` also uses Vitest and should be routed through Moon tasks.

## Validation Guidance

- Rust-only change: usually run `cargo check -p <crate> --target-dir target/agent`.
- Rust behavior change: prefer `moon run <project>:test`; if you need narrower coverage, expose that scoped command through Moon.
- UI change: usually run `moon run ui:check` plus the narrowest matching Moon-backed test task.
- Do not start long-running dev servers unless the user asks.

## Formatting And Imports

### Rust

- `rustfmt.toml` uses edition `2024`.
- Import groups are `std`, external crates, then local crate/modules.
- Imports are reordered automatically and merged at crate granularity.

### TypeScript / Svelte

- Root Prettier rules: 2 spaces, single quotes, no semicolons, trailing commas, width 80.
- `prettier-plugin-organize-imports` sorts imports automatically.
- `apps/ui/.prettierrc` also enables `prettier-plugin-tailwindcss`.
- Biome lints JS/TS; Prettier formats it.

## Naming And File Conventions

- Rust: `snake_case` functions/modules/variables, `PascalCase` types/traits, `SCREAMING_SNAKE_CASE` constants.
- TS/Svelte: `camelCase` functions/variables, `PascalCase` components/classes/types.
- Runes-based Svelte service modules often use `.service.svelte.ts`.
- UI activity registration files commonly use `.activity.ts`.
- Keep TS tests near the code they cover; Rust tests may be inline or under `tests/`.

## Types And Architecture

- Canonical domain types live in Rust, especially under `libs/entities/rs/` and sibling entity crates.
- Generate TS bindings from Rust exports instead of duplicating shapes manually.
- Prefer explicit types on public APIs and non-obvious return values.
- `apps/ui/tsconfig.json` is strict; the root TS config is looser for legacy packages.
- In UI code, prefer aliases like `$lib`, `$services`, `$components`, and `$design` over deep relative imports.

## Error Handling

### Rust

- `anyhow` is the common application-level error type.
- Prefer `anyhow::Result<T>` or a local crate result alias if one exists.
- Add `.context(...)` / `.with_context(...)` for IO, parsing, subprocess, and network failures.
- Use `bail!` / `anyhow!` for clear early exits.

### TypeScript / UI

- Prefer `unknown` for caught errors and normalize before surfacing them.
- Fail gracefully in UI/service code; many services use helper wrappers and toast feedback.
- Avoid introducing `any` unless there is no practical typed option.
- Gate noisy debug logging behind a prefix and/or development checks.

## Testing Conventions

- Vitest is the main JS/TS unit test runner.
- Playwright is used for UI e2e coverage.
- Rust uses `#[test]`, `#[tokio::test]`, inline test modules, and integration tests.
- Favor narrow, behavior-focused tests over broad end-to-end setups.

## Commit Expectations

- Use conventional commits: `feat(scope): ...`, `fix(scope): ...`, `chore(scope): ...`.
- Keep commits task-scoped; do not bundle unrelated files.
- Do not push or create remote PR side effects unless explicitly asked.

## Useful References

- `docs/TESTING.md`
- `docs/CONTRIBUTING.md`
- `libs/myko/rs/MIGRATION.md`
- `libs/myko/rs/OPTIMIZATION.md`
