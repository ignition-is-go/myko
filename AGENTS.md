# Repository Guidelines

## Project Structure & Module Organization

- `apps/` hosts deployable surfaces such as `server`, `linkd`, `link-ui`, and `sync_server`. Source sits in `src/`, with platform extras under `src-tauri` or language-specific folders.
- `libs/` collects shared packages across languages: TypeScript SDK and UI utilities (`libs/sdk/ts`, `libs/svelte-schema-form`), Rust crates (`libs/link/core`, `libs/sync/core`, `libs/rs-macros`), and Python packages registered through `pyproject.toml`.
- `tools/` centralizes automation (`tools/scripts/*.sh`, `*.ts`) and scaffolding (`tools/scaffold`). Reuse these instead of handcrafting release steps.
- Assets and documentation live in `assets/` and `apps/docs.rship.io`. Tests co-locate with code (`*.test.ts`, `tests/`, or `__tests__`) to keep modules self-describing.

## Vixi Virtual Cam UI

- Never ever touch anything related to the Vixi Virtual Cam UI. Leave its files, configuration, and build steps completely untouched.

## Build, Test, and Development Commands

- `pnpm install` installs JS/TS workspaces; run after syncing submodules via `pnpm run sync`.
- `pnpm run gen` refreshes generated indexes and entity code; re-run after adding packages or schema types.
- `pnpm run typecheck-sdk` and `pnpm run typecheck-postgres` run strict TypeScript checks for major SDK surfaces.
- `pnpm -r test` executes all package `test` scripts (Vitest suites).
- `cargo test --workspace` validates all Rust crates; use `cargo clippy -- -D warnings` before shipping.
- `uv build --all-packages` (or `pnpm run py`) ensures Python packages build cleanly prior to publishing.

## Coding Style & Naming Conventions

TypeScript and Svelte code format with Prettier (2-space indent) and organize imports automatically; use `pnpm run format:all` before committing. Rust code must pass `cargo fmt` and Clippy; Python follows PEP 8 with `uv` tooling. Apply `camelCase` for JS/TS values, `PascalCase` for components/types, and `snake_case` in Rust and Python functions. TODO/NOTE comments include initials (`// TODO(ts): ...`) and keep lines under 120 characters per `CRUSH.md`.

## Testing Guidelines

Write Vitest unit tests alongside TypeScript sources (`*.test.ts`) and cover any new contracts or edge cases. Rust modules require either inline `#[cfg(test)]` blocks or files under `tests/`; prefer integration tests for crate boundaries. When touching Python packages, add tests under each package’s `tests/` directory and run them with `uv run pytest` or the package-specific script. Document any non-automated verification in the PR description.

## Commit & Pull Request Guidelines

Follow Conventional Commits observed in history (e.g., `feat(linkd): add executor metrics`). Keep commits scoped and runnable; rerun type checks and tests before pushing. Pull requests need a clear summary, linked issue or ticket, and notes on deployment impact. Attach screenshots or recordings when UI behavior changes, and call out follow-up tasks or migrations so reviewers can plan.
