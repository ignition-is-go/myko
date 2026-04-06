# Myko

An event-sourcing CQRS framework. Define entities with the `#[myko_item]` macro; get commands, queries, and reactive state management automatically.

## Core Concept

**Commands** modify state. **Events** are persisted. **Queries** return current state. **Sagas** react to events and trigger further commands. The framework handles persistence, real-time sync, and cross-language type generation.

## Quick Start

```bash
# Prerequisites: Bun, Rust
bun install
cargo check
cargo test
```

## Structure

```
libs/myko/rs/       # Canonical Rust implementation
libs/myko/macros/   # #[myko_item] proc macro
libs/myko/server/   # Server runtime (WebSocket, Kafka, federation)
libs/myko/leptos/   # Leptos web-ui integration
libs/myko/ts/       # TypeScript port
libs/myko/py/       # Python bindings
libs/myko/cpp/      # C++ bindings
libs/myko/csharp/   # C# bindings
libs/autosocket/    # Auto-reconnecting WebSocket transport
```

## Dependencies

- **Hyphae** (reactive dataflow) — external, expected at `../hyphae/hyphae`

## Usage

```rust
use myko_macros::myko_item;

#[myko_item]
pub struct Target {
    pub name: String,
}
// Auto-generates: GetAllTargets, GetTargetById, DeleteTarget, PartialTarget, etc.
```

## License

MIT OR Apache-2.0
