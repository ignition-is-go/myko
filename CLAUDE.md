# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Rocketship (rship) is a centralized control platform for orchestrating reactive event relationships within networks of integrated multimedia systems. It's a polyglot monorepo combining TypeScript, Rust, Python, C#, and Swift.

**Core Concept**: External software **Services** run **Executors** that connect to the rship server via WebSocket. Executors publish **Targets** (interactable entities) with **Emitters** (state observers) and **Actions** (commands). **Bindings** define reactive relationships between Emitters and Actions, organized into **Scenes** and **Calendars**.

## Development Commands

### JavaScript/TypeScript

```bash
pnpm install                          # Install dependencies
pnpm dev --filter @rship/server       # Run server in watch mode (MYKO_PORT=5155)
pnpm dev --filter @rship/ui           # Run UI dev server
pnpm typecheck-sdk                    # Type check SDK
pnpm typecheck-postgres               # Type check postgres lib
pnpm build --filter <package>         # Build specific package
pnpm format:all                       # Format all code with prettier
```

### Rust

```bash
cargo build --release                 # Build release
cargo test                            # Run all tests
cargo test -- --nocapture             # Run tests with output
cargo test <test_name>                # Run single test
cargo clippy -- -D warnings           # Lint with clippy
cargo fmt                             # Format Rust code
```

### Python

```bash
uv pip install -e .                   # Install package in editable mode
pytest                                # Run all tests
pytest -k <test_name>                 # Run single test
```

### Multi-language Publishing

```bash
pnpm jsr:publish                      # Publish TypeScript to JSR
pnpm py:publish                       # Publish Python packages
pnpm rust:publish                     # Publish Rust crates
pnpm cs:publish                       # Publish C# packages
```

### Versioning

```bash
pnpm versionstamp                     # Generate version metadata
pnpm versionwrite                     # Update versions across packages
pnpm gen                              # Run scaffolding and generate entity index
```

## Architecture

### Core Framework: Myko (`/libs/myko/`)

Event-sourcing CQRS framework powering rship's reactive architecture:

- **@myko/core**: Base event sourcing primitives
  - `MItem`: Base entity with MD5 content hashing
  - `MEvent`: Immutable SET/DEL events with timestamps
  - `MCommand`: Command specifications (intent)
  - `MQuery`: State snapshots
  - `MSaga`: Observable-based event processors
- **@myko/ws**: Real-time bidirectional WebSocket with MessagePack encoding
- **@myko/gateway**: Server bootstrap, Auth0 integration, OpenTelemetry tracing
- **@myko/kafka**: Kafka-based event persistence
- **@myko/sqlite / @myko/postgres / @myko/surreal**: Storage backends
- **@myko/rs**: Rust client library with FFI-friendly callback APIs for language bindings
  - `MykoClient`: WebSocket client with connection management
  - `watch_query_callback`: Query state management (upserts/deletes)
  - `watch_connection_status_callback`: Connection status streaming
  - Types exported via `ts-rs` for TypeScript consumption
- **@myko/ts**: NAPI-based TypeScript client wrapping @myko/rs
  - Thin wrapper (~95 lines Rust, ~75 lines TypeScript)
  - RxJS Observable APIs over native callbacks
  - Build: `pnpm build --filter @myko/ts`
  - Type generation: `pnpm --filter @myko/rs gen` (runs `cargo test --lib`)

**Pattern**: Commands → Events → State Updates → Queries

### Entity System (`/libs/entities/`)

Rship-specific domain entities built on Myko:

- **Core entities**: Target, Instance, Machine, Emitter, Action, Binding, Scene, Calendar, Pulse, EventTrack
- **BindingNode trees**: Complex execution graphs with Expression → Condition → Constraint → Delay → Action
- **Handlers** (`/handlers/`): Business logic per entity type (e.g., `binding-handler.ts`, `scene-handler.ts`)
- Uses `reflect-metadata` for runtime type registration

Entity handlers are loaded by the server at startup and process Commands to generate Events.

### SDK (`/libs/sdk/`)

Multi-language executor development kit:

- **TypeScript** (primary): `RshipExecClient` with fluent API
  - `InstanceProxy → TargetProxy → EmitterProxy | ActionProxy`
- **Rust, Python, C#, Swift**: Multi-language support for executor development

Executors use the SDK to:

1. Connect to rship server via WebSocket
2. Declare Instances, Targets, Emitters, Actions
3. Push Pulses (real-time data from Emitters)
4. Receive and execute Actions

### Link/RPC Layer (`/libs/link/`)

gRPC-based RPC for controller management:

- **Protocol Buffers** define Link service (`link.proto`)
  - Methods: Disconnect, SetRshipUrl, GetControllers, ConnectController, etc.
- **TypeScript bindings**: Auto-generated via `protoc-gen-ts_proto`
- **Rust implementation**: gRPC server in `/link/core/`

Used for managing external controller connections (hardware control surfaces, etc.).

### Asset Store (`/libs/asset-store/`)

Actor-based S3-compatible file storage system:

- **Core (Rust)**: Actor-based using `ractor`
  - Storage Manager, Upload Manager, Presence Manager, WebSocket Manager actors
- **Client (TypeScript)**: Type-safe NAPI-RS bindings
- Supports MinIO, AWS S3 with multipart uploads and real-time WebSocket updates

### Communication & Data Flow

```
Executor (Push Pulses via WebSocket)
    ↓
Server (Process via Entity Handlers, Execute Bindings)
    ├─ Commands from UI → Actions to Executors
    └─ Events → Real-time Updates to UI
    ↓
Persistence (Kafka Event Log)
```

**WebSocket Message Types**:

1. **Commands**: `MWrappedCommand` with transaction ID
2. **Events**: SET (create/update) or DEL (delete) with timestamp
3. **Pulses**: Real-time emitter data (not persisted)
4. **Queries**: State snapshots

**Binding Execution**: BindingNode trees process Pulses through expression evaluation, conditions, constraints, delays, and finally invoke Actions.

### Applications (`/apps/`)

- **server**: Main Bun-based server
  - Entry: `/apps/server/src/main.ts`
  - Bootstraps Myko gateway, loads entity handlers, sets up persistence
  - Required environment variables:
    - `KAFKA_BROKERS` - Comma-separated Kafka broker addresses
    - `MYKO_HOST_ADDRESS` - Server host address
    - `RSHIP_CLUSTER_SECRET` - Cluster authentication secret
    - `AUTH_0_DOMAIN` - Auth0 domain for authentication
    - `MYKO_PORT` - Server port (typically 5155 for dev)
  - Optional: `MYKO_TRACING_ENDPOINT` - OpenTelemetry tracing endpoint
- **ui**: Svelte 5 + SvelteKit web UI
  - Real-time editor, 3D visualization (Threlte + Three.js)
  - Schema-based forms, Auth0 authentication
  - Cross-platform: Web, iOS/Android via Capacitor
- **execs**: Executor implementations (each integrates an external system)
  - TypeScript: ableton-cli, protocol-router, viewpoint, ventuz, noise
  - Python: demo-py, music-analysis
  - C#: disguise, pixera, dirigera
  - visionOS (Swift), touch-host (TouchDesigner)

### Multi-Language Type Sharing

- **Rust → TypeScript**: `ts-rs` derive macros generate TypeScript types from Rust structs
  - Types generated via `cargo test --lib` (ts-rs uses test harness for codegen)
  - Output to `bindings/` directory, re-exported via `bindings/index.ts`
  - Run `pnpm --filter @myko/rs gen` to regenerate types
- **Protocol Buffers**: Language-agnostic schemas for RPC (Link layer)
- **NAPI-RS**: Rust native modules with auto-generated TypeScript bindings (Asset Store, Sync, @myko/ts)

## Code Style

### Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat(scope): description` - New features
- `fix(scope): description` - Bug fixes
- `chore(scope): description` - Maintenance tasks

Commits drive release notes and CI workflows.

### Comments

TODO & NOTE comments should include author's initials:

```typescript
// TODO(ts): need to implement
// NOTE(ts): informational message
```

### Formatting

- **JS/TS**: Use `prettier` with `prettier-plugin-organize-imports`
- **Rust**: Use `rustfmt`
- Lines under 120 characters
- Comments explain _why_, not _what_

### Naming Conventions

- **JS/TS**: `camelCase` for variables/functions, `PascalCase` for classes/types
- **Rust**: `snake_case` for variables/functions, `PascalCase` for structs/enums
- **Python**: Follow PEP8 (`snake_case` for variables/functions, `PascalCase` for classes)

## Project Structure

```
/apps/
├── server/           # Main Bun server (entry: src/main.ts)
├── ui/               # Svelte 5 UI with SvelteKit
├── execs/            # 15+ executor implementations
├── asset_store/      # Asset storage service
├── linkd/            # Link daemon
└── myko/             # Myko server standalone

/libs/
├── myko/             # Event sourcing framework (13 modules)
├── entities/         # Rship entity definitions & handlers
├── sdk/              # Executor SDK (6 languages)
├── link/             # gRPC RPC layer
├── asset-store/      # File storage system (Rust core + TS client)
├── types/            # Shared TypeScript types
├── sync/             # Sync/FFI layer
└── [20+ integration libraries]
```

## Key Implementation Patterns

### Event Sourcing + CQRS

All state changes flow through immutable events:

1. UI/Executor sends Command
2. Entity handler validates and generates Events
3. Events persisted to Kafka
4. Sagas react to events and may generate new Commands
5. Queries provide current state snapshots

### Reactive Streams (RxJS)

Heavy use of Observables for real-time data flow. Entity handlers and UI components subscribe to event streams.

### Hash-Based Versioning

`MItem` uses MD5 content hashing for optimistic concurrency control and conflict detection.

### Actor Model

Asset Store uses `ractor` for concurrent processing with supervision trees. This pattern may expand to other subsystems (see CRUSH.md for migration plans).

### Stateless Executors

Executors are bridges to external software. State remains in the external system; Executors translate between native APIs and rship's abstract model.

## Important Notes

- **Server Runtime**: Uses Bun, not Node.js
- **Package Manager**: pnpm with workspaces
- **Monorepo**: All packages in `/apps/` and `/libs/` defined in `pnpm-workspace.yaml`
- **Type Safety**: Extensive use of TypeScript with strict null checks
- **Real-time Performance**: See CRUSH.md for Rust optimization guidelines (actor patterns, lock-free structures, channel sizing, serialization)
- **Submodules**: TouchDesigner and Unreal integrations are git submodules (auto-updated via preinstall hook)

## Environment Setup

1. Install dependencies: `pnpm install` (runs git submodule update automatically)
2. Version stamping runs automatically in postinstall
3. Development requires Bun runtime for server
4. Rust toolchain for native modules
5. Python with `uv` for Python packages

## Debugging & Development Workflow

### Running Specific Packages

```bash
pnpm dev --filter @rship/server        # Run server with hot reload
pnpm dev --filter @rship/ui            # Run UI dev server
pnpm build --filter @rship/sdk         # Build specific package
```

### Working with Executors

1. Start rship server first (`pnpm dev --filter @rship/server`)
2. Run executor (varies by language - see executor's README)
3. Executor connects via WebSocket to publish Targets/Emitters/Actions
4. Use UI to create Bindings between Emitters and Actions

### Common Issues

- **Kafka connection errors**: Ensure `KAFKA_BROKERS` is set and Kafka is running
- **WebSocket connection fails**: Check `MYKO_PORT` and `MYKO_HOST_ADDRESS` match between server and clients
- **Type generation out of sync**: Run `pnpm versionstamp` to regenerate types
- **Submodule not initialized**: Run `git submodule update --init --recursive --remote`

## Code Integration Guidelines

These guidelines capture lessons learned from previous code contributions to help Claude Code produce changes that align with project standards.

### 1. Respect Explicit Opt-In Patterns

**Pattern**: This project uses environment variable guards for diagnostic/monitoring features even when they only produce debug logs.

**Example**:

```typescript
// Preferred: Explicit opt-in with env guard
if (process.env['MEMORY_MONITOR'] !== 'true') {
  return
}
setInterval(() => {
  logger.debug('Memory stats...')
}, 5000)

// Don't: Remove guards and rely solely on log level
// Even though debug logs won't show at INFO level, the interval still runs
// and collects data unnecessarily
```

**Rationale**: Environment guards prevent performance overhead (intervals, data collection) even when logs won't be displayed. This is intentional design, not cruft to remove.

### 2. Commit Organization

**Pattern**: Separate critical bug fixes from nice-to-have improvements.

**Example**:

```
Commit 1: fix: myko gateway memory leaks
  - Core subscription cleanup (takeUntil + finalize)
  - Client disconnect handling
  - Repo caching for watchId/clientDisconnect
  - Log cleanup sagas

Commit 2: fix(server): better debug logging control
  - Migrate console.log to MykoLogger
  - Add MYKO_INITIAL_LOG_LEVEL support
  - Consolidate diagnostic output format
```

**Rationale**: Makes it easier to cherry-pick critical fixes, revert non-essential changes, and understand git history. Bug fixes should be complete and include all related changes in one commit.

### 3. Code Formatting

**Pattern**: Let automated formatters (prettier, rustfmt) handle formatting. Don't try to match formatting manually in edits.

**Why**: The user will run formatters anyway. Focus on logical correctness, not whitespace alignment. Mismatched formatting creates noisy diffs and merge conflicts.

### 4. Comprehensive Issue Resolution

**Pattern**: When fixing systemic issues (memory leaks, race conditions, etc.), address all instances across the codebase in a single commit.

**Example**: For the memory leak fix, included:

- All scene engine methods (6 methods fixed)
- Repo-level caching (watchId)
- Bootstrap-level caching (clientDisconnect)
- Related cleanup sagas (LinkLog, ExecLog)
- Diagnostic tools for future debugging

**Rationale**: Partial fixes leave technical debt and make it harder to verify the issue is fully resolved. Group related changes together so the entire fix can be reviewed, tested, and potentially reverted as a unit.

### 5. Import Cleanup

**Pattern**: Remove unused imports as part of the change that makes them unused, not as a separate "cleanup" commit.

**Example**:

```typescript
// When replacing takeWhile with takeUntil:
import {
  takeUntil,  // Added
- takeWhile,  // Removed in same commit
  tap,
} from 'rxjs'
```

**Rationale**: Keeps commits atomic and prevents dead code from accumulating between commits.

### 6. Prefer Existing Patterns

**Pattern**: Before suggesting architectural changes, check if the project already has established patterns for similar functionality.

**Example**: The project already had:

- MykoLogger for structured logging
- Environment variable guards for diagnostic features
- Debug log level for non-production diagnostics

Don't suggest inventing new patterns when existing ones work fine.

### 7. Performance-Conscious Defaults

**Pattern**: This project prioritizes runtime performance over convenience. Diagnostic features should be:

- Opt-in via environment variables
- Use debug/verbose log levels (not info)
- Minimal overhead when disabled

**Why**: Rship handles real-time multimedia control with high message throughput. Even "cheap" operations like collecting memory stats every 5 seconds add up at scale.

### 8. URL Path Design for Reverse Proxies

**Pattern**: Use query parameters instead of path segments for dynamic identifiers that may contain special characters (especially `/` or `%2F`).

**Example**:

```rust
// Preferred: Query parameter approach
GET /asset?key=folder%2Fsubfolder%2Ffile.png
GET /thumbnail?key=textures%2Fwood.jpg

// Avoid: Path segment approach
GET /assets/folder%2Fsubfolder%2Ffile.png/download
GET /thumbnails/textures%2Fwood.jpg
```

**Why**: Reverse proxies like Traefik often decode `%2F` to `/` in path segments before forwarding requests, breaking routes that expect encoded slashes. Query parameters are not decoded by proxies and reach the backend intact. This affects any identifier that could contain forward slashes (file paths, S3 object keys, etc.).
