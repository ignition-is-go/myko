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
  - Environment variables: `KAFKA_BROKERS`, `MYKO_HOST_ADDRESS`, `RSHIP_CLUSTER_SECRET`, `AUTH_0_DOMAIN`, `MYKO_PORT`
  
- **ui**: Svelte 5 + SvelteKit web UI
  - Real-time editor, 3D visualization (Threlte + Three.js)
  - Schema-based forms, Auth0 authentication
  - Cross-platform: Web, iOS/Android via Capacitor
  
- **execs**: Executor implementations
  - Ableton, Pixera, Disguise, Dirigera, Ventuz, Viewpoint, Protocol Router, etc.
  - Each integrates a specific external system with rship

### Multi-Language Type Sharing

- **Rust → TypeScript**: `ts-rs` derive macros generate TypeScript types from Rust structs
- **Protocol Buffers**: Language-agnostic schemas for RPC (Link layer)
- **NAPI-RS**: Rust native modules with auto-generated TypeScript bindings (Asset Store, Sync)

## Development Principles

### Production Software Mindset

This is production software used in live entertainment, broadcast, and installation contexts. Every change must consider:

- **Multi-developer maintenance**: Code will be read and modified by many engineers. Prioritize clarity over cleverness. Use descriptive names, add comments for non-obvious logic, and follow established patterns in the codebase.
- **Modular architecture**: Features should be self-contained with clear boundaries. Avoid tight coupling between modules. New functionality should extend existing abstractions rather than create parallel systems.
- **Backward compatibility**: Changes to entities, handlers, or SDK APIs may affect existing projects and executors. Consider migration paths and deprecation strategies.
- **Error resilience**: Production deployments cannot crash. Handle edge cases, validate inputs at system boundaries, and provide meaningful error messages.

### Distributed Architecture Awareness

Rship is inherently distributed. Always consider:

- **Sessions**: Users work within sessions that scope their project context. UI state, entity queries, and commands operate within session boundaries. Don't assume single-user or single-session.
- **Window Groups**: The UI supports multiple synchronized windows (e.g., control room + stage view). State must stay consistent across window group members. Consider what happens when the same data is viewed/edited from multiple windows.
- **Multi-client reality**: Multiple executors, multiple UI clients, and multiple users may connect simultaneously. Entity state can change at any time from any client. Design for eventual consistency and handle concurrent modifications gracefully.
- **Connection lifecycle**: Clients connect, disconnect, and reconnect. Executors may go offline. UI must handle these states and recover gracefully. Never assume persistent connections.

### Thoroughness Requirements

When investigating or fixing issues:

- **Find all instances**: A bug in one place often exists in similar code elsewhere. Search the codebase for related patterns. If fixing a binding handler issue, check all binding handlers. If fixing a UI component pattern, check all components using that pattern.
- **Trace the full path**: Follow data from origin to destination. For UI issues, trace from user action → component → service → command → handler → event → query → UI update. For executor issues, trace pulse → binding → action → executor.
- **Check all entity types**: If a change affects how entities work, verify it against all relevant entity types (Target, Binding, Scene, Calendar, etc.).
- **Verify across platforms**: UI runs on web, iOS, Android (Capacitor), and desktop (Tauri). Executors run on various OS. Consider platform-specific behavior.

### Complete Feature Implementation

Features must be fully realized, not rushed to completion:

- **Plan the full user flow**: Before implementing, map out the complete user journey. What initiates the action? What feedback does the user see? How do they know it succeeded or failed? What happens on error? How do they undo or modify?
- **Design before coding**: For significant features, document the approach. What entities are involved? What commands/events? What UI components? What edge cases? Get alignment before writing code.
- **Build all implications**: A feature isn't done when the "happy path" works. Consider:
  - Empty states (no data yet)
  - Loading states (data fetching)
  - Error states (operation failed)
  - Edge cases (unusual inputs, race conditions)
  - Permissions (who can do this?)
  - Persistence (does state survive refresh/reconnect?)
  - Undo/redo (can the user reverse this?)
- **Test the integration**: Features must work within the larger system. Test with real executors, multiple clients, and realistic data volumes.
- **Don't cut corners to finish**: If a feature is taking longer than expected, communicate and adjust scope rather than shipping incomplete work. Half-implemented features create technical debt and user confusion.

### UI Development Guidelines

When building or modifying UI:

- **Consider the full interaction model**:
  - Keyboard navigation and shortcuts
  - Touch/mobile interactions (if applicable)
  - Accessibility (screen readers, color contrast)
  - Responsive behavior across screen sizes
- **Handle real-time updates**: Data changes from other clients. Subscriptions must handle updates, deletions, and additions. Don't assume data is static.
- **Manage loading and error states**: Every async operation needs loading indication and error handling. Users should never see a frozen UI or wonder if their action worked.
- **Follow existing component patterns**: Check how similar features are implemented. Use existing design system components. Maintain visual and behavioral consistency.
- **Consider window group context**: Some UI may appear in multiple windows. State should be consistent. Consider which window should handle which interactions.

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
- Comments explain *why*, not *what*

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
- **Real-time Performance**: See CRUSH.md for optimization guidelines (lock-free structures, channel sizing, serialization)
- **Submodules**: Unreal integration is a git submodule (`libs/unreal/rship-unreal`)

## Environment Setup

1. Install dependencies: `pnpm install` (runs git submodule update automatically)
2. Version stamping runs automatically in postinstall
3. Development requires Bun runtime for server
4. Rust toolchain for native modules
5. Python with `uv` for Python packages
