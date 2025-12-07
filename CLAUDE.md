# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Rocketship (rship) is a centralized control platform for orchestrating reactive event relationships within networks of integrated multimedia systems. It's a polyglot monorepo combining TypeScript, Rust, Python, C#, and Swift.

**Core Concept**: External software **Services** run **Executors** that connect to the rship server via WebSocket. Executors publish **Targets** (interactable entities) with **Emitters** (state observers) and **Actions** (commands). **Bindings** define reactive relationships between Emitters and Actions, organized into **Scenes** and **Calendars**.

## Platform Philosophy

### Core Tenets

Rship represents a paradigm shift from imperative ("do this, then that") to declarative ("when this, then that") control programming. The platform is built on three pillars:

1. **Events**: All control data flows through real-time, schema-defined event streams
2. **Reactivity**: Relationships are programmed declaratively, enabling complex behavior and real-time adaptability
3. **Time**: Temporal relationships enable precise event synchronization from micro to macro time scales

### Technology Should...

- Expand possibility, not inhibit process
- Serve creative purpose, not justify its own existence
- Adapt to creative intent, not dictate limitations
- Encourage idea development, not just facilitate implementation

### User Personas

When building features, consider these distinct user needs:

**Artists/Performers**: "I want technology to augment creativity, not be an obstacle. I need a responsive environment that reacts to my performance without worrying about breaking control flow. If I look at the interface, I want visualizations of how the system works, not technical details."

**Creative Directors/Producers**: "I want to know where the show lives, what it looks/feels like, and how parts relate. I need high-level creative control without technical complexity—visualizations, dashboards, and design-driven tools."

**Technical Directors/Engineers**: "I need to know everything is working and have deep control when it's not. I want monitoring dashboards, system topology, asset management, and troubleshooting tools. I balance creative needs with technical realities."

### Platform Hierarchy

Understand these organizational levels:

- **Organizations**: Top-level entities, often tied to hardware/compute ownership
- **Contexts**: Broad production processes (previz, rehearsal, touring) tied to a server cluster
- **Projects**: Encapsulate scenes, bindings, event tracks, variables, sessions within a context
- **Sessions**: Instance activation and assignment within a project
- **Scenes**: Container for bindings and node graphs

**Critical constraint**: An instance can only be assigned to one project at a time to prevent conflicting state management.

## UX Design Principles

### Progressive Information Density

Information reveals itself based on context and user needs. Design UI with these layers:

1. **At-A-Glance**: Visualizations, live status, active states, "what's happening now"
2. **Creating/Editing**: Working with entities, parameters, temporal/logical behavior, "how things work"
3. **Debugging**: Network traffic, logs, event traces, performance profiling, "why things happened"
4. **Development**: Tools for executor developers to help themselves

### UX Modes

Design UI to support these distinct operational modes:

**Compose Mode** (Planning/Designing):
- Feels like sketching, wireframing, drafting
- Low stakes, freely exploring ideas
- Infinite canvas, visual placement, frames, placeholders
- Maximum previsualization and simulation

**Refine Mode** (Editing/Finetuning):
- Major structure exists, now polish
- Data visualization, smoothing, tweening, ranging
- Instant feedback loop for edits
- High information density acceptable (tables, lists)
- Entity cross-relations visible to prevent unwanted consequences

**Operate Mode** (Live/Show):
- Show is live, mostly observing and monitoring
- UI elements are bigger, less dense for fast readability
- Manual cues and realtime parameter editing
- Emergency countermeasures accessible

### View Organization

The UI organizes around what users are trying to accomplish:

- **Workspaces**: Saved pane layouts shared among teams
- **Panes**: Flexible containers with tabs (like IDE or terminal multiplexer)
- **Binary tree splitting**: Ctrl+P splits pane opposite to aspect ratio
- **Focus follows cursor**
- **Projects tied to windows, sessions can differ between panes**

### Command Palette

Users access most UI commands through the command palette. It also detects natural language for AI-assisted operations:

- "Give me a view to the right with a scene list and target list"
- "Bind all of these physical light intensity emitters to all of these virtual light intensity actions"
- "Visualize this event relationship"

### Reactivity/Determinism Awareness

Users must understand event relationships and system behavior:

- What bindings are active in which scenes?
- What events could lead to scene activation?
- **Guardrails**: Warn users entering "undefined behavior territory"
- **Scene arming/disarming**: Control when reactive scenes can activate
- **Locking**: Protect entities from accidental changes
- **Flagging**: Mark entities for warnings if affected by edits

### Don't Make Users Think

Surface relevant information automatically:

- What do action/emitter fields mean? (semantic info, units, descriptions)
- What does this entity belong/relate to?
- Use consistent terminology across all contexts
- Visual feedback for all state changes (UI-only, server response, other users)

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

### Design for Massive Scale

**CRITICAL**: Rship is global infrastructure for controlling distributed multimedia systems. Every feature must be built with massive scale in mind from day one:

**Scale targets to always consider:**
- **Thousands of nodes** in a single scene graph
- **Thousands of machines** connected simultaneously
- **Thousands of lights/fixtures** being controlled in real-time
- **Hundreds of executors** pushing pulses concurrently
- **Dozens of UI clients** viewing/editing the same project
- **Millions of events** flowing through the system per hour

**What this means for implementation:**

- **UI rendering**: Use virtualization for lists and grids. Never render thousands of DOM elements. Implement windowing, pagination, or canvas-based rendering for large datasets.
- **Data structures**: Choose O(1) or O(log n) operations over O(n). Use Maps/Sets instead of array searches. Consider spatial indexing (R-trees, quadtrees) for spatial queries.
- **Network efficiency**: Batch updates where possible. Use delta/diff protocols instead of full state sync. Implement pagination for queries. Consider WebSocket message coalescing.
- **Memory management**: Stream large datasets instead of loading entirely into memory. Implement proper cleanup and disposal. Watch for memory leaks in subscriptions and event listeners.
- **Database queries**: Always consider query performance at scale. Add appropriate indexes. Avoid N+1 query patterns. Use projections to fetch only needed fields.
- **Real-time updates**: Design subscriptions to handle high-frequency updates without overwhelming the UI. Implement throttling/debouncing. Consider update batching.
- **Scene graphs**: Node operations must scale. Avoid full graph traversals. Use incremental updates. Consider graph partitioning for very large scenes.

**Questions to ask before implementing:**
1. What happens when there are 10,000 items instead of 10?
2. What happens when 100 clients are connected instead of 1?
3. What happens when updates arrive at 1000/sec instead of 1/sec?
4. Will this cause memory to grow unbounded over time?
5. Does this require loading all data before showing anything?

**Anti-patterns to avoid:**
- Loading all entities into memory at startup
- Rendering all items in a list regardless of viewport
- Full state synchronization on every change
- Unthrottled real-time updates to UI
- Recursive operations without depth limits
- Storing unbounded history/logs in memory

### Use Current APIs and Documentation

**CRITICAL**: Always verify you're using current framework APIs. Training data may be outdated. Before implementing:

1. **Check official documentation** for the current version of any framework or library
2. **Search the existing codebase** to see how similar functionality is already implemented
3. **Verify version compatibility** - check `package.json` or `Cargo.toml` for actual versions in use

**Framework-specific guidance for this project:**

- **Svelte 5** (not Svelte 4): Uses runes (`$state`, `$derived`, `$effect`), not reactive statements (`$:`). Components use `<script>` not `<script lang="ts">` with stores. Check existing components in `apps/ui/src/lib/` for patterns.
- **SvelteKit 2**: File-based routing in `src/routes/`. Uses `+page.svelte`, `+layout.svelte`, `+server.ts` conventions. Load functions in `+page.ts` or `+layout.ts`.
- **Tauri 2** (not Tauri 1): Different plugin system, command structure, and IPC. Uses `@tauri-apps/api` v2 imports. Check `apps/ui-desktop/src-tauri/` for current patterns.
- **Threlte 8**: Three.js integration for Svelte 5. Different component API from earlier versions.
- **RxJS 7**: Used extensively for reactive streams. Check `@myko/` packages for observable patterns.

**When uncertain about an API:**
- Search the codebase for existing usage: `grep -r "functionName" --include="*.ts"`
- Check the package version: `cat package.json | grep "package-name"`
- Consult current docs (use web search if needed)
- Ask the user if multiple approaches seem valid

**Common outdated patterns to avoid:**
- Svelte 4 reactive statements (`$:`) → Use Svelte 5 runes (`$derived`, `$effect`)
- Svelte stores (`writable`, `readable`) → Use Svelte 5 `$state` in `.svelte.ts` files
- `on:click` → Use `onclick` (Svelte 5 event handlers)
- `export let prop` → Use `let { prop } = $props()` (Svelte 5)
- Tauri 1.x `invoke` patterns → Use Tauri 2 command bindings
- `createEventDispatcher` → Use callback props or `$host()` in Svelte 5

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
- **Form element styling**: When using select/dropdown elements, always ensure adequate padding-right for the caret/arrow indicator. Never let the caret overlap with the text content. Use `min-width` to ensure options are readable.

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
