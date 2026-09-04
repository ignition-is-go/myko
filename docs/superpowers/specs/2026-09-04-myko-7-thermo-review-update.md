# Myko 7 architecture review update

**Date:** 2026-09-04

**Target:** `fb36256c5bfbcfdc558967e6092c166308cb7b49` plus the current dirty worktree

**Prior review:** `/tmp/myko-7-thermo/`

## Verdict

Approve the collapsed architecture.

The earlier review found the right root problem: Myko 7 duplicated mature Myko 6 application, reactive, session, client, and UI machinery. The implementation now uses the retained Myko 6 owners as the only framework runtime and composes the new durable federation, authority, and transport semantics behind them. In particular:

- keep Myko 6 as the application framework and runtime base;
- move Myko 7's new domain semantics into the existing Myko 6 owners;
- delete the parallel Myko 7 handler, reactive runtime, client, and session implementations as callers migrate;
- preserve one current cross-transport semantic contract, but do not preserve either existing wire format for compatibility.

Those conditions are now satisfied. The 2026-09-03 convergence design remains superseded by the retained-ownership design.

## Current findings

| Finding | Status | Final evidence |
| --- | --- | --- |
| B1. Greenfield monoliths | Fixed | Duplicate crates were deleted. The remaining crate roots are 98 lines for federation, 98 for Iroh, 71 for authority, and 62 for local; their implementations are split into ownership-focused modules. |
| B2. Two application frameworks | Fixed | Retained `myko` owns the only public handler family, inventories, activation, session, client, and watches. `myko-app` and `myko-app-macros` were deleted. |
| B3. Snapshot query path | Fixed | Federated projections feed the retained lazy keyed `MapQuery` and materialization path. The duplicate snapshot and collection bridge implementations are absent. |
| B4. Transport-specific application clients | Fixed | Local, Iroh, embedded, and WebSocket paths adapt to retained `MykoClient`; `myko-runtime` and transport-specific application-client owners were deleted. |
| B5. Unbounded delivery | Fixed | Durable and control paths use bounded lossless queues and paged history; compatible live map revisions coalesce or force resynchronization. Disconnected client sends are bounded. |
| B6. Three collection truths | Fixed | Retained keyed watches publish rows with one coherent revision carrying diff, frontier, epoch, and liveness. The duplicate `LiveCollection` runtime is gone. |
| B7. Optional-bag authorization | Fixed | One typed `PreparedRequest` derives one `AccessTarget`; optional target fields and `normalized_claims` are absent. |
| B8. Monolithic authority evaluator and self-bypass | Fixed | The 71-line root delegates to domain, command, fact, evaluator, policy, and test modules. `evaluate` is a thin 11-line coordinator with cyclomatic complexity 3 and cognitive complexity 5; typed stages validate leases and capabilities and resolve grants, delegations, and obligations. Framework authority writes use an explicit private trusted path. |
| B9. Repeated request interpretation | Fixed | Serialized requests cross one exhaustive preparation boundary. Routing and authority consume the resulting prepared operation and target without rematching wire variants. |
| B10. Runtime compatibility branches | Fixed | Legacy scope decoding, raw peer restoration, Iroh aliases, unversioned cursor behavior, and compatibility gateway code were deleted. Wire compatibility is intentionally not maintained. |
| B11. Hidden global Tokio runtime | Fixed | The duplicate `HandlerRuntime` was deleted. Retained node, server, session, and client lifecycle owners drive work and joined shutdown. |
| B12. Typed item identity erased to strings | Fixed | `ItemProjection<T>` stores `BTreeMap<T::Id, ItemState<T>>`; codecs erase identity only at durable or transport boundaries. |
| B13. The migration verifier proves the wrong destination | Fixed | `scripts/check-myko-collapse.sh final` rejects duplicate owners and compatibility branches and passes against the final structure. |
| B14. One source shuts down the shared handler runtime | Fixed | Subscription release no longer owns or shuts down a shared runtime. Retained Myko lifecycle regressions and the downstream Forrest workspace gates pass against the same Myko source fingerprint. |

## Preserve

Preserve the mature Myko 6 mechanisms that already solve the shared framework problem:

- author macros and inventory registration;
- lazy keyed `MapQuery` and scalar report plans;
- one materialization boundary and weak request caches;
- connection-scoped `ClientSession` ownership of guards, cancellation, sequence, cursor, and windows;
- `SessionSink` and typed pending responses before serialization;
- `MykoClient` reconnect and watch sharing;
- `QueryMapWatch` and `ViewMapWatch` as the UI-facing reactive state used by Leptos and GPUI.

Move these Myko 7 domain semantics into those owners:

- typed services, items, commands, scopes, and service-filtered activation;
- immutable event history, Redb durability, and durable command lifecycle;
- local-origin-only command execution and idempotent ingestion;
- authenticated Iroh replication with source identity, selection, checkpoints, and authorization;
- principals, claims, capabilities, grants, delegation, approvals, leases, and admission, continuation, and effect checks;
- replay-then-follow typed projections, coherent cursor-only advances, and composite frontiers;
- node readiness, peer reconciliation, discovery, pairing, and joined shutdown.

## Delete

The migration is incomplete while any of these remain as competing production implementations:

- Myko 7 `QueryHandler`, `ReportHandler`, and `ViewHandler` traits;
- `HandlerRuntime`, Myko 7 projection caches, and `myko-runtime::ApplicationClient`;
- Myko 7 `LiveCollection` as a second client and composition state model;
- typed local, Iroh, and node application-client wrappers over the duplicate runtime;
- repeated `NodeRequest` matching and optional-bag `AccessRequest` construction;
- synthetic-principal authority bypass and request-time facts fallback;
- compatibility-only scope, peer, cursor, and wire branches;
- workspace defaults and checks that define the Myko 7 island as the product.

## Required gates

The final change must prove all of the following against the real consumers:

1. The workspace contains one public handler family and one application/session/client runtime.
2. Myko 6 query, report, view, reconnect, window, server, Leptos, and GPUI tests pass.
3. Ported Myko 7 durable command, Redb restart, Iroh replication, source-reset, authority revocation, discovery, pairing, and shutdown tests pass.
4. A one-row mutation produces one keyed update through embedded, local, Iroh, and optional WebSocket connectors.
5. Durable and control streams are bounded and lossless. Compatible live diffs either coalesce correctly or force resynchronization.
6. Forrest's `cargo test -p forrest-mobile-core --lib` passes, including projection lifetime and deletion propagation.
7. Generated bindings come from the final Rust types. No compatibility wire fixtures or aliases remain unless a current consumer requires them.
8. `cargo flux` formatting, checks, tests, and strict Clippy pass with the repository's target-directory rules.

## Evidence

The codebase graph was refreshed in full mode against the final source worktree at generation `2026-09-04T17:27:18Z`. It indexed 11,140 nodes and 60,382 edges with no skipped or partially parsed Rust files. Coverage checks found no recorded issue for every relied-on authority, federation, Iroh, local, wire, and retained-core path. Direct source checks cover non-code manifests and the deletion predicates.

Final verification includes:

```text
scripts/check-myko-collapse.sh final
cargo flux run gen
cargo flux run check
cargo flux run lint
cargo flux run test
cargo clippy --all-targets --target-dir target/agent -- -D warnings
cargo clippy -p myko --all-targets --all-features --target-dir target/agent -- -D warnings
cargo test -p myko --all-features --target-dir target/agent
cargo test -p myko-federation --target-dir target/agent
cargo test -p myko-authority --target-dir target/agent
cargo test -p myko-iroh --target-dir target/agent
```

The downstream Forrest workspace format, test, strict-Clippy, architecture, and focused regression gates run through its `/home/trevor/Code/myko-federation` symlink, which resolves to this checkout. The Myko source fingerprint is checked before and after that run.
