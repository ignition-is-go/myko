# Myko 7 collapse implementation plan

**Goal:** Keep the retained Myko 6 application, reactive, session, client, and UI runtime; move the genuine Myko 7 durable federation, authority, and node semantics into those owners; delete the duplicate runtime without maintaining either old wire protocol.

**Design:** `docs/superpowers/specs/2026-09-04-myko-7-collapse-into-retained-v6-ownership.md`

**Review:** `docs/superpowers/specs/2026-09-04-myko-7-thermo-review-update.md`

## Done predicate

The migration is complete only when all statements are true at the same revision:

- retained `myko` contains the only public query, report, view, and command handler family;
- retained `ClientSession` and `SessionSink` contain the only session and delivery ownership;
- retained `MykoClient`, `QueryMapWatch`, and `ViewMapWatch` contain the only application client and watch ownership;
- `myko-app`, `myko-app-macros`, `myko-runtime`, `myko-session`, and `myko-websocket-gateway` have no production callers and are deleted;
- local, Iroh, embedded, and WebSocket paths are connectors or sinks over the retained runtime;
- `myko-items::ItemProjection<T>` stores `T::Id`, and live map state publishes rows plus one revision atomically;
- one exhaustive frame conversion produces `PreparedRequest` and `AccessTarget`; optional-bag authorization and repeated request matching are deleted;
- authority admission, continuation, and effect evaluation use indexed facts and a narrow trusted framework write path;
- compatibility-only wire, scope, peer, and cursor aliases are deleted; only an explicit versioned storage migration may remain;
- durable commands, Redb restart, authenticated Iroh replication, source reset, composite frontiers, revocation, discovery, pairing, readiness, and joined shutdown pass in their final owners;
- the two focused Forrest regressions and the full `forrest-mobile-core` library suite pass;
- generated bindings contain no deleted crate or compatibility wire names;
- `scripts/check-myko-collapse.sh final`, `cargo flux run gen`, `cargo flux run check`, `cargo flux run test`, and strict Clippy pass.

## Work list

- [x] Refresh the prior architecture review against the current dirty worktree.
- [x] Reproduce and root-cause the Forrest projection lifetime and deletion regressions.
- [x] Ground retained Myko 6 ownership and genuine Myko 7 semantics in source, history, tickets, and consumer tests.
- [x] Compare independent retained-v6 architecture candidates and cross-judge them.
- [x] Select the focused-crate design and graft its transaction, revision, source-reset, cache-key, and verification details.
- [x] Replace the wrong-direction convergence checker with the collapse checker and record its failing baseline.
- [x] Unit 1: extend retained item, service, scope, and command authoring contracts; migrate generated macros and registrations.
- [x] Unit 2: migrate Myko 7 handler callers onto retained traits and materialization; delete `myko-app` and `myko-app-macros`.
- [x] Unit 3: connect typed federation projections to retained `MapQuery`/`Materialize`; delete `HandlerRuntime`, per-client collection bridges, and duplicate caches while retaining one lifecycle value type.
- [x] Unit 4: move prepared request routing, follows, and bounded delivery into retained `ClientSession`; delete `myko-session`.
- [x] Unit 5: add connector support and durable command watches to retained `MykoClient`; delete `myko-runtime` and typed transport client wrappers.
- [x] Unit 6: replace wire requests and authorization metadata with `PreparedRequest` and typed `AccessTarget`; delete legacy wire and repeated matching.
- [x] Unit 7: split authority evaluation, require indexed facts, and install the trusted framework commit path.
- [x] Unit 8: move WebSocket edge behavior into retained `myko-server`, flatten remaining monolith roots, and delete compatibility branches.
- [x] Unit 9: reset workspace defaults, regenerate bindings, and run all architecture and consumer gates.
- [x] Run an independent final review over the review, design, decision trail, final diff, and verification evidence.
- [x] Resolve every material review finding or record it under `Attention` with evidence.

## Unit protocol

Each unit follows one auditable loop:

1. State a falsifiable ownership or behavior hypothesis.
2. Run the narrow current-state test and record `VERIFIED`, `NOT VERIFIED`, or `INCONCLUSIVE`.
3. Migrate all callers in that unit before deleting the superseded API.
4. Do not add a compatibility shim solely to keep an intermediate shape alive.
5. Run the narrow test, the applicable collapse-checker phase, and a focused package check.
6. Inspect the diff, caller graph, and generated output affected by the unit.
7. Append the evidence and result to the decision trail.

The dirty tree is a staging branch. Each unit should be buildable where practical, but no intermediate compatibility design is part of the product contract.

## Verification matrix

### Structure

- Collapse checker rejects duplicate crates, imports, handler traits, session/client owners, string-keyed projections, collection bridges, optional-bag access, and named compatibility branches.
- Workspace metadata selects retained `myko`, `myko-server`, retained UI crates, and focused federation/authority/node/transport crates.
- Codebase graph and direct source checks agree that every production caller has moved before deletion.

### Reactive and session behavior

- One-row changes yield one keyed update over embedded, local, Iroh, and WebSocket connectors.
- Projection rows and `MapRevision { diff, frontier, epoch, liveness }` publish in one Hyphae wave.
- Reconnect resets the epoch and accepts a fresh sequence-zero snapshot without an intermediate disconnect callback.
- Durable and control lanes are bounded and lossless; compatible live map updates coalesce or force resynchronization.
- Closing one subscription never invalidates another source or shared projection.

### Durable and authority behavior

- Admission, local-origin check, effect authorization, journal append, projection publication, and checkpoint advancement follow the design's ordered commit protocol.
- A changed source node id resets replay; filtered cursors are never reused across selections.
- Restart preserves identity, history, command lifecycle, and replication checkpoints.
- Revocation and continuation checks stop live access without a synthetic internal principal.

### Consumer and generated output

Run from `/home/trevor/Code/forrest` when the focused runtime cutover is ready:

```bash
cargo test -p forrest-mobile-core --lib tests::local_agent_runtime_stream_projects_typed_live_output -- --exact --nocapture
cargo test -p forrest-mobile-core --lib tests::local_declared_network_resource_can_be_removed -- --exact --nocapture
cargo test -p forrest-mobile-core --lib -- --nocapture
```

Run repository-wide formatting, generation, checks, and tests through `cargo flux`. Use direct `cargo test` only for focused package or regression checks and always use `--target-dir target/agent` inside this repository.

## Attention

No unresolved material findings. The final audit verified the retained-owner collapse, modular roots, typed request and authority boundaries, bounded durable delivery, typed projection identity, compatibility deletion, generated bindings, and downstream consumer behavior. B1-B14 are resolved at the final Myko source fingerprint; wire compatibility is deliberately not part of the contract.
