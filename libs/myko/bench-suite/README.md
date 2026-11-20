# myko-bench-suite

Level-2 benchmark, stress and regression suite for myko. It answers one
question: does performance hold once values are realistic? It drives real
`#[myko_item]` stores, generated queries, views, reports, relationships and
`ClientSession` with rship-shaped entities at the scale of the rship load
test.

The crate is not published. Its entities register only in this crate's
inventories, so they never reach a myko build.

## Three ways to run it

| use | command | when |
| --- | --- | --- |
| Correctness, reduced scale | `cargo flux run test`, or `RUST_TEST_THREADS=1 cargo test -p myko-bench-suite --features ci` | Every change. About 30 s. |
| Timed matrix, full scale | `cargo bench -p myko-bench-suite --features bench --bench level2` | Before and after a change to the write path, queries, reports or hyphae. |
| Regression check | `cargo run --release -p myko-bench-suite --bin bench-compare -- <results.jsonl>` | After a timed run. |

The timed matrix is behind the `bench` feature. Without it `cargo bench`
does not build the target, so nothing starts a timed series by accident.

## Tests

`tests/scenarios.rs` runs every scenario at reduced scale. A test fails when
a correctness check inside the scenario fails: a row not delivered, a
sequence gap, a value out of its writer's order, a wrong value, a cascade
that is not exact, a live cache entry left behind. Timings are not asserted.

One gap on this branch is recorded in the tests, not hidden:

- `a_multi_type_mutation_is_never_seen_in_part` is `#[ignore]`. A three-way
  join shows part of a mutation that spans three entity types (gate L2-5).
  Remove the attribute when the engine settles a batch as one step.

## Timed runs

```sh
cargo bench -p myko-bench-suite --features bench --bench level2            # whole matrix
cargo bench -p myko-bench-suite --features bench --bench level2 -- sweep   # labels containing "sweep"
```

- `matrix.txt` lists every data point: label, time limit in seconds,
  scenario and arguments.
- Each run is a fresh process, because hyphae's reactive world is
  process-global.
- `MYKO_BENCH_RUNS` sets the runs per data point. The default is 5.
- Records are appended to `target/bench-suite/results-<commit>.jsonl`.
  `MYKO_BENCH_OUT` names another file. Delete the file before a rerun on the
  same commit, or the old runs are counted with the new.
- Time the series on a quiet machine. On the shared build host, take the
  measurement lock first:
  `exec 9>>/mnt/tank/targets/load-runs/load.lock; flock 9`.

One record per data point per run:

```json
{"label":"sweep-fk","run":3,"commit":"73d601f327","scale":1.0,"version":"6.17.1",
 "scenario":"sweep","params":{"kind":"fk","queries":50000,"rows":50001},
 "metrics":{"hot_call_p50_us":14.2,"spread_latency_p99_us":61.0},
 "checks":{"nothing_lost":true,"per_writer_order":true}}
```

`scale` is the `scale=` multiplier of the run: 1.0 is the scale in
`matrix.txt`. A run that does not finish is recorded as
`{"label":…,"run":…,"failed_run":true}`.

## Regression check

```sh
cargo run --release -p myko-bench-suite --bin bench-compare -- \
  target/bench-suite/results-<commit>.jsonl
```

It compares the median of the runs of each data point with `baseline.json`
and exits 1 if anything regressed. A data point regresses when:

- a tracked metric's median is worse than the baseline median by more than
  the metric's tolerance, and by more than 2 units (2 µs on a microsecond
  metric);
- a correctness check fails that the baseline does not list under
  `known_failing_checks`;
- a run did not finish, where the baseline's runs did.

The tracked metrics are the `TRACKED` list in `src/bin/bench-compare.rs`.
They are medians and per-row costs. p99 figures are recorded but not gated,
because five runs do not pin a p99.

### The tolerance

Each metric's tolerance is fixed when the baseline is written:

```
tolerance = max(25%, 2 × (max − min) / median of the baseline runs)
```

- Twice the range means a rerun of the same commit does not trip the check.
  The range of five runs is what the machine did to that metric.
- The 25% floor is for metrics whose five baseline runs happened to agree.
  On the shared host, the same commit moved by 10% to 20% between series
  with other work running. Read `baseline.json` for the tolerance each
  metric got.
- A noisy metric gets a wide tolerance and so catches only large
  regressions. That is the honest reading of a noisy metric. To tighten it,
  re-baseline on a quiet machine.

### Re-baseline

Re-baseline when a change is meant to move the numbers, or when the
reference machine changes. On the commit that becomes the reference:

```sh
rm -f target/bench-suite/results-*.jsonl
cargo bench -p myko-bench-suite --features bench --bench level2
cargo run --release -p myko-bench-suite --bin bench-compare -- \
  target/bench-suite/results-<commit>.jsonl --write-baseline
git add libs/myko/bench-suite/baseline.json
```

Say in the commit message which machine and commit produced the baseline.
Review the diff of `known_failing_checks`: a check that moved into it is a
regression being accepted.

## Layout

| path | what it is |
| --- | --- |
| `src/entities.rs` | Entities modelled on rship's Scene, Target, Emitter, BindingNode, Pulse and Bundle. |
| `src/reports.rs`, `src/views.rs` | A keyed report, a report that reads reports, a grouped report, a two-way and a three-way join. |
| `src/harness.rs` | Server context, client sessions and the delivery sink that checks drops, order and values. |
| `src/tick.rs` | The 30 Hz tick writer, the independent probe and subscription churn. |
| `src/scenarios.rs` | The scenarios. |
| `src/compat.rs` | The only code that depends on what this myko version offers. |
| `matrix.txt` | The timed data points. |
| `baseline.json` | The reference medians and tolerances. |

## Scenarios

| scenario | covers |
| --- | --- |
| `m1`, `m2`, `m3` | A 30 Hz tick of 25,000 changes against a filtered query, a joined view and a report family, with an independent probe write beside it. |
| `m5` | `m1` with subscription churn. |
| `m7`, `payload` | The same graph with another wire format or payload size. `count=1` counts clones, equality checks and serializations of the payload. |
| `idle` | The probe alone. |
| `sweep` | Write-to-delivery latency and write cost as the live query count grows to 50,000. |
| `churn_cost` | A client connecting, subscribing four ways and leaving. |
| `chain` | A report that reads reports, 1, 5 and 20 levels deep. |
| `belongs_to` | Foreign-key index churn, cascade delete and `owns_many`. |
| `bursts` | Event bursts on one key. |
| `atomic` | One mutation across three entity types, read by a three-way join. |
| `shared_params` | Twelve identical subscriptions. |
| `setup` | Load from an empty store. |
| `write_cost` | The cost of one changed row, component by component. |
| `snapshot` | `query_snapshot` after a write: by id, by ids, by a `belongs_to` key and by an unindexed field. |

The subscription path is the server's: the registered parser reads the wire
JSON, the registered cell factory builds the cell, and `ClientSession`
subscribes it. There is no socket. The writer queues each response and
drain threads serialize it, as `ChannelWriter` and its writer tasks do.
