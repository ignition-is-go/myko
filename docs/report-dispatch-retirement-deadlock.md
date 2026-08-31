# Draft investigation: report dispatch versus subscription retirement

## Scope and status

This draft includes the candidate lock fix and three regression tests, following
operator approval to add source edits. It is not approved for merge, release,
or deployment. Nothing has been released or deployed.

Investigated baseline: Myko 6.5.9, commit
`9c8ff87fdd277cf1778a4f9d5c1546f7be543482`.

## Operational context

On 2026-08-31, Pulse Cluster experienced two failures of different severity:

- Restart `70c5d088-10b1-4618-a44c-4e4a627d6650` displayed successful stop actions
  and then missed intermediate launch progress. It eventually completed at
  01:32:29 EDT: total 4m37s, including 4m13s launch confirmation.
- Down `455bef66-d010-467b-b474-941f74565e74`, admitted at 02:12:59 EDT, remained
  pending on render-12 and render-13 for more than ten minutes. A fresh server
  report agreed with the CLI; this was not merely a stale terminal display.

Both affected agents were connected and emitting fresh observations. Render-12
was already idle; render-13 still had UnrealEditor PID 4932 rendering at about
30 fps. Both local action journals ended with the previous restart's start
actions around 01:28:09 EDT; neither contained the new down operation.

Restarting only the two PulseNode services caused the existing down operation
to complete. Subsequent checks showed no UnrealEditor process on either host,
no active deployment or operation, and cluster phase `stopped`. No machine
reboots or additional cluster lifecycle commands were issued for recovery.

The deployed agents were `2.11.11-rc.8`, which **already renews action report
subscriptions every 30 seconds**. Adding that renewal again is not a fix for
this incident. The runtime used Myko 6.5.9.

## Reproduced defect

In `libs/myko/core/src/client/mod.rs` on the baseline:

1. Native report dispatch invokes a handler while retaining the read guard
   returned by `report_handlers.get`. The WASM dispatch path has the same shape.
2. The typed and raw report callbacks upgrade a weak report Cell reference.
3. If subscription retirement drops the external reference while the callback
   owns that Cell, the callback can release the final strong reference.
4. Cell teardown invokes `report_cancel_guard`, which calls
   `report_handlers.remove` for the same transaction.
5. Removal needs the write lock on the same DashMap shard whose read lock is
   still held by this callback's dispatcher. The dispatcher deadlocks itself.

This explains how a report delivery worker can stop while independent heartbeat
and command activity continues. It is a deterministic library defect consistent
with the incident, **not a captured proof of the exact production thread stack**:
no pre-restart agent dump was retained. The first incident's precise relationship
to this race is likewise not established.

## Deterministic reproduction procedure

The local regression uses the actual report cancellation guard, not sleeps or
an assumed network outage:

1. Create a Myko client with auto-reconnect disabled and a raw report watch with
   a fixed transaction ID. No live server or render host is required.
2. Install a test response callback that mirrors the production callback's weak
   Cell upgrade. Use two barriers to pause it after the upgrade and before the
   upgraded reference is released.
3. Dispatch a response on another thread. Wait until the callback owns the Cell.
4. Drop the external report reference, then release the callback barrier.
5. Require dispatch completion within two seconds. With the original
   lock-held callback invocation, this assertion fails: Cell cleanup blocks on
   removal from the handler table.
6. With the candidate lock fix, require completion, verify the retired handler
   was removed, and dispatch another report successfully through the same
   handler table.

The test deliberately controls the weak-reference/retirement overlap; it does
not depend on the probability of encountering the race in a live restart loop.

## Local evidence

The three regression tests are included in this branch. For the negative
control, only the dispatch helper was temporarily
changed back to invoking the callback under the original DashMap read guard.
That reproduces the baseline's locking behavior while retaining the test harness.

Negative-control command:

```sh
cargo test -p myko --lib \
  report_dispatch_tests::retiring_report_during_response_does_not_block_subsequent_reports \
  --target-dir target/agent -- --nocapture
```

Observed result: exit 101, one failed test after 2.00s, with assertion message
`retiring a report deadlocked its response callback`.

After restoring the candidate fix:

```sh
cargo test -p myko --lib report_dispatch_tests --target-dir target/agent --quiet
```

Observed result: **3 passed, 0 failed**:

- `report_callback_can_cancel_itself_without_deadlocking_dispatch`
- `cancelled_report_responses_are_ignored`
- `retiring_report_during_response_does_not_block_subsequent_reports`

Broader default-feature library testing was **not green**:

| Checkout | Result |
| --- | --- |
| Candidate fix and three added tests | 174 passed, 11 failed |
| Unmodified baseline, no added tests | 171 passed, 11 failed |

Failures involved query-map, graph, relation-index, and history-reactivity tests.
The failing sets were not identical between runs. This establishes that the
baseline is also red in this environment, not that every broader failure has
been diagnosed or proven unrelated. No full-suite or fleet validation is claimed.

## Proposed correction for review

Store report handlers with shared ownership, clone the handler while holding
the table guard, release the guard, and only then invoke the callback. Apply the
same rule to native and WASM dispatch and both typed/raw report registration.
Do not alter lifecycle policy, action fences, journal replay, or readiness rules.

Before promotion, review cancellation concurrency semantics, validate against
the deployed dependency set, resolve or account for broader test failures, and
exercise subscription retirement/reconnect while repeatedly completing fenced
operations in an authorized isolated environment. This draft authorizes none
of the publication, rollout, or live lifecycle steps.
