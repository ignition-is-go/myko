# Native CI compilation profile

The native quality-gates job disables dev/test debug symbols and incremental
compilation for every Cargo invocation, including generation, Clippy, tests,
PostgreSQL tests and feature-boundary checks. Optimization, debug assertions,
overflow checks, features and test selection retain their existing settings.
Failure diagnostics retain panic messages and test output, but symbolized
backtraces and source-level debugging have less information.

`CI_NATIVE_CACHE_POLICY` supplies both the Flux snapshot scope and the alternative
setup-rust cache shared key. The pinned Flux key includes `CARGO_PROFILE_*`
variables, but does not include `CARGO_INCREMENTAL`. The explicit namespace keeps
this complete policy separate from earlier compiler snapshots, including the
default-branch fallback. Change the policy version when its compatibility changes.
It does not remove targets already present on a runner.

Local qualification used Rust 1.97.1, four build jobs, serial Rust tests and the
unchanged native command bodies. Checks, generation, strict lint, tests and all
four feature-boundary checks passed. The three PostgreSQL command bodies passed
against an isolated PostgreSQL 16.15 fixture. That fixture used a different user
and locale setup from CI; CI continues to execute its existing setup scripts.

The restored compiler target contained 7.81 GiB of apparent file data. Actual
Flux 0.12 local-store save and cold restore passed with a 1.62 GiB compressed
archive. Sampled workspace usage reached 12.09 GiB, including dependency caches
and the local snapshot store; the separate PostgreSQL fixture occupied about
91 MiB. Samples can miss transient peaks, and local-store transfer differs from
S3. These results qualify that workload, not a measured comparison against the
previous profile or a guarantee of future runner capacity. GPUI and WebAssembly
jobs retain their existing policies.
