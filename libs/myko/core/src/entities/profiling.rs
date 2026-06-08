//! No-sudo CPU profiling for myko servers.
//!
//! `StartProfile` / `StopProfile` are framework commands (auto-surfaced on the
//! MCP endpoint). The same start/report routines back an in-process `profile()`
//! helper for profiling unit tests and benches. All real work is gated behind
//! the `profiling` feature and native targets; otherwise the commands return a
//! "profiling not enabled" error.

use crate::command::{CommandContext, CommandError, CommandHandler};

#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
mod imp {
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;

    /// One active profile at a time (pprof permits a single process-wide guard).
    pub(super) struct ActiveProfile {
        pub guard: pprof::ProfilerGuard<'static>,
        pub started: Instant,
        pub frequency_hz: u32,
    }

    static ACTIVE: OnceLock<Mutex<Option<ActiveProfile>>> = OnceLock::new();

    fn slot() -> &'static Mutex<Option<ActiveProfile>> {
        ACTIVE.get_or_init(|| Mutex::new(None))
    }

    /// Build and store a profiler guard. Errs if one is already running.
    pub(super) fn start(frequency_hz: u32) -> Result<(), String> {
        let mut guard_slot = slot().lock().unwrap_or_else(|e| e.into_inner());
        if guard_slot.is_some() {
            return Err("a profile is already running; call StopProfile first".to_string());
        }
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency_hz as i32)
            .blocklist(&["libc", "libpthread", "vdso", "libgcc"])
            .build()
            .map_err(|e| format!("failed to start profiler: {e}"))?;
        *guard_slot = Some(ActiveProfile {
            guard,
            started: Instant::now(),
            frequency_hz,
        });
        Ok(())
    }

    /// Stop the active profile and render artifacts. Errs if none active.
    pub(super) fn stop() -> Result<super::StopProfileOutput, String> {
        let active = slot()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| "no active profile to stop".to_string())?;
        let report = active
            .guard
            .report()
            .build()
            .map_err(|e| format!("failed to build report: {e}"))?;
        Ok(render(&report, active.started.elapsed(), active.frequency_hz))
    }

    /// Format folded stacks + write an SVG; assemble the output struct.
    pub(super) fn render(
        report: &pprof::Report,
        elapsed: std::time::Duration,
        frequency_hz: u32,
    ) -> super::StopProfileOutput {
        let folded = fold(report);
        let sample_count: usize = report.data.values().map(|v| (*v).max(0) as usize).sum();
        let svg_path = write_svg(report);
        super::StopProfileOutput {
            folded,
            svg_path,
            sample_count,
            duration_ms: elapsed.as_millis() as u64,
            frequency_hz,
        }
    }

    /// Collapse `report.data` into inferno "folded" format:
    /// `root;child;leaf <count>` per line, root-first.
    fn fold(report: &pprof::Report) -> String {
        let mut lines: Vec<String> = report
            .data
            .iter()
            .map(|(frames, count)| {
                // frames.frames is leaf-first; reverse for root-first folded form.
                let mut parts: Vec<String> = Vec::new();
                for frame in frames.frames.iter().rev() {
                    // inner Vec = inlined symbols at this frame; take the outermost name.
                    if let Some(sym) = frame.first() {
                        parts.push(sym.name());
                    }
                }
                format!("{} {}", parts.join(";"), count)
            })
            .collect();
        lines.sort();
        lines.join("\n")
    }

    fn write_svg(report: &pprof::Report) -> Option<String> {
        use std::io::Write;
        let dir = std::env::var("MYKO_PROFILE_DIR").unwrap_or_else(|_| "./perf-traces".to_string());
        if std::fs::create_dir_all(&dir).is_err() {
            return None;
        }
        let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
        let path = format!("{dir}/profile-{stamp}.svg");
        let mut buf: Vec<u8> = Vec::new();
        if report.flamegraph(&mut buf).is_err() {
            return None;
        }
        let mut file = std::fs::File::create(&path).ok()?;
        file.write_all(&buf).ok()?;
        std::fs::canonicalize(&path)
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
    }
}

/// Result of `StopProfile`. `folded` is the agent-readable primary artifact.
#[myko_macros::myko_report_output]
pub struct StopProfileOutput {
    /// Collapsed stacks, one per line: `root;child;leaf <count>`. Sortable/greppable.
    pub folded: String,
    /// Absolute path to a flamegraph `.svg` written to disk for humans (if writable).
    pub svg_path: Option<String>,
    pub sample_count: usize,
    pub duration_ms: u64,
    pub frequency_hz: u32,
}

/// Outcome of the in-process `profile()` helper.
#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
pub struct ProfileReport {
    pub folded: String,
    pub svg_path: Option<String>,
    pub sample_count: usize,
}

/// Profile a closure in-process (for unit tests / benches). Returns the
/// closure's value alongside the collected profile.
#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
pub fn profile<T>(frequency_hz: u32, f: impl FnOnce() -> T) -> (T, ProfileReport) {
    start_profile(frequency_hz).expect("profile(): another profile already active");
    let value = f();
    let out = stop_profile().expect("profile(): stop failed");
    (
        value,
        ProfileReport {
            folded: out.folded,
            svg_path: out.svg_path,
            sample_count: out.sample_count,
        },
    )
}

/// Start the process-wide profiler. Shared by `StartProfile` and `profile()`.
#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
pub fn start_profile(frequency_hz: u32) -> Result<(), String> {
    imp::start(frequency_hz)
}

/// Stop the process-wide profiler and render artifacts. Shared by `StopProfile`
/// and `profile()`.
#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
pub fn stop_profile() -> Result<StopProfileOutput, String> {
    imp::stop()
}

/// Default sampling frequency when the caller does not specify one.
pub const DEFAULT_FREQUENCY_HZ: u32 = 99;

/// Resolve the effective sampling frequency from an optional override.
pub fn effective_hz(frequency_hz: Option<u32>) -> u32 {
    frequency_hz.unwrap_or(DEFAULT_FREQUENCY_HZ)
}

/// Start CPU sampling of this server process.
#[myko_macros::myko_command(bool)]
pub struct StartProfile {
    /// Sampling frequency in Hz. Defaults to 99 when omitted.
    #[serde(default)]
    pub frequency_hz: Option<u32>,
}

/// Stop CPU sampling and return the collected profile.
#[myko_macros::myko_command(StopProfileOutput)]
pub struct StopProfile {}

#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
impl CommandHandler for StartProfile {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        start_profile(effective_hz(self.frequency_hz))
            .map(|()| true)
            .map_err(|message| CommandError {
                tx: ctx.tx().to_string(),
                command_id: ctx.command_id.to_string(),
                message,
            })
    }
}

#[cfg(not(all(not(target_arch = "wasm32"), feature = "profiling")))]
impl CommandHandler for StartProfile {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        Err(CommandError {
            tx: ctx.tx().to_string(),
            command_id: ctx.command_id.to_string(),
            message: "profiling not enabled (build with the `profiling` feature on a native target)"
                .to_string(),
        })
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "profiling"))]
impl CommandHandler for StopProfile {
    fn execute(self, ctx: CommandContext) -> Result<StopProfileOutput, CommandError> {
        stop_profile().map_err(|message| CommandError {
            tx: ctx.tx().to_string(),
            command_id: ctx.command_id.to_string(),
            message,
        })
    }
}

#[cfg(not(all(not(target_arch = "wasm32"), feature = "profiling")))]
impl CommandHandler for StopProfile {
    fn execute(self, ctx: CommandContext) -> Result<StopProfileOutput, CommandError> {
        Err(CommandError {
            tx: ctx.tx().to_string(),
            command_id: ctx.command_id.to_string(),
            message: "profiling not enabled (build with the `profiling` feature on a native target)"
                .to_string(),
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32"), feature = "profiling"))]
mod tests {
    use super::*;

    /// Burns CPU so the sampler is guaranteed to collect frames.
    fn burn(iters: u64) -> u64 {
        let mut acc = 0u64;
        for i in 0..iters {
            acc = acc.wrapping_add(i.wrapping_mul(2654435761));
        }
        acc
    }

    #[test]
    fn profile_helper_collects_folded_stacks() {
        let (out, report) = profile(997, || burn(50_000_000));
        assert!(out != 0, "work ran");
        assert!(report.sample_count > 0, "collected at least one sample");
        assert!(
            report.folded.contains("burn"),
            "folded stacks should name the hot fn; got:\n{}",
            report.folded
        );
    }

    #[test]
    fn double_start_is_rejected() {
        start_profile(997).expect("first start ok");
        assert!(start_profile(997).is_err(), "second concurrent start rejected");
        let _ = stop_profile();
    }

    #[test]
    fn stop_without_start_errors() {
        let _ = stop_profile();
        assert!(stop_profile().is_err(), "stop with no active profile errors");
    }

    #[test]
    fn commands_serialize_and_default_frequency() {
        let start = StartProfile { frequency_hz: None };
        let v = serde_json::to_value(&start).unwrap();
        let back: StartProfile = serde_json::from_value(v).unwrap();
        assert_eq!(back.frequency_hz, None);
        assert_eq!(effective_hz(back.frequency_hz), 99, "None → 99 Hz default");
        assert_eq!(effective_hz(Some(250)), 250);

        // StopProfileOutput is the declared result type and round-trips.
        let out = StopProfileOutput {
            folded: "a;b 3".into(),
            svg_path: None,
            sample_count: 3,
            duration_ms: 10,
            frequency_hz: 99,
        };
        let rv = serde_json::to_value(&out).unwrap();
        let _: StopProfileOutput = serde_json::from_value(rv).unwrap();
    }
}

crate::register_ts_export!(StopProfileOutput);
