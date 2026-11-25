//! Deploy-time overrides for every time-based limit the server enforces.
//!
//! Each limit reads one environment variable on first use. Unset keeps the
//! built-in default, `0` disables the limit entirely (wait forever / never
//! expire), and anything else that is not a non-negative integer panics with a
//! message naming the variable. [`validate`] reads them all at startup so a typo
//! fails the boot rather than the first request that happens to hit the limit.

use std::{future::Future, sync::OnceLock, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Secs,
    Millis,
}

impl Unit {
    const fn name(self) -> &'static str {
        match self {
            Self::Secs => "seconds",
            Self::Millis => "milliseconds",
        }
    }

    const fn duration(self, value: u64) -> Duration {
        match self {
            Self::Secs => Duration::from_secs(value),
            Self::Millis => Duration::from_millis(value),
        }
    }
}

/// The limit expired before the guarded work finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimedOut;

/// One environment-configurable time limit; `None` once resolved means disabled.
pub struct TimeLimit {
    env: &'static str,
    unit: Unit,
    default: u64,
    resolved: OnceLock<Option<Duration>>,
}

impl TimeLimit {
    const fn new(env: &'static str, unit: Unit, default: u64) -> Self {
        Self {
            env,
            unit,
            default,
            resolved: OnceLock::new(),
        }
    }

    pub const fn env(&self) -> &'static str {
        self.env
    }

    /// The configured limit, or `None` when it is disabled.
    ///
    /// # Panics
    /// Panics when the variable is set to something other than a non-negative integer.
    #[expect(
        clippy::panic,
        reason = "a malformed limit must fail loudly; Server::run validates first so this is a backstop"
    )]
    pub fn get(&self) -> Option<Duration> {
        *self.resolved.get_or_init(|| match self.try_from_env() {
            Ok(limit) => limit,
            Err(reason) => panic!("{reason}"),
        })
    }

    /// Read the variable without caching or panicking.
    pub fn try_from_env(&self) -> Result<Option<Duration>, String> {
        match std::env::var(self.env) {
            Ok(value) => self.parse(Some(&value)),
            Err(std::env::VarError::NotPresent) => self.parse(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(self.invalid()),
        }
    }

    fn parse(&self, raw: Option<&str>) -> Result<Option<Duration>, String> {
        let value = raw.map_or(Ok(self.default), |raw| {
            raw.trim().parse::<u64>().map_err(|_| self.invalid())
        })?;
        Ok((value != 0).then(|| self.unit.duration(value)))
    }

    fn invalid(&self) -> String {
        format!(
            "{} must be a non-negative integer number of {} (0 disables the limit)",
            self.env,
            self.unit.name()
        )
    }

    /// Run `future` under this limit; a disabled limit waits forever.
    pub async fn run<F: Future>(&self, future: F) -> Result<F::Output, TimedOut> {
        match self.get() {
            Some(limit) => tokio::time::timeout(limit, future)
                .await
                .map_err(|_| TimedOut),
            None => Ok(future.await),
        }
    }

    /// Wait on a flume receiver under this limit; a disabled limit waits forever.
    pub fn recv<T>(&self, rx: &flume::Receiver<T>) -> Result<T, String> {
        self.get().map_or_else(
            || rx.recv().map_err(|err| err.to_string()),
            |limit| rx.recv_timeout(limit).map_err(|err| err.to_string()),
        )
    }
}

pub static POSTGRES_CATCH_UP_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_POSTGRES_CATCH_UP_TIMEOUT_SECS", Unit::Secs, 300);
pub static POSTGRES_FLUSH_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_POSTGRES_FLUSH_TIMEOUT_SECS", Unit::Secs, 10);
pub static POSTGRES_CONNECT_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_POSTGRES_CONNECT_TIMEOUT_SECS", Unit::Secs, 10);
pub static POSTGRES_TCP_USER_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_POSTGRES_TCP_USER_TIMEOUT_SECS", Unit::Secs, 30);
pub static HTTP_HEAD_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_HTTP_HEAD_TIMEOUT_SECS", Unit::Secs, 10);
pub static HTTP_AUTH_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_HTTP_AUTH_TIMEOUT_SECS", Unit::Secs, 10);
pub static HTTP_SHUTDOWN_DRAIN_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_HTTP_SHUTDOWN_DRAIN_TIMEOUT_MS", Unit::Millis, 250);
pub static OAUTH_BODY_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_OAUTH_BODY_TIMEOUT_SECS", Unit::Secs, 10);
pub static WS_OUTBOUND_COMMAND_TTL: TimeLimit =
    TimeLimit::new("MYKO_WS_OUTBOUND_COMMAND_TTL_SECS", Unit::Secs, 10);
pub static MCP_EXECUTE_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_MCP_EXECUTE_TIMEOUT_SECS", Unit::Secs, 30);
pub static MCP_SCRIPT_DEADLINE: TimeLimit =
    TimeLimit::new("MYKO_MCP_SCRIPT_DEADLINE_SECS", Unit::Secs, 10);
pub static MCP_QUERY_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_MCP_QUERY_TIMEOUT_SECS", Unit::Secs, 5);
pub static MCP_REPORT_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_MCP_REPORT_TIMEOUT_SECS", Unit::Secs, 5);
pub static MCP_COMMAND_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_MCP_COMMAND_TIMEOUT_SECS", Unit::Secs, 10);
pub static MCP_CONNECT_TIMEOUT: TimeLimit =
    TimeLimit::new("MYKO_MCP_CONNECT_TIMEOUT_SECS", Unit::Secs, 5);

const ALL: [&TimeLimit; 15] = [
    &POSTGRES_CATCH_UP_TIMEOUT,
    &POSTGRES_FLUSH_TIMEOUT,
    &POSTGRES_CONNECT_TIMEOUT,
    &POSTGRES_TCP_USER_TIMEOUT,
    &HTTP_HEAD_TIMEOUT,
    &HTTP_AUTH_TIMEOUT,
    &HTTP_SHUTDOWN_DRAIN_TIMEOUT,
    &OAUTH_BODY_TIMEOUT,
    &WS_OUTBOUND_COMMAND_TTL,
    &MCP_EXECUTE_TIMEOUT,
    &MCP_SCRIPT_DEADLINE,
    &MCP_QUERY_TIMEOUT,
    &MCP_REPORT_TIMEOUT,
    &MCP_COMMAND_TIMEOUT,
    &MCP_CONNECT_TIMEOUT,
];

/// Resolve every limit so a malformed value fails startup with the variable named.
///
/// # Errors
/// Returns every malformed variable, one per line.
pub fn validate() -> Result<(), String> {
    let errors: Vec<String> = ALL
        .iter()
        .filter_map(|limit| match limit.try_from_env() {
            Ok(value) => {
                let _ = limit.resolved.set(value);
                None
            }
            Err(reason) => Some(reason),
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static LIMIT: TimeLimit = TimeLimit::new("MYKO_TEST_LIMIT_SECS", Unit::Secs, 10);

    #[test]
    fn unset_uses_default() {
        assert_eq!(LIMIT.parse(None), Ok(Some(Duration::from_secs(10))));
    }

    #[test]
    fn zero_disables_the_limit() {
        assert_eq!(LIMIT.parse(Some("0")), Ok(None));
    }

    #[test]
    fn value_overrides_default_in_its_unit() {
        assert_eq!(LIMIT.parse(Some("900")), Ok(Some(Duration::from_mins(15))));
        let millis = TimeLimit::new("MYKO_TEST_LIMIT_MS", Unit::Millis, 250);
        assert_eq!(
            millis.parse(Some("40")),
            Ok(Some(Duration::from_millis(40)))
        );
    }

    #[test]
    fn garbage_is_an_error_naming_the_variable() {
        for raw in ["-1", "forever", "1.5", ""] {
            let result = LIMIT.parse(Some(raw));
            assert!(
                matches!(&result, Err(error) if error.contains("MYKO_TEST_LIMIT_SECS")),
                "{raw:?} -> {result:?}"
            );
        }
    }

    #[test]
    fn every_limit_has_a_distinct_env_name() {
        let mut names: Vec<_> = ALL.iter().map(|limit| limit.env()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }
}
