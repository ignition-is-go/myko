//! Per-client MCP tool filters.
//!
//! Two complementary, client-configured filters compose into [`ClientFilters`]:
//!
//! 1. **Name filter** — glob allow/deny over tool names. Denied tools
//!    disappear from `tools/list` and a `tools/call` against them returns
//!    method-not-found. Source:
//!    - HTTP/WS: `X-Myko-Tools-Allow` and `X-Myko-Tools-Deny` request headers.
//!    - Stdio: `MYKO_MCP_TOOLS_ALLOW` / `MYKO_MCP_TOOLS_DENY` env vars.
//!
//! 2. **Call filter** — argument-aware constraints on `tools/call`. The
//!    client expresses per-tool, per-argument allow/deny value lists. A
//!    rejection surfaces as MCP `isError: true` content with a descriptive
//!    message — the "invalid input data" shape per spec, distinct from the
//!    protocol-level `-32601` used for unknown / name-denied tools. Source:
//!    - HTTP/WS: `X-Myko-Tool-Constraints` request header (JSON).
//!    - Stdio: `MYKO_MCP_TOOL_CONSTRAINTS` env var (JSON).
//!
//! ### Constraint JSON shape
//!
//! ```json
//! {
//!   "command:RunPlaybook": {
//!     "playbook_id": { "allow": ["site", "deploy"] }
//!   },
//!   "command:Tag": {
//!     "namespace": { "deny": ["prod"] }
//!   }
//! }
//! ```
//!
//! Argument paths are top-level JSON keys on the `arguments` object. Allow
//! is positive (must match one of); deny excludes (must not match any).
//! Deny wins. A missing argument when an `allow` list is set → denied.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

// ─── Header / env names ────────────────────────────────────────────────────

/// HTTP header carrying the name allowlist.
pub const ALLOW_HEADER: &str = "X-Myko-Tools-Allow";
/// HTTP header carrying the name denylist.
pub const DENY_HEADER: &str = "X-Myko-Tools-Deny";
/// HTTP header carrying the JSON tool-call constraint spec.
pub const CONSTRAINTS_HEADER: &str = "X-Myko-Tool-Constraints";

/// Stdio env var carrying the name allowlist.
pub const ALLOW_ENV: &str = "MYKO_MCP_TOOLS_ALLOW";
/// Stdio env var carrying the name denylist.
pub const DENY_ENV: &str = "MYKO_MCP_TOOLS_DENY";
/// Stdio env var carrying the JSON tool-call constraint spec.
pub const CONSTRAINTS_ENV: &str = "MYKO_MCP_TOOL_CONSTRAINTS";

// ─── Name patterns ─────────────────────────────────────────────────────────

/// A glob pattern for matching tool names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    /// `*` — matches everything.
    Any,
    /// `prefix*` — matches names starting with `prefix`.
    Prefix(String),
    /// `*suffix` — matches names ending with `suffix`.
    Suffix(String),
    /// Exact match.
    Exact(String),
}

impl Pattern {
    /// Parse a single glob pattern. Empty string returns `None`.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if s == "*" {
            return Some(Pattern::Any);
        }
        match (s.starts_with('*'), s.ends_with('*')) {
            (true, true) if s.len() == 2 => Some(Pattern::Any),
            (false, true) => Some(Pattern::Prefix(s[..s.len() - 1].to_string())),
            (true, false) => Some(Pattern::Suffix(s[1..].to_string())),
            _ => Some(Pattern::Exact(s.to_string())),
        }
    }

    /// Test whether `name` matches this pattern.
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Pattern::Any => true,
            Pattern::Prefix(p) => name.starts_with(p),
            Pattern::Suffix(s) => name.ends_with(s),
            Pattern::Exact(e) => name == e,
        }
    }
}

// ─── Argument constraints ──────────────────────────────────────────────────

/// Allow/deny constraint on one argument of one tool.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ArgConstraint {
    #[serde(default)]
    pub allow: Option<Vec<Value>>,
    #[serde(default)]
    pub deny: Option<Vec<Value>>,
}

impl ArgConstraint {
    /// Returns `Ok(())` if `value` is permitted by this constraint, or
    /// `Err(reason)` with a short human-readable message.
    pub fn check(&self, arg_name: &str, value: Option<&Value>) -> Result<(), String> {
        if let Some(deny) = &self.deny
            && let Some(v) = value
            && deny.contains(v)
        {
            return Err(format!("argument `{}` value not allowed", arg_name));
        }
        if let Some(allow) = &self.allow {
            match value {
                Some(v) if allow.contains(v) => return Ok(()),
                Some(_) => return Err(format!("argument `{}` value not in allowlist", arg_name)),
                None => return Err(format!("argument `{}` is required by filter", arg_name)),
            }
        }
        Ok(())
    }
}

// ─── ClientFilters ─────────────────────────────────────────────────────────

/// Per-client filter combining a name allow/deny and per-call argument
/// constraints. Driven by request headers (HTTP/WS) or environment variables
/// (stdio).
#[derive(Debug, Clone, Default)]
pub struct ClientFilters {
    name_allow: Vec<Pattern>,
    name_deny: Vec<Pattern>,
    /// `tool_name -> { arg_name -> ArgConstraint }`. Empty = no call constraints.
    call: HashMap<String, HashMap<String, ArgConstraint>>,
}

impl ClientFilters {
    /// A filter that permits everything (no headers / no env vars set).
    pub fn allow_all() -> Self {
        Self::default()
    }

    /// Build from raw strings. `constraints_json` is parsed lenient — invalid
    /// JSON is treated as no constraints (the request is not rejected for
    /// malformed filter config; that would be a footgun for ops). A parse
    /// error is logged.
    pub fn from_strings(
        name_allow: Option<&str>,
        name_deny: Option<&str>,
        constraints_json: Option<&str>,
    ) -> Self {
        let name_allow = parse_patterns(name_allow);
        let name_deny = parse_patterns(name_deny);
        let call = parse_constraints(constraints_json);
        Self {
            name_allow,
            name_deny,
            call,
        }
    }

    /// `true` iff the tool name is permitted by the name allow/deny rules.
    ///
    /// Deny wins. Empty allow list means "allow anything not denied".
    pub fn allows_name(&self, name: &str) -> bool {
        if self.name_deny.iter().any(|p| p.matches(name)) {
            return false;
        }
        if self.name_allow.is_empty() {
            return true;
        }
        self.name_allow.iter().any(|p| p.matches(name))
    }

    /// Apply argument constraints for a `tools/call`. `Ok(())` if no
    /// constraints apply or every constraint passes. `Err(message)` if any
    /// constraint rejects, with a short human-readable reason.
    ///
    /// Name-level allow/deny is *not* re-checked here; callers should run
    /// [`allows_name`](Self::allows_name) first.
    pub fn allows_call(&self, tool_name: &str, arguments: &Value) -> Result<(), String> {
        let Some(tool_constraints) = self.call.get(tool_name) else {
            return Ok(());
        };
        let args_obj = arguments.as_object();
        for (arg_name, constraint) in tool_constraints {
            let arg_value = args_obj.and_then(|o| o.get(arg_name));
            constraint.check(arg_name, arg_value)?;
        }
        Ok(())
    }
}

fn parse_patterns(raw: Option<&str>) -> Vec<Pattern> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split(',').filter_map(Pattern::parse).collect()
}

fn parse_constraints(raw: Option<&str>) -> HashMap<String, HashMap<String, ArgConstraint>> {
    let Some(raw) = raw else {
        return HashMap::new();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return HashMap::new();
    }
    match serde_json::from_str(trimmed) {
        Ok(parsed) => parsed,
        Err(e) => {
            log::warn!("[mcp] ignoring malformed tool-call constraint spec: {}", e);
            HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ─── Name filter ───────────────────────────────────────────────────────

    #[test]
    fn empty_filter_allows_everything() {
        let f = ClientFilters::allow_all();
        assert!(f.allows_name("anything"));
        assert!(f.allows_name("command:DeleteEverything"));
    }

    #[test]
    fn star_allows_everything() {
        let f = ClientFilters::from_strings(Some("*"), None, None);
        assert!(f.allows_name("query:GetAllTargets"));
    }

    #[test]
    fn prefix_pattern() {
        let f = ClientFilters::from_strings(Some("query:*"), None, None);
        assert!(f.allows_name("query:GetAllTargets"));
        assert!(!f.allows_name("command:DoStuff"));
    }

    #[test]
    fn suffix_pattern() {
        let f = ClientFilters::from_strings(Some("*Internal"), None, None);
        assert!(f.allows_name("query:GetThingInternal"));
        assert!(!f.allows_name("query:GetThing"));
    }

    #[test]
    fn deny_wins_on_name_conflict() {
        let f = ClientFilters::from_strings(Some("query:*"), Some("query:GetSecret"), None);
        assert!(f.allows_name("query:GetAllTargets"));
        assert!(!f.allows_name("query:GetSecret"));
    }

    #[test]
    fn empty_allow_with_deny_means_allow_all_minus_denied() {
        let f = ClientFilters::from_strings(None, Some("command:Delete*"), None);
        assert!(f.allows_name("query:GetAllTargets"));
        assert!(!f.allows_name("command:DeleteThing"));
    }

    #[test]
    fn comma_separated_allow_list() {
        let f = ClientFilters::from_strings(Some("query:*,report:HealthCheck"), None, None);
        assert!(f.allows_name("query:Anything"));
        assert!(f.allows_name("report:HealthCheck"));
        assert!(!f.allows_name("report:OtherReport"));
        assert!(!f.allows_name("command:DoStuff"));
    }

    #[test]
    fn whitespace_around_patterns_is_stripped() {
        let f = ClientFilters::from_strings(Some(" query:* , report:H "), None, None);
        assert!(f.allows_name("query:GetAll"));
        assert!(f.allows_name("report:H"));
    }

    #[test]
    fn exact_match() {
        let f = ClientFilters::from_strings(Some("query:GetAllTargets"), None, None);
        assert!(f.allows_name("query:GetAllTargets"));
        assert!(!f.allows_name("query:GetAllTargetsExtra"));
    }

    // ─── Call filter ───────────────────────────────────────────────────────

    fn run_playbook_constraint() -> &'static str {
        r#"{"command:RunPlaybook":{"playbook_id":{"allow":["site","deploy"]}}}"#
    }

    #[test]
    fn no_constraints_for_tool_passes() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        assert!(
            f.allows_call("command:Other", &json!({"anything": "goes"}))
                .is_ok()
        );
    }

    #[test]
    fn allow_list_passes_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        assert!(
            f.allows_call("command:RunPlaybook", &json!({"playbook_id": "site"}))
                .is_ok()
        );
    }

    #[test]
    fn allow_list_rejects_non_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        let err = f
            .allows_call("command:RunPlaybook", &json!({"playbook_id": "danger"}))
            .unwrap_err();
        assert!(err.contains("playbook_id"));
        assert!(err.contains("allowlist"));
    }

    #[test]
    fn allow_list_rejects_missing_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        let err = f
            .allows_call("command:RunPlaybook", &json!({}))
            .unwrap_err();
        assert!(err.contains("required"));
    }

    #[test]
    fn deny_list_rejects_matching_arg() {
        let f = ClientFilters::from_strings(
            None,
            None,
            Some(r#"{"command:Tag":{"namespace":{"deny":["prod"]}}}"#),
        );
        assert!(
            f.allows_call("command:Tag", &json!({"namespace": "staging"}))
                .is_ok()
        );
        let err = f
            .allows_call("command:Tag", &json!({"namespace": "prod"}))
            .unwrap_err();
        assert!(err.contains("namespace"));
    }

    #[test]
    fn deny_wins_when_both_allow_and_deny_listed() {
        let f = ClientFilters::from_strings(
            None,
            None,
            Some(r#"{"command:X":{"a":{"allow":["1","2"],"deny":["2"]}}}"#),
        );
        assert!(f.allows_call("command:X", &json!({"a": "1"})).is_ok());
        assert!(f.allows_call("command:X", &json!({"a": "2"})).is_err());
    }

    #[test]
    fn malformed_constraint_json_is_ignored() {
        // Better to be permissive than to brick a request on bad filter config.
        let f = ClientFilters::from_strings(None, None, Some("not json"));
        assert!(f.allows_call("any:tool", &json!({})).is_ok());
    }
}
