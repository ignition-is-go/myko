//! Per-client MCP filters, aligned to the two error categories the MCP spec
//! defines for tools ([Tools / Error Handling][mcp-tool-errors]):
//!
//! 1. **Tool visibility** — glob allow/deny over tool names. A hidden tool
//!    disappears from `tools/list` and a `tools/call` against it returns the
//!    MCP **Protocol Error** `{"code": -32602, "message": "Unknown tool: …"}`.
//!    Source:
//!    - HTTP/WS: `X-Myko-Tool-Visibility-Allow` and
//!      `X-Myko-Tool-Visibility-Deny` request headers.
//!    - Stdio: `MYKO_MCP_TOOL_VISIBILITY_ALLOW` /
//!      `MYKO_MCP_TOOL_VISIBILITY_DENY` env vars.
//!
//! 2. **Argument validation** — client-supplied constraints on the JSON
//!    `arguments` of a `tools/call`. A constraint failure surfaces as an MCP
//!    **Tool Execution Error** (`isError: true` content with a descriptive
//!    message), the spec's "Invalid input data" category — distinct from a
//!    Protocol Error. Source:
//!    - HTTP/WS: `X-Myko-Tool-Arguments` request header (JSON).
//!    - Stdio: `MYKO_MCP_TOOL_ARGUMENTS` env var (JSON).
//!
//! [mcp-tool-errors]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools#error-handling
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

/// HTTP header carrying the tool-visibility allowlist (glob patterns).
pub const VISIBILITY_ALLOW_HEADER: &str = "X-Myko-Tool-Visibility-Allow";
/// HTTP header carrying the tool-visibility denylist (glob patterns).
pub const VISIBILITY_DENY_HEADER: &str = "X-Myko-Tool-Visibility-Deny";
/// HTTP header carrying the JSON argument-validation spec.
pub const ARGUMENTS_HEADER: &str = "X-Myko-Tool-Arguments";

/// Stdio env var carrying the tool-visibility allowlist.
pub const VISIBILITY_ALLOW_ENV: &str = "MYKO_MCP_TOOL_VISIBILITY_ALLOW";
/// Stdio env var carrying the tool-visibility denylist.
pub const VISIBILITY_DENY_ENV: &str = "MYKO_MCP_TOOL_VISIBILITY_DENY";
/// Stdio env var carrying the JSON argument-validation spec.
pub const ARGUMENTS_ENV: &str = "MYKO_MCP_TOOL_ARGUMENTS";

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

/// Per-client filter combining tool-visibility rules with per-call argument
/// constraints. Driven by request headers (HTTP/WS) or environment variables
/// (stdio).
#[derive(Debug, Clone, Default)]
pub struct ClientFilters {
    /// Glob patterns the tool name must match. Empty = visibility unrestricted.
    visibility_allow: Vec<Pattern>,
    /// Glob patterns that hide a tool. Deny wins.
    visibility_deny: Vec<Pattern>,
    /// `tool_name -> { arg_name -> ArgConstraint }`. Empty = no argument
    /// validation.
    argument_constraints: HashMap<String, HashMap<String, ArgConstraint>>,
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
        visibility_allow: Option<&str>,
        visibility_deny: Option<&str>,
        constraints_json: Option<&str>,
    ) -> Self {
        Self {
            visibility_allow: parse_patterns(visibility_allow),
            visibility_deny: parse_patterns(visibility_deny),
            argument_constraints: parse_constraints(constraints_json),
        }
    }

    /// `true` if the tool name is visible to this client.
    ///
    /// A `false` return means a `tools/call` against this name produces an
    /// MCP **Protocol Error** (`-32602`, "Unknown tool: …") and the tool is
    /// omitted from `tools/list` / `resources/list`. Deny wins; an empty
    /// allow list means "visible unless explicitly denied".
    pub fn tool_visible(&self, name: &str) -> bool {
        if self.visibility_deny.iter().any(|p| p.matches(name)) {
            return false;
        }
        if self.visibility_allow.is_empty() {
            return true;
        }
        self.visibility_allow.iter().any(|p| p.matches(name))
    }

    /// Validate the JSON `arguments` of a `tools/call`.
    ///
    /// `Ok(())` if no constraints apply or every constraint passes.
    /// `Err(message)` surfaces as an MCP **Tool Execution Error**
    /// (`isError: true` content with the message), the spec's
    /// "Invalid input data" category.
    ///
    /// Visibility is *not* re-checked here; callers run
    /// [`tool_visible`](Self::tool_visible) first.
    pub fn validate_call(&self, tool_name: &str, arguments: &Value) -> Result<(), String> {
        let Some(tool_constraints) = self.argument_constraints.get(tool_name) else {
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
        assert!(f.tool_visible("anything"));
        assert!(f.tool_visible("command:DeleteEverything"));
    }

    #[test]
    fn star_allows_everything() {
        let f = ClientFilters::from_strings(Some("*"), None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
    }

    #[test]
    fn prefix_pattern() {
        let f = ClientFilters::from_strings(Some("query:*"), None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("command:DoStuff"));
    }

    #[test]
    fn suffix_pattern() {
        let f = ClientFilters::from_strings(Some("*Internal"), None, None);
        assert!(f.tool_visible("query:GetThingInternal"));
        assert!(!f.tool_visible("query:GetThing"));
    }

    #[test]
    fn deny_wins_on_name_conflict() {
        let f = ClientFilters::from_strings(Some("query:*"), Some("query:GetSecret"), None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("query:GetSecret"));
    }

    #[test]
    fn empty_allow_with_deny_means_allow_all_minus_denied() {
        let f = ClientFilters::from_strings(None, Some("command:Delete*"), None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("command:DeleteThing"));
    }

    #[test]
    fn comma_separated_allow_list() {
        let f = ClientFilters::from_strings(Some("query:*,report:HealthCheck"), None, None);
        assert!(f.tool_visible("query:Anything"));
        assert!(f.tool_visible("report:HealthCheck"));
        assert!(!f.tool_visible("report:OtherReport"));
        assert!(!f.tool_visible("command:DoStuff"));
    }

    #[test]
    fn whitespace_around_patterns_is_stripped() {
        let f = ClientFilters::from_strings(Some(" query:* , report:H "), None, None);
        assert!(f.tool_visible("query:GetAll"));
        assert!(f.tool_visible("report:H"));
    }

    #[test]
    fn exact_match() {
        let f = ClientFilters::from_strings(Some("query:GetAllTargets"), None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("query:GetAllTargetsExtra"));
    }

    // ─── Call filter ───────────────────────────────────────────────────────

    fn run_playbook_constraint() -> &'static str {
        r#"{"command:RunPlaybook":{"playbook_id":{"allow":["site","deploy"]}}}"#
    }

    #[test]
    fn no_constraints_for_tool_passes() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        assert!(
            f.validate_call("command:Other", &json!({"anything": "goes"}))
                .is_ok()
        );
    }

    #[test]
    fn allow_list_passes_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        assert!(
            f.validate_call("command:RunPlaybook", &json!({"playbook_id": "site"}))
                .is_ok()
        );
    }

    #[test]
    fn allow_list_rejects_non_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        let err = f
            .validate_call("command:RunPlaybook", &json!({"playbook_id": "danger"}))
            .unwrap_err();
        assert!(err.contains("playbook_id"));
        assert!(err.contains("allowlist"));
    }

    #[test]
    fn allow_list_rejects_missing_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_constraint()));
        let err = f
            .validate_call("command:RunPlaybook", &json!({}))
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
            f.validate_call("command:Tag", &json!({"namespace": "staging"}))
                .is_ok()
        );
        let err = f
            .validate_call("command:Tag", &json!({"namespace": "prod"}))
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
        assert!(f.validate_call("command:X", &json!({"a": "1"})).is_ok());
        assert!(f.validate_call("command:X", &json!({"a": "2"})).is_err());
    }

    #[test]
    fn malformed_constraint_json_is_ignored() {
        // Better to be permissive than to brick a request on bad filter config.
        let f = ClientFilters::from_strings(None, None, Some("not json"));
        assert!(f.validate_call("any:tool", &json!({})).is_ok());
    }
}
