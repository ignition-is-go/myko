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
//! 2. **Tool callability** — per-tool, per-argument value allow/deny lists.
//!    A failure surfaces as an MCP **Tool Execution Error** (`isError: true`
//!    content with a descriptive message), the spec's "Invalid input data"
//!    category — distinct from a Protocol Error. Source:
//!    - HTTP/WS: `X-Myko-Tool-Callable-Allow` and
//!      `X-Myko-Tool-Callable-Deny` request headers (JSON).
//!    - Stdio: `MYKO_MCP_TOOL_CALLABLE_ALLOW` /
//!      `MYKO_MCP_TOOL_CALLABLE_DENY` env vars (JSON).
//!
//! [mcp-tool-errors]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools#error-handling
//!
//! ### Callability JSON shape
//!
//! Each callable header carries one JSON object: `tool -> arg -> [values]`.
//! Polarity comes from which header the JSON is in.
//!
//! ```json
//! // X-Myko-Tool-Callable-Allow
//! {
//!   "command_RunPlaybook": { "playbook_id": ["site", "deploy"] }
//! }
//!
//! // X-Myko-Tool-Callable-Deny
//! {
//!   "command_Tag":         { "namespace":   ["prod"] }
//! }
//! ```
//!
//! Legacy `command:RunPlaybook` / `query:*` syntax in configs is accepted
//! transparently — see [`normalize_tool_name`]. New configs should use the
//! `_` form, which matches the `OpenAI` tool-name regex `[a-zA-Z0-9_-]+` and
//! avoids tool-call-serializer bugs in some LLMs.
//!
//! Semantics, per tool/arg:
//! - **Allow** is positive: the arg must be present on the call and its
//!   value must appear in the list.
//! - **Deny** excludes: if the arg is present and its value appears in the
//!   list, the call is rejected.
//! - Deny wins. If the same tool/arg/value appears on both sides, deny.

use std::collections::HashMap;

use serde_json::Value;

// ─── Header / env names ────────────────────────────────────────────────────

/// HTTP header carrying the tool-visibility allowlist (glob patterns).
pub const VISIBILITY_ALLOW_HEADER: &str = "X-Myko-Tool-Visibility-Allow";
/// HTTP header carrying the tool-visibility denylist (glob patterns).
pub const VISIBILITY_DENY_HEADER: &str = "X-Myko-Tool-Visibility-Deny";
/// HTTP header carrying the JSON tool-callable allowlist.
pub const CALLABLE_ALLOW_HEADER: &str = "X-Myko-Tool-Callable-Allow";
/// HTTP header carrying the JSON tool-callable denylist.
pub const CALLABLE_DENY_HEADER: &str = "X-Myko-Tool-Callable-Deny";

/// Stdio env var carrying the tool-visibility allowlist.
pub const VISIBILITY_ALLOW_ENV: &str = "MYKO_MCP_TOOL_VISIBILITY_ALLOW";
/// Stdio env var carrying the tool-visibility denylist.
pub const VISIBILITY_DENY_ENV: &str = "MYKO_MCP_TOOL_VISIBILITY_DENY";
/// Stdio env var carrying the JSON tool-callable allowlist.
pub const CALLABLE_ALLOW_ENV: &str = "MYKO_MCP_TOOL_CALLABLE_ALLOW";
/// Stdio env var carrying the JSON tool-callable denylist.
pub const CALLABLE_DENY_ENV: &str = "MYKO_MCP_TOOL_CALLABLE_DENY";

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
    ///
    /// The pattern is normalized: a leading `kind:` separator becomes
    /// `kind_` so legacy configs (e.g. `command:Run*`) keep matching after
    /// myko's MCP wire switched to the underscore form. See
    /// [`normalize_tool_name`].
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let s = normalize_tool_name(s.trim());
        if s.is_empty() {
            return None;
        }
        if s == "*" {
            return Some(Self::Any);
        }
        match (s.starts_with('*'), s.ends_with('*')) {
            (true, true) if s.len() == 2 => Some(Self::Any),
            (false, true) => s
                .get(..s.len().saturating_sub(1))
                .map(|value| Self::Prefix(value.to_string())),
            (true, false) => s.get(1..).map(|value| Self::Suffix(value.to_string())),
            _ => Some(Self::Exact(s)),
        }
    }

    /// Test whether `name` matches this pattern.
    #[must_use]
    pub fn matches(&self, name: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Prefix(p) => name.starts_with(p),
            Self::Suffix(s) => name.ends_with(s),
            Self::Exact(e) => name == e,
        }
    }
}

// ─── Callability map ───────────────────────────────────────────────────────

/// `tool_name -> { arg_name -> [values] }`.
type CallabilityMap = HashMap<String, HashMap<String, Vec<Value>>>;

// ─── ClientFilters ─────────────────────────────────────────────────────────

/// Per-client filter combining tool-visibility (name-level) and
/// tool-callability (argument-level) rules. Driven by request headers
/// (HTTP/WS) or environment variables (stdio).
#[derive(Debug, Clone, Default)]
pub struct ClientFilters {
    /// Glob patterns the tool name must match. Empty = visibility unrestricted.
    visibility_allow: Vec<Pattern>,
    /// Glob patterns that hide a tool. Deny wins.
    visibility_deny: Vec<Pattern>,
    /// Tools/args whose values must appear in the listed values to be
    /// callable. Empty = no positive callability constraint.
    callable_allow: CallabilityMap,
    /// Tools/args whose values must *not* appear in the listed values to be
    /// callable. Deny wins.
    callable_deny: CallabilityMap,
}

impl ClientFilters {
    /// A filter that permits everything (no headers / no env vars set).
    #[must_use]
    pub fn allow_all() -> Self {
        Self::default()
    }

    /// Build from raw strings. Callability inputs are JSON; malformed JSON
    /// is treated as no constraints (logged at WARN) — bricking a request
    /// on bad filter config would be a footgun for ops.
    #[must_use]
    pub fn from_strings(
        visibility_allow: Option<&str>,
        visibility_deny: Option<&str>,
        callable_allow_json: Option<&str>,
        callable_deny_json: Option<&str>,
    ) -> Self {
        Self {
            visibility_allow: parse_patterns(visibility_allow),
            visibility_deny: parse_patterns(visibility_deny),
            callable_allow: parse_callability(callable_allow_json, "callable-allow"),
            callable_deny: parse_callability(callable_deny_json, "callable-deny"),
        }
    }

    /// `true` if the tool name is visible to this client.
    ///
    /// A `false` return means a `tools/call` against this name produces an
    /// MCP **Protocol Error** (`-32602`, "Unknown tool: …") and the tool is
    /// omitted from `tools/list` / `resources/list`. Deny wins; an empty
    /// allow list means "visible unless explicitly denied".
    #[must_use]
    pub fn tool_visible(&self, name: &str) -> bool {
        // Normalize at the boundary so legacy `kind:Id` and canonical
        // `kind_Id` produce the same visibility decision.
        let name = normalize_tool_name(name);
        let name = name.as_str();
        if self.visibility_deny.iter().any(|p| p.matches(name)) {
            return false;
        }
        if self.visibility_allow.is_empty() {
            return true;
        }
        self.visibility_allow.iter().any(|p| p.matches(name))
    }

    /// `true` if the top-level `search`/`execute`/`connection_status` tools
    /// are visible to this client — unlike [`tool_visible`](Self::tool_visible),
    /// only `deny` applies; a non-empty `allow` list does *not* hide these.
    ///
    /// These three are entry points, not operations: an operator's allow
    /// list is written in terms of operation names (`report:Foo`,
    /// `command:Bar`) to scope *which operations* a client can reach — that
    /// scoping is enforced separately, per call, inside `search`'s index
    /// filtering and `execute`'s sandbox (both still call `tool_visible` on
    /// the operation's own `{kind}_{id}` name). If `search`/`execute`
    /// themselves were gated by the same allow list, an operation-scoped
    /// allow list with no explicit `search`/`execute` entry would hide both
    /// tools entirely — making the server unreachable despite the
    /// operator's intent being to scope operations, not remove entry
    /// points. Explicit `deny` still works normally for operators who want
    /// to lock a client out of a tool entirely.
    #[must_use]
    pub fn meta_tool_visible(&self, name: &str) -> bool {
        let name = normalize_tool_name(name);
        !self.visibility_deny.iter().any(|p| p.matches(&name))
    }

    /// Check whether a `tools/call` is callable for this client given its
    /// JSON `arguments`.
    ///
    /// `Ok(())` if no callability constraints apply or every constraint
    /// passes. `Err(message)` surfaces as an MCP **Tool Execution Error**
    /// (`isError: true` content with the message), the spec's
    /// "Invalid input data" category.
    ///
    /// Visibility is *not* re-checked here; callers run
    /// [`tool_visible`](Self::tool_visible) first.
    /// # Errors
    ///
    /// Returns an error when the tool or its arguments are denied by the filter.
    pub fn tool_callable(&self, tool_name: &str, arguments: &Value) -> Result<(), String> {
        // Normalize at the boundary so legacy `kind:Id` configs match the
        // canonical `kind_Id` we use as the map key.
        let tool_name = normalize_tool_name(tool_name);
        let tool_name = tool_name.as_str();
        let args_obj = arguments.as_object();

        // Deny wins. Reject the call if any arg's value appears in the deny
        // list for this tool.
        if let Some(deny_args) = self.callable_deny.get(tool_name) {
            for (arg_name, denied_values) in deny_args {
                let Some(value) = args_obj.and_then(|o| o.get(arg_name)) else {
                    continue;
                };
                if denied_values.contains(value) {
                    return Err(format!("argument `{arg_name}` value not allowed"));
                }
            }
        }

        // Positive allow: if a tool/arg appears in the allow map, the call
        // must supply that arg and its value must appear in the list.
        if let Some(allow_args) = self.callable_allow.get(tool_name) {
            for (arg_name, allowed_values) in allow_args {
                let value = args_obj.and_then(|o| o.get(arg_name));
                match value {
                    Some(v) if allowed_values.contains(v) => {}
                    Some(_) => {
                        return Err(format!("argument `{arg_name}` value not in allowlist"));
                    }
                    None => {
                        return Err(format!("argument `{arg_name}` is required by filter"));
                    }
                }
            }
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

fn parse_callability(raw: Option<&str>, label: &str) -> CallabilityMap {
    let Some(raw) = raw else {
        return CallabilityMap::new();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return CallabilityMap::new();
    }
    match serde_json::from_str::<CallabilityMap>(trimmed) {
        Ok(parsed) => parsed
            .into_iter()
            .map(|(k, v)| (normalize_tool_name(&k), v))
            .collect(),
        Err(e) => {
            tracing::warn!("[mcp] ignoring malformed tool-{} spec: {}", label, e);
            CallabilityMap::new()
        }
    }
}

/// Convert a leading `kind:` separator to `kind_`. Entity ids never contain
/// `:` (`PascalCase` from `#[myko_item]`), so the first colon is unambiguous.
///
/// Lets legacy callers continue using configs / patterns / tool-call names
/// written as `command:RunPlaybook` while the MCP wire advertises the
/// underscore form `command_RunPlaybook` (required because some LLM
/// tool-call serializers — confirmed against gpt-oss-20b on 2026-06-02 —
/// drop the `arguments` field when names contain `:`). The underscore form
/// also matches the `OpenAI` tool-name regex `[a-zA-Z0-9_-]+`.
fn normalize_tool_name(name: &str) -> String {
    name.find(':').map_or_else(
        || name.to_string(),
        |pos| {
            let mut out = String::with_capacity(name.len());
            out.push_str(name.get(..pos).unwrap_or_default());
            out.push('_');
            out.push_str(name.get(pos.saturating_add(1)..).unwrap_or_default());
            out
        },
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ─── Visibility ────────────────────────────────────────────────────────

    #[test]
    fn empty_filter_allows_everything() {
        let f = ClientFilters::allow_all();
        assert!(f.tool_visible("anything"));
        assert!(f.tool_visible("command:DeleteEverything"));
    }

    #[test]
    fn star_allows_everything() {
        let f = ClientFilters::from_strings(Some("*"), None, None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
    }

    #[test]
    fn prefix_pattern() {
        let f = ClientFilters::from_strings(Some("query:*"), None, None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("command:DoStuff"));
    }

    #[test]
    fn suffix_pattern() {
        let f = ClientFilters::from_strings(Some("*Internal"), None, None, None);
        assert!(f.tool_visible("query:GetThingInternal"));
        assert!(!f.tool_visible("query:GetThing"));
    }

    #[test]
    fn deny_wins_on_name_conflict() {
        let f = ClientFilters::from_strings(Some("query:*"), Some("query:GetSecret"), None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("query:GetSecret"));
    }

    #[test]
    fn empty_allow_with_deny_means_allow_all_minus_denied() {
        let f = ClientFilters::from_strings(None, Some("command:Delete*"), None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("command:DeleteThing"));
    }

    #[test]
    fn comma_separated_allow_list() {
        let f = ClientFilters::from_strings(Some("query:*,report:HealthCheck"), None, None, None);
        assert!(f.tool_visible("query:Anything"));
        assert!(f.tool_visible("report:HealthCheck"));
        assert!(!f.tool_visible("report:OtherReport"));
        assert!(!f.tool_visible("command:DoStuff"));
    }

    #[test]
    fn whitespace_around_patterns_is_stripped() {
        let f = ClientFilters::from_strings(Some(" query:* , report:H "), None, None, None);
        assert!(f.tool_visible("query:GetAll"));
        assert!(f.tool_visible("report:H"));
    }

    #[test]
    fn exact_match() {
        let f = ClientFilters::from_strings(Some("query:GetAllTargets"), None, None, None);
        assert!(f.tool_visible("query:GetAllTargets"));
        assert!(!f.tool_visible("query:GetAllTargetsExtra"));
    }

    // ─── Callability ───────────────────────────────────────────────────────

    fn run_playbook_allow() -> &'static str {
        r#"{"command:RunPlaybook":{"playbook_id":["site","deploy"]}}"#
    }

    #[test]
    fn no_callability_rules_passes() {
        let f = ClientFilters::allow_all();
        assert!(f.tool_callable("any:tool", &json!({"x": 1})).is_ok());
    }

    #[test]
    fn allow_list_passes_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_allow()), None);
        assert!(
            f.tool_callable("command:RunPlaybook", &json!({"playbook_id": "site"}))
                .is_ok()
        );
    }

    #[test]
    fn allow_list_rejects_non_matching_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_allow()), None);
        let result = f.tool_callable("command:RunPlaybook", &json!({"playbook_id": "danger"}));
        assert!(result.is_err());
        let Err(err) = result else { return };
        assert!(err.contains("playbook_id"));
        assert!(err.contains("allowlist"));
    }

    #[test]
    fn allow_list_rejects_missing_arg() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_allow()), None);
        let result = f.tool_callable("command:RunPlaybook", &json!({}));
        assert!(result.is_err());
        let Err(err) = result else { return };
        assert!(err.contains("required"));
    }

    #[test]
    fn deny_list_rejects_matching_arg() {
        let f = ClientFilters::from_strings(
            None,
            None,
            None,
            Some(r#"{"command:Tag":{"namespace":["prod"]}}"#),
        );
        assert!(
            f.tool_callable("command:Tag", &json!({"namespace": "staging"}))
                .is_ok()
        );
        let result = f.tool_callable("command:Tag", &json!({"namespace": "prod"}));
        assert!(result.is_err());
        let Err(err) = result else { return };
        assert!(err.contains("namespace"));
    }

    #[test]
    fn deny_wins_when_both_allow_and_deny_listed() {
        let f = ClientFilters::from_strings(
            None,
            None,
            Some(r#"{"command:X":{"a":["1","2"]}}"#),
            Some(r#"{"command:X":{"a":["2"]}}"#),
        );
        assert!(f.tool_callable("command:X", &json!({"a": "1"})).is_ok());
        assert!(f.tool_callable("command:X", &json!({"a": "2"})).is_err());
    }

    #[test]
    fn unrelated_tools_pass_through() {
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_allow()), None);
        assert!(
            f.tool_callable("command:Other", &json!({"anything": "goes"}))
                .is_ok()
        );
    }

    #[test]
    fn malformed_callability_json_is_ignored() {
        let f = ClientFilters::from_strings(None, None, Some("not json"), Some("not json"));
        assert!(f.tool_callable("any:tool", &json!({})).is_ok());
    }

    // ─── Separator normalization (`:` legacy ↔ `_` canonical) ──────────────

    #[test]
    fn underscore_form_is_accepted_for_visibility() {
        // Configs in either form should produce equivalent decisions.
        let f = ClientFilters::from_strings(Some("query_*"), None, None, None);
        assert!(f.tool_visible("query_GetAllTargets"));
        assert!(f.tool_visible("query:GetAllTargets")); // legacy form still matches
        assert!(!f.tool_visible("command_DoStuff"));
    }

    #[test]
    fn colon_pattern_matches_underscore_name() {
        // Operator wrote `query:*` in their config; wire now advertises
        // `query_GetAllTargets`. Normalization makes this transparent.
        let f = ClientFilters::from_strings(Some("query:*"), None, None, None);
        assert!(f.tool_visible("query_GetAllTargets"));
    }

    #[test]
    fn callability_map_normalizes_keys() {
        // Config uses legacy `command:RunPlaybook`; runtime calls
        // `command_RunPlaybook`. Both should resolve to the same row.
        let f = ClientFilters::from_strings(None, None, Some(run_playbook_allow()), None);
        assert!(
            f.tool_callable("command_RunPlaybook", &json!({"playbook_id": "site"}))
                .is_ok()
        );
        let result = f.tool_callable("command_RunPlaybook", &json!({"playbook_id": "danger"}));
        assert!(result.is_err());
        let Err(err) = result else { return };
        assert!(err.contains("allowlist"));
    }

    #[test]
    fn normalize_tool_name_idempotent_on_underscore_form() {
        assert_eq!(normalize_tool_name("command_X"), "command_X");
        assert_eq!(normalize_tool_name("command:X"), "command_X");
        assert_eq!(normalize_tool_name("plain"), "plain");
        // Only the first separator is replaced; ids never contain `:` anyway.
        assert_eq!(normalize_tool_name("a:b:c"), "a_b:c");
    }

    // ─── meta_tool_visible ─────────────────────────────────────────────────

    #[test]
    fn meta_tool_visible_ignores_op_level_allow_list() {
        // Reproduces the pulse-ctx upgrade scenario: an operation-scoped
        // allow list (written for the old one-tool-per-operation wire
        // shape) must not hide search/execute/connection_status, which
        // carry no capability of their own — capability is enforced
        // per-operation inside search/execute instead.
        let f = ClientFilters::from_strings(
            Some("report:GetContextRecordsByAttribute,command:WriteContextRecord"),
            None,
            None,
            None,
        );
        assert!(f.meta_tool_visible("search"));
        assert!(f.meta_tool_visible("execute"));
        assert!(f.meta_tool_visible("connection_status"));
        // The op-level allow list still fully applies to `tool_visible`,
        // which is what search/execute use internally per operation.
        assert!(f.tool_visible("report_GetContextRecordsByAttribute"));
        assert!(!f.tool_visible("command_DeleteEverything"));
    }

    #[test]
    fn meta_tool_visible_still_respects_explicit_deny() {
        let f = ClientFilters::from_strings(None, Some("execute"), None, None);
        assert!(!f.meta_tool_visible("execute"));
        assert!(f.meta_tool_visible("search"));
        assert!(f.meta_tool_visible("connection_status"));
    }

    #[test]
    fn meta_tool_visible_allows_everything_by_default() {
        let f = ClientFilters::allow_all();
        assert!(f.meta_tool_visible("search"));
        assert!(f.meta_tool_visible("execute"));
        assert!(f.meta_tool_visible("connection_status"));
    }
}
