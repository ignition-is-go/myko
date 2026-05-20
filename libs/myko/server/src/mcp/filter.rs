//! Per-client tool filter for the MCP endpoint.
//!
//! Parsed from request headers:
//! - `X-Myko-Tools-Allow: query:*,report:GetSystemHealth`
//! - `X-Myko-Tools-Deny:  command:Delete*,command:Reset*`
//!
//! Patterns are comma-separated globs supporting `*` (universal),
//! `prefix*`, `*suffix`, and exact match. Deny wins on conflict.

/// Header name for the allow list.
pub const ALLOW_HEADER: &str = "X-Myko-Tools-Allow";
/// Header name for the deny list.
pub const DENY_HEADER: &str = "X-Myko-Tools-Deny";

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

/// Allow/deny rules for tool names. Empty `allow` means allow-all.
#[derive(Debug, Clone, Default)]
pub struct ToolFilter {
    allow: Vec<Pattern>,
    deny: Vec<Pattern>,
}

impl ToolFilter {
    /// Build a filter that allows everything (no headers present).
    pub fn allow_all() -> Self {
        Self::default()
    }

    /// Build from raw header strings. Either may be `None` or empty.
    pub fn from_headers(allow: Option<&str>, deny: Option<&str>) -> Self {
        let allow = parse_patterns(allow);
        let deny = parse_patterns(deny);
        Self { allow, deny }
    }

    /// Returns `true` iff the tool name is permitted by this filter.
    ///
    /// Deny always wins. An empty allow list means "allow anything not denied".
    pub fn allows(&self, name: &str) -> bool {
        if self.deny.iter().any(|p| p.matches(name)) {
            return false;
        }
        if self.allow.is_empty() {
            return true;
        }
        self.allow.iter().any(|p| p.matches(name))
    }
}

fn parse_patterns(raw: Option<&str>) -> Vec<Pattern> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split(',').filter_map(Pattern::parse).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_allows_everything() {
        let f = ToolFilter::allow_all();
        assert!(f.allows("anything"));
        assert!(f.allows("command:DeleteEverything"));
    }

    #[test]
    fn star_allows_everything() {
        let f = ToolFilter::from_headers(Some("*"), None);
        assert!(f.allows("query:GetAllTargets"));
    }

    #[test]
    fn prefix_pattern() {
        let f = ToolFilter::from_headers(Some("query:*"), None);
        assert!(f.allows("query:GetAllTargets"));
        assert!(!f.allows("command:DoStuff"));
    }

    #[test]
    fn suffix_pattern() {
        let f = ToolFilter::from_headers(Some("*Internal"), None);
        assert!(f.allows("query:GetThingInternal"));
        assert!(!f.allows("query:GetThing"));
    }

    #[test]
    fn deny_wins_on_conflict() {
        let f = ToolFilter::from_headers(Some("query:*"), Some("query:GetSecret"));
        assert!(f.allows("query:GetAllTargets"));
        assert!(!f.allows("query:GetSecret"));
    }

    #[test]
    fn empty_allow_with_deny_means_allow_all_minus_denied() {
        let f = ToolFilter::from_headers(None, Some("command:Delete*"));
        assert!(f.allows("query:GetAllTargets"));
        assert!(!f.allows("command:DeleteThing"));
    }

    #[test]
    fn comma_separated_allow_list() {
        let f = ToolFilter::from_headers(Some("query:*,report:HealthCheck"), None);
        assert!(f.allows("query:Anything"));
        assert!(f.allows("report:HealthCheck"));
        assert!(!f.allows("report:OtherReport"));
        assert!(!f.allows("command:DoStuff"));
    }

    #[test]
    fn whitespace_around_patterns_is_stripped() {
        let f = ToolFilter::from_headers(Some(" query:* , report:H "), None);
        assert!(f.allows("query:GetAll"));
        assert!(f.allows("report:H"));
    }

    #[test]
    fn exact_match() {
        let f = ToolFilter::from_headers(Some("query:GetAllTargets"), None);
        assert!(f.allows("query:GetAllTargets"));
        assert!(!f.allows("query:GetAllTargetsExtra"));
    }
}
