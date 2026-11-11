//! Command registration via inventory.

/// Registration for type generation (separate from handler registration).
#[derive(Debug)]
pub struct CommandRegistration {
    pub command_id: &'static str,
    pub result_type: &'static str,
    pub result_type_crate: &'static str,
    pub crate_name: &'static str,
    /// Command struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// Command struct's doc comment, if any.
    pub description: Option<&'static str>,
}

inventory::collect!(CommandRegistration);
