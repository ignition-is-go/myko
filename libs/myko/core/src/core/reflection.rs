//! Shared reflection metadata captured at macro-expansion time and embedded
//! directly in query/view/report/command registrations.
//!
//! This is deliberately *not* derived from ts-rs `.ts` binding files at
//! runtime: those are generated per-crate (each crate's `#[derive(TS)]`
//! writes to its own `bindings/` directory), so a single runtime bindings-dir
//! path can't resolve arguments for types defined in a different crate than
//! the one hosting the MCP server — and it depends on codegen having already
//! run. Capturing field names/types/optionality in the macro itself, where
//! the full field list is already available, means every crate's operations
//! carry this data automatically and it can never go stale relative to the
//! compiled binary.

/// One field of a query/view/report/command's argument struct, captured by
/// the defining macro directly from the struct's own field list.
#[derive(Debug, Clone, Copy)]
pub struct OperationArgField {
    pub name: &'static str,
    /// Rust type as written on the field (e.g. `"Option<String>"`).
    pub rust_type: &'static str,
    /// `true` if the field's type is `Option<...>`.
    pub optional: bool,
}
