//! Command registration via inventory.

/// Registration for type generation (separate from handler registration).
#[derive(Debug)]
pub struct CommandRegistration {
    pub command_id: &'static str,
    pub result_type: &'static str,
    pub result_type_crate: &'static str,
    pub crate_name: &'static str,
    /// `#[myko_command(.., public)]` — when the server has auth configured, a
    /// `public` command skips per-command token verification (it must be
    /// callable unauthenticated). Default `false` = auth required when enforced.
    pub public: bool,
}

inventory::collect!(CommandRegistration);
