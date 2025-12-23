//! Command registration via inventory.

/// Registration for type generation (separate from handler registration).
#[derive(Debug)]
pub struct CommandRegistration {
    pub command_id: &'static str,
    pub result_type: &'static str,
    pub result_type_crate: &'static str,
    pub crate_name: &'static str,
}

inventory::collect!(CommandRegistration);
