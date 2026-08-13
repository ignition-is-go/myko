//! Backend-agnostic registrations consumed by generated-code renderers.

use std::collections::HashSet;

use crate::{
    command::CommandRegistration, core::item::ItemRegistration, query::QueryRegistration,
    report::ReportRegistration, view::ViewRegistration,
};

/// A literal constant value that renderers can express in their target language.
#[derive(Debug, PartialEq)]
pub enum TypegenConstValue {
    Str(&'static str),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// A constant owned by the crate where the registration macro was invoked.
pub struct TypegenConstRegistration {
    pub name: &'static str,
    pub value: TypegenConstValue,
    pub crate_path: &'static str,
}

inventory::collect!(TypegenConstRegistration);

/// A Rust type selected for generated-language export.
///
/// This describes ownership only. Rendering callbacks belong to
/// backend-specific adapter registrations.
pub struct TypegenTypeRegistration {
    pub id: &'static str,
    pub type_name: &'static str,
    pub crate_path: &'static str,
}

inventory::collect!(TypegenTypeRegistration);

/// Registration for a language-neutral generated typegen module.
pub struct TypegenModuleRegistration {
    /// Stable registration identifier, used in diagnostics.
    pub id: &'static str,
    /// Registering crate/module path, used to select the current typegen crate.
    pub crate_path: &'static str,
    /// Build the module IR from the registering crate's inventories.
    pub build: fn() -> crate::typegen_module::TypegenModule,
}

inventory::collect!(TypegenModuleRegistration);

/// The registrations owned by one crate, ready for any language renderer.
pub struct TypegenCatalog {
    pub types: Vec<&'static TypegenTypeRegistration>,
    pub constants: Vec<&'static TypegenConstRegistration>,
    pub modules: Vec<&'static TypegenModuleRegistration>,
    pub items: Vec<&'static ItemRegistration>,
    pub queries: Vec<&'static QueryRegistration>,
    pub views: Vec<&'static ViewRegistration>,
    pub reports: Vec<&'static ReportRegistration>,
    pub commands: Vec<&'static CommandRegistration>,
}

impl TypegenCatalog {
    #[must_use]
    pub fn collect(crate_name: &str) -> Self {
        Self {
            types: inventory::iter::<TypegenTypeRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_path, crate_name))
                .collect(),
            constants: inventory::iter::<TypegenConstRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_path, crate_name))
                .collect(),
            modules: inventory::iter::<TypegenModuleRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_path, crate_name))
                .collect(),
            items: inventory::iter::<ItemRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_name, crate_name))
                .collect(),
            queries: inventory::iter::<QueryRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_name, crate_name))
                .collect(),
            views: inventory::iter::<ViewRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_name, crate_name))
                .collect(),
            reports: inventory::iter::<ReportRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_name, crate_name))
                .collect(),
            commands: inventory::iter::<CommandRegistration>
                .into_iter()
                .filter(|entry| registration_belongs_to_crate(entry.crate_name, crate_name))
                .collect(),
        }
    }

    #[must_use]
    pub fn type_ids(&self) -> HashSet<&'static str> {
        self.types
            .iter()
            .map(|registration| registration.id)
            .collect()
    }
}

/// Whether a `module_path!()` registration belongs to `crate_name`.
///
/// Cargo package names are normalized from hyphens to underscores by the
/// typegen entry point before this comparison.
#[must_use]
pub fn registration_belongs_to_crate(registration_path: &str, crate_name: &str) -> bool {
    registration_path.split("::").next() == Some(crate_name)
}

#[cfg(test)]
mod tests {
    use super::{TypegenCatalog, TypegenTypeRegistration, registration_belongs_to_crate};

    inventory::submit! {
        TypegenTypeRegistration {
            id: "rship::Own",
            type_name: "Own",
            crate_path: "rship::nested",
        }
    }

    inventory::submit! {
        TypegenTypeRegistration {
            id: "rship_core::Foreign",
            type_name: "Foreign",
            crate_path: "rship_core",
        }
    }

    #[test]
    fn catalog_excludes_a_sibling_crate_with_a_shared_name_prefix() {
        let catalog = TypegenCatalog::collect("rship");
        let ids = catalog.type_ids();

        assert!(ids.contains("rship::Own"));
        assert!(!ids.contains("rship_core::Foreign"));
    }

    #[test]
    fn crate_ownership_compares_the_module_path_root() {
        assert!(registration_belongs_to_crate("rship", "rship"));
        assert!(registration_belongs_to_crate("rship::nested", "rship"));
        assert!(!registration_belongs_to_crate("rship_core", "rship"));
        assert!(!registration_belongs_to_crate("my_rship::nested", "rship"));
        assert!(!registration_belongs_to_crate("rshipper", "rship"));
    }
}
