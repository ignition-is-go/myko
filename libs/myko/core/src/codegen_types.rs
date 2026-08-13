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

/// Marks a Myko-owned DTO as a dependency of downstream generated bindings.
pub struct FrameworkTypegenRegistration {
    pub type_id: &'static str,
}

inventory::collect!(FrameworkTypegenRegistration);

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
    /// Collect registrations owned by one crate.
    #[must_use]
    pub fn collect(crate_name: &str) -> Self {
        Self::collect_crates([crate_name])
    }

    /// Collect registrations owned by an explicit set of crates.
    ///
    /// Each name is matched against the exact `module_path!()` root; shared
    /// prefixes do not opt a crate in.
    #[must_use]
    pub fn collect_crates<I, S>(crate_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let crate_names = crate_names
            .into_iter()
            .map(|name| name.as_ref().to_owned())
            .collect::<HashSet<_>>();
        Self::collect_matching(|path| {
            path.split("::")
                .next()
                .is_some_and(|name| crate_names.contains(name))
        })
    }

    /// Collect Myko-owned shared DTO types without selecting framework
    /// items, queries, views, reports, commands, constants, or modules.
    ///
    /// Merge this catalog into a downstream aggregate when its generated DTOs
    /// refer to framework support types such as filters or `ClientId`.
    #[must_use]
    pub fn collect_framework_types() -> Self {
        Self {
            types: {
                let framework_ids = inventory::iter::<FrameworkTypegenRegistration>
                    .into_iter()
                    .map(|entry| entry.type_id)
                    .collect::<HashSet<_>>();
                inventory::iter::<TypegenTypeRegistration>
                    .into_iter()
                    .filter(|entry| framework_ids.contains(entry.id))
                    .collect()
            },
            constants: Vec::new(),
            modules: Vec::new(),
            items: Vec::new(),
            queries: Vec::new(),
            views: Vec::new(),
            reports: Vec::new(),
            commands: Vec::new(),
        }
    }

    /// Collect registrations from a delimiter-safe crate family.
    ///
    /// A family `acme_entities` selects that exact crate and crates whose
    /// names begin with `acme_entities_`. It does not select similarly named
    /// crates such as `acme_entities2`.
    #[must_use]
    pub fn collect_crate_family(family: &str) -> Self {
        Self::collect_matching(|path| registration_belongs_to_crate_family(path, family))
    }

    fn collect_matching(selected: impl Fn(&str) -> bool) -> Self {
        Self {
            types: inventory::iter::<TypegenTypeRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
            constants: inventory::iter::<TypegenConstRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
            modules: inventory::iter::<TypegenModuleRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
            items: inventory::iter::<ItemRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_name))
                .collect(),
            queries: inventory::iter::<QueryRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_name))
                .collect(),
            views: inventory::iter::<ViewRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_name))
                .collect(),
            reports: inventory::iter::<ReportRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_name))
                .collect(),
            commands: inventory::iter::<CommandRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_name))
                .collect(),
        }
    }

    /// Merge another catalog, retaining each inventory registration once.
    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        extend_unique(&mut self.types, other.types);
        extend_unique(&mut self.constants, other.constants);
        extend_unique(&mut self.modules, other.modules);
        extend_unique(&mut self.items, other.items);
        extend_unique(&mut self.queries, other.queries);
        extend_unique(&mut self.views, other.views);
        extend_unique(&mut self.reports, other.reports);
        extend_unique(&mut self.commands, other.commands);
        self
    }

    #[must_use]
    pub fn type_ids(&self) -> HashSet<&'static str> {
        self.types
            .iter()
            .map(|registration| registration.id)
            .collect()
    }
}

fn extend_unique<T: 'static>(target: &mut Vec<&'static T>, source: Vec<&'static T>) {
    let mut seen = target
        .iter()
        .map(|entry| std::ptr::from_ref(*entry))
        .collect::<HashSet<_>>();
    target.extend(
        source
            .into_iter()
            .filter(|entry| seen.insert(std::ptr::from_ref(*entry))),
    );
}

/// Whether a `module_path!()` registration belongs to `crate_name`.
///
/// Cargo package names are normalized from hyphens to underscores by the
/// typegen entry point before this comparison.
#[must_use]
pub fn registration_belongs_to_crate(registration_path: &str, crate_name: &str) -> bool {
    registration_path.split("::").next() == Some(crate_name)
}

/// Whether a registration belongs to a delimiter-safe crate family.
#[must_use]
pub fn registration_belongs_to_crate_family(registration_path: &str, family: &str) -> bool {
    registration_path.split("::").next().is_some_and(|root| {
        root == family
            || root
                .strip_prefix(family)
                .is_some_and(|suffix| suffix.starts_with('_'))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        TypegenCatalog, TypegenTypeRegistration, registration_belongs_to_crate,
        registration_belongs_to_crate_family,
    };

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
    fn aggregate_catalog_collects_only_explicit_crates() {
        let catalog = TypegenCatalog::collect_crates(["rship_core", "unregistered_crate"]);
        let ids = catalog.type_ids();

        assert!(!ids.contains("rship::Own"));
        assert!(ids.contains("rship_core::Foreign"));
    }

    #[test]
    fn crate_ownership_compares_the_module_path_root() {
        assert!(registration_belongs_to_crate("rship", "rship"));
        assert!(registration_belongs_to_crate("rship::nested", "rship"));
        assert!(!registration_belongs_to_crate("rship_core", "rship"));
        assert!(!registration_belongs_to_crate("my_rship::nested", "rship"));
        assert!(!registration_belongs_to_crate("rshipper", "rship"));
    }
    #[test]
    fn crate_family_requires_an_underscore_delimiter() {
        assert!(registration_belongs_to_crate_family(
            "rship_entities::nested",
            "rship_entities"
        ));
        assert!(registration_belongs_to_crate_family(
            "rship_entities_nodes::nested",
            "rship_entities"
        ));
        assert!(!registration_belongs_to_crate_family(
            "rship_entities2::nested",
            "rship_entities"
        ));
        assert!(!registration_belongs_to_crate_family(
            "other_rship_entities::nested",
            "rship_entities"
        ));
    }

    #[test]
    fn framework_types_compose_without_framework_operations_or_duplicates() {
        let framework = TypegenCatalog::collect_framework_types();
        let type_names = framework
            .types
            .iter()
            .map(|entry| entry.type_name)
            .collect::<HashSet<_>>();

        assert!(type_names.contains("IdFilter<Arc<str>>"));
        assert!(type_names.contains("StringFilter"));
        assert!(type_names.contains("ClientId"));
        assert_eq!(type_names.len(), 6);
        assert!(framework.items.is_empty());
        assert!(framework.queries.is_empty());
        assert!(framework.commands.is_empty());

        let type_count = framework.types.len();
        let merged = framework.merge(TypegenCatalog::collect_framework_types());
        assert_eq!(merged.types.len(), type_count);
    }
}
