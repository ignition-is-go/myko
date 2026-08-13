//! Language-neutral declarations for generated typegen modules.
//!
//! Downstream crates build this small IR from their Rust registrations.  The
//! target-language renderer lives in Myko, so downstream code never has to
//! assemble (or escape) TypeScript source text.

/// A generated typegen module.
#[derive(Clone, Debug, PartialEq)]
pub struct TypegenModule {
    /// Output path, relative to the bindings directory. A missing `.ts`
    /// extension is added by the TypeScript renderer.
    pub path: String,
    /// Declarations, in source order.
    pub declarations: Vec<Declaration>,
    /// Whether index modules should be generated for parent directories.
    pub barrels: bool,
}

impl TypegenModule {
    /// Create an empty module. Parent-directory barrels are enabled by default.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            declarations: Vec::new(),
            barrels: true,
        }
    }

    /// Append a declaration.
    #[must_use]
    pub fn declare(mut self, declaration: Declaration) -> Self {
        self.declarations.push(declaration);
        self
    }

    /// Enable or disable generated parent-directory barrels.
    #[must_use]
    pub const fn with_barrels(mut self, barrels: bool) -> Self {
        self.barrels = barrels;
        self
    }

    /// Append the conventional constants, array, index, and finder for a registry.
    #[must_use]
    pub fn declare_registry(mut self, registry: Registry) -> Self {
        self.declarations.extend(registry.into_declarations());
        self
    }
}

/// One named value in a generated registry.
#[derive(Clone, Debug, PartialEq)]
pub struct RegistryEntry {
    pub name: String,
    pub value: Value,
}

impl RegistryEntry {
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// The common generated registry shape: named constants, an all-values array,
/// a key-field index, and a finder function.
#[derive(Clone, Debug, PartialEq)]
pub struct Registry {
    pub entries: Vec<RegistryEntry>,
    pub value_type: Type,
    pub key_field: String,
    pub all_name: String,
    pub index_name: String,
    pub find_name: String,
    pub parameter: String,
    pub key_type: Type,
}

impl Registry {
    pub fn new(
        value_type: Type,
        key_field: impl Into<String>,
        all_name: impl Into<String>,
        index_name: impl Into<String>,
        find_name: impl Into<String>,
    ) -> Self {
        Self {
            entries: Vec::new(),
            value_type,
            key_field: key_field.into(),
            all_name: all_name.into(),
            index_name: index_name.into(),
            find_name: find_name.into(),
            parameter: "key".into(),
            key_type: Type::String,
        }
    }

    #[must_use]
    pub fn entry(mut self, entry: RegistryEntry) -> Self {
        self.entries.push(entry);
        self
    }

    #[must_use]
    pub fn with_parameter(mut self, parameter: impl Into<String>) -> Self {
        self.parameter = parameter.into();
        self
    }

    #[must_use]
    pub fn with_key_type(mut self, key_type: Type) -> Self {
        self.key_type = key_type;
        self
    }

    fn into_declarations(self) -> Vec<Declaration> {
        let mut declarations = self
            .entries
            .iter()
            .map(|entry| {
                Declaration::constant(
                    entry.name.clone(),
                    self.value_type.clone(),
                    entry.value.clone(),
                )
            })
            .collect::<Vec<_>>();
        declarations.push(Declaration::constant(
            self.all_name.clone(),
            Type::array(self.value_type.clone()),
            Value::Array(
                self.entries
                    .into_iter()
                    .map(|entry| Value::reference(entry.name))
                    .collect(),
            ),
        ));
        declarations.push(Declaration::Index {
            name: self.index_name.clone(),
            source: self.all_name,
            key_field: self.key_field,
            value_type: self.value_type.clone(),
        });
        declarations.push(Declaration::Find {
            name: self.find_name,
            index: self.index_name,
            parameter: self.parameter,
            key_type: self.key_type,
            value_type: self.value_type,
        });
        declarations
    }
}

/// A type in the portable SDK-module IR.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    String,
    Boolean,
    Number,
    Named(String),
    Array(Box<Self>),
    Optional(Box<Self>),
    StringUnion(Vec<String>),
    Object(Vec<Field>),
    Record(Box<Self>, Box<Self>),
}

impl Type {
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named(name.into())
    }

    #[must_use]
    pub fn array(item: Self) -> Self {
        Self::Array(Box::new(item))
    }

    #[must_use]
    pub fn optional(inner: Self) -> Self {
        Self::Optional(Box::new(inner))
    }

    #[must_use]
    pub fn record(key: Self, value: Self) -> Self {
        Self::Record(Box::new(key), Box::new(value))
    }
}

/// A field in an object type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    pub optional: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: Type) -> Self {
        Self {
            name: name.into(),
            ty,
            optional: false,
        }
    }

    #[must_use]
    pub const fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
}

/// A data value or reference used by a declaration.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    String(String),
    Bool(bool),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Reference(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl Value {
    /// Convert any serializable Rust value into the portable module IR.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails or produces an unrepresentable number.
    pub fn from_serializable(value: &impl serde::Serialize) -> serde_json::Result<Self> {
        Self::try_from(serde_json::to_value(value)?)
    }

    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    pub fn reference(name: impl Into<String>) -> Self {
        Self::Reference(name.into())
    }

    pub fn object(entries: impl IntoIterator<Item = (impl Into<String>, Self)>) -> Self {
        Self::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = serde_json::Error;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(value),
            serde_json::Value::String(value) => Self::String(value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Self::Integer(value)
                } else if let Some(value) = value.as_u64() {
                    Self::Unsigned(value)
                } else {
                    Self::Float(value.as_f64().ok_or_else(|| {
                        <serde_json::Error as serde::de::Error>::custom(
                            "JSON number cannot be represented in typegen IR",
                        )
                    })?)
                }
            }
            serde_json::Value::Array(values) => Self::Array(
                values
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            serde_json::Value::Object(entries) => Self::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| Ok((key, Self::try_from(value)?)))
                    .collect::<Result<_, serde_json::Error>>()?,
            ),
        })
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::string(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// A leaf value accepted in a generated predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operand {
    ParameterField(String),
    String(String),
    Bool(bool),
}

/// A portable boolean predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicate {
    Equal(Operand, Operand),
    NotEqual(Operand, Operand),
    And(Vec<Self>),
    Or(Vec<Self>),
}

/// A declaration supported by the SDK renderer.
#[derive(Clone, Debug, PartialEq)]
pub enum Declaration {
    Import {
        names: Vec<String>,
        from: String,
        type_only: bool,
    },
    TypeAlias {
        name: String,
        doc: Option<String>,
        ty: Type,
    },
    Const {
        name: String,
        doc: Option<String>,
        ty: Option<Type>,
        value: Value,
        immutable: bool,
        satisfies: Option<Type>,
    },
    FilteredArray {
        name: String,
        source: String,
        parameter: String,
        predicate: Predicate,
    },
    Index {
        name: String,
        source: String,
        key_field: String,
        value_type: Type,
    },
    Find {
        name: String,
        index: String,
        parameter: String,
        key_type: Type,
        value_type: Type,
    },
    LookupOr {
        name: String,
        index: String,
        parameter: String,
        key_type: Type,
        value_type: Type,
        fallback: Value,
    },
}

impl Declaration {
    /// Import generated values or types from another module.
    pub fn import(
        names: impl IntoIterator<Item = impl Into<String>>,
        from: impl Into<String>,
    ) -> Self {
        Self::Import {
            names: names.into_iter().map(Into::into).collect(),
            from: from.into(),
            type_only: false,
        }
    }

    /// Import generated types from another module.
    pub fn import_type(
        names: impl IntoIterator<Item = impl Into<String>>,
        from: impl Into<String>,
    ) -> Self {
        Self::Import {
            names: names.into_iter().map(Into::into).collect(),
            from: from.into(),
            type_only: true,
        }
    }

    pub fn type_alias(name: impl Into<String>, ty: Type) -> Self {
        Self::TypeAlias {
            name: name.into(),
            doc: None,
            ty,
        }
    }

    pub fn constant(name: impl Into<String>, ty: Type, value: Value) -> Self {
        Self::Const {
            name: name.into(),
            doc: None,
            ty: Some(ty),
            value,
            immutable: false,
            satisfies: None,
        }
    }

    pub fn inferred_constant(name: impl Into<String>, value: Value) -> Self {
        Self::Const {
            name: name.into(),
            doc: None,
            ty: None,
            value,
            immutable: false,
            satisfies: None,
        }
    }

    /// Attach documentation to declarations which support it.
    #[must_use]
    pub fn documented(mut self, doc: impl Into<String>) -> Self {
        match &mut self {
            Self::TypeAlias { doc: target, .. } | Self::Const { doc: target, .. } => {
                *target = Some(doc.into());
            }
            _ => {}
        }
        self
    }

    /// Render a constant with a const assertion.
    #[must_use]
    pub const fn immutable(mut self) -> Self {
        if let Self::Const { immutable, .. } = &mut self {
            *immutable = true;
        }
        self
    }

    /// Render a constant with a target-language `satisfies` check.
    #[must_use]
    pub fn satisfies(mut self, ty: Type) -> Self {
        if let Self::Const { satisfies, .. } = &mut self {
            *satisfies = Some(ty);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::{Declaration, Registry, RegistryEntry, Type, TypegenModule, Value};

    #[derive(Serialize)]
    struct Example<'a> {
        name: &'a str,
        optional: Option<bool>,
        count: u64,
    }

    #[test]
    fn converts_serializable_values_without_redeclaring_their_shape() {
        let value = Value::from_serializable(&Example {
            name: "example",
            optional: None,
            count: u64::MAX,
        })
        .expect("example should serialize");

        assert_eq!(
            value,
            Value::Object(vec![
                ("count".into(), Value::Unsigned(u64::MAX)),
                ("name".into(), Value::String("example".into())),
                ("optional".into(), Value::Null),
            ])
        );
    }

    #[test]
    fn registry_appends_constants_array_index_and_finder() {
        let module = TypegenModule::new("registry").declare_registry(
            Registry::new(
                Type::named("Entry"),
                "key",
                "allEntries",
                "entriesByKey",
                "findEntry",
            )
            .entry(RegistryEntry::new(
                "FIRST",
                Value::object([("key", "first".into())]),
            ))
            .with_parameter("key"),
        );

        assert_eq!(module.declarations.len(), 4);
        assert!(matches!(
            &module.declarations[2],
            Declaration::Index { name, source, .. }
                if name == "entriesByKey" && source == "allEntries"
        ));
        assert!(matches!(
            &module.declarations[3],
            Declaration::Find { name, index, .. }
                if name == "findEntry" && index == "entriesByKey"
        ));
    }
}
