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
    String(String),
    Bool(bool),
    Integer(i64),
    Float(f64),
    Reference(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl Value {
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
