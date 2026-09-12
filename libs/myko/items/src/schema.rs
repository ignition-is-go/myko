//! Generated serialized shapes, not execution eligibility or rollout approval.
//!
//! The `schema` feature makes the item macros derive structural schemas for
//! items, IDs, subtypes, command inputs, and generated query inputs. Custom
//! serialization needs a matching schema implementation or Schemars attribute.
//! These schemas cannot prove application semantics or that an older writer
//! preserves newer fields.
//!
//! Enable the composing crate's `schema` feature (`myko` or `myko-node`) so its
//! macro and framework dependencies also enable schema support. Enabling only
//! this leaf crate's feature does not configure those upstream dependencies.
//! No runtime route may treat a missing schema as compatible.

use schemars::{JsonSchema, Schema, generate::SchemaSettings};

use crate::{ItemScope, MykoItem, ServiceTypeId};

/// Separate contracts for emitted JSON and accepted JSON, including nested types.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeSchema {
    pub serialization: Schema,
    pub deserialization: Schema,
}

impl TypeSchema {
    #[must_use]
    pub fn of<T: JsonSchema>() -> Self {
        Self {
            serialization: SchemaSettings::draft2020_12()
                .for_serialize()
                .into_generator()
                .into_root_schema_for::<T>(),
            deserialization: SchemaSettings::draft2020_12()
                .for_deserialize()
                .into_generator()
                .into_root_schema_for::<T>(),
        }
    }
}

/// An item's declared identity and placement alongside its generated payload schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemSchema {
    pub service_id: ServiceTypeId,
    pub item_type: &'static str,
    pub declared_version: u32,
    pub scope: ItemScope,
    pub value: TypeSchema,
}

impl ItemSchema {
    #[must_use]
    pub fn of<T: MykoItem + JsonSchema>() -> Self {
        Self {
            service_id: T::SERVICE_ID,
            item_type: T::ITEM_TYPE,
            declared_version: T::SCHEMA_VERSION,
            scope: T::SCOPE,
            value: TypeSchema::of::<T>(),
        }
    }
}

/// Typed values inside framework-owned result envelopes.
#[derive(Debug, Clone, PartialEq)]
pub enum HandlerResultSchema {
    Value(TypeSchema),
    /// One row's value, excluding framework row keys, ordering, and revisions.
    Rows(TypeSchema),
}

/// Declared handler payload shapes. Does not establish execution eligibility.
#[derive(Debug, Clone, PartialEq)]
pub struct HandlerPayloadSchema {
    pub arguments: TypeSchema,
    pub result: HandlerResultSchema,
}

impl HandlerPayloadSchema {
    #[must_use]
    pub fn value<A: JsonSchema, O: JsonSchema>() -> Self {
        Self {
            arguments: TypeSchema::of::<A>(),
            result: HandlerResultSchema::Value(TypeSchema::of::<O>()),
        }
    }

    #[must_use]
    pub fn rows<A: JsonSchema, R: JsonSchema>() -> Self {
        Self {
            arguments: TypeSchema::of::<A>(),
            result: HandlerResultSchema::Rows(TypeSchema::of::<R>()),
        }
    }
}
