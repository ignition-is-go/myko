mod joined;
mod status;
mod task;

pub use joined::*;
pub use status::*;
pub use task::*;

/// Compile-time proof that `#[unfilterable]` keeps a field out of the
/// generated query struct — the escape hatch that lets an entity embed a type
/// whose crate cannot implement `Filterable` for it (orphan rule).
#[cfg(test)]
mod unfilterable_tests {
    use crate::prelude::*;

    /// Deliberately implements neither `Filterable` nor `Ord`: standing in for
    /// a shared contract type owned by another crate. The `f64` is why `Eq` is
    /// not derivable here either, which is typical of real editorial payloads.
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::TS)]
    pub struct OpaquePayload {
        pub note: String,
        pub weight: f64,
    }

    #[myko_item]
    pub struct DemoWithOpaque {
        pub name: String,
        #[unfilterable]
        pub payload: Option<OpaquePayload>,
    }

    #[test]
    fn the_query_keeps_filterable_fields_and_drops_the_opaque_one() {
        // `name` is still queryable...
        let query = DemoWithOpaqueQuery {
            name: Some("a".into()),
            ..Default::default()
        };
        assert!(query.name.is_some());
        // ...and the struct simply has no `payload` field to set, which is
        // what makes the entity compile at all.
    }
}
