//! Entity shapes modelled on rship's `Scene`, `Target`, `Emitter`,
//! `BindingNode`, `Pulse` and `Bundle`. Each `#[myko_item]` lives in its own
//! module because the macro re-imports hyphae traits at module scope.

pub use blob::Blob;
pub use bundle::*;
pub use emitter::*;
pub use gauge::*;
pub use node::*;
pub use part::*;
pub use probe::*;
pub use scene::*;
pub use target::*;

pub mod blob {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use myko::{schemars, serde_json::Value};

    /// The nested JSON value an entity carries. Hand-written `Clone`,
    /// `PartialEq` and `Serialize` count calls while [`COUNTING`] is set, so
    /// an accounting run can attribute per-change cost to cloning, equality
    /// and serialization.
    #[derive(Debug, Default)]
    pub struct Blob(pub Value);

    myko::impl_ts_as!(Blob, "unknown");

    pub static COUNTING: AtomicBool = AtomicBool::new(false);
    pub static CLONES: AtomicU64 = AtomicU64::new(0);
    pub static EQS: AtomicU64 = AtomicU64::new(0);
    pub static SERIALIZES: AtomicU64 = AtomicU64::new(0);

    #[inline]
    fn count(counter: &AtomicU64) {
        if COUNTING.load(Ordering::Relaxed) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Clone for Blob {
        fn clone(&self) -> Self {
            count(&CLONES);
            Self(self.0.clone())
        }
    }

    impl PartialEq for Blob {
        fn eq(&self, other: &Self) -> bool {
            count(&EQS);
            self.0 == other.0
        }
    }

    impl serde::Serialize for Blob {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            count(&SERIALIZES);
            self.0.serialize(serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for Blob {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            Value::deserialize(deserializer).map(Self)
        }
    }

    impl schemars::JsonSchema for Blob {
        fn schema_name() -> std::borrow::Cow<'static, str> {
            "Blob".into()
        }

        fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
            Value::json_schema(generator)
        }
    }

    impl myko::query::Filterable for Blob {
        type Filter = myko::query::Unfilterable;
    }
}

pub mod scene {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::Blob;

    #[myko_item]
    pub struct BScene {
        #[searchable]
        pub name: String,
        pub project_id: Arc<str>,
        pub color: String,
        #[serde(default)]
        pub tags: Vec<Arc<str>>,
        pub armed: bool,
        pub revision: u64,
        pub settings: Blob,
    }
}

pub mod target {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::Blob;

    #[myko_item]
    pub struct BTarget {
        #[searchable]
        pub name: String,
        #[serde(default)]
        pub parent_targets: Vec<BTargetId>,
        #[searchable]
        pub service_id: String,
        #[searchable]
        pub category: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub managed: Option<bool>,
        #[serde(default)]
        pub tags: Vec<Arc<str>>,
        pub revision: u64,
        pub metadata: Blob,
    }
}

pub mod emitter {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::{
        Blob,
        target::{BTarget, BTargetId},
    };

    #[myko_item]
    pub struct BEmitter {
        #[searchable]
        pub name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub category: Option<Arc<str>>,
        #[belongs_to(BTarget)]
        pub target_id: BTargetId,
        pub service_id: String,
        pub revision: u64,
        #[serde(default)]
        pub schema: Option<Blob>,
    }
}

pub mod node {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::{
        Blob,
        scene::{BScene, BSceneId},
    };

    /// The entity the 30 Hz tick rewrites. `tick` and `seq` let the sink
    /// attribute a delivery to the write that caused it.
    #[myko_item]
    pub struct BNode {
        #[belongs_to(BScene)]
        pub scene_id: BSceneId,
        pub node_type: Arc<str>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub display_name: Option<String>,
        pub category: Arc<str>,
        #[serde(default)]
        pub anchors: Vec<Arc<str>>,
        pub enabled: bool,
        pub x: f64,
        pub y: f64,
        pub tick: u64,
        pub seq: u64,
        pub writer: u32,
        pub data: Blob,
    }
}

pub mod probe {
    use std::sync::Arc;

    use myko::prelude::*;

    /// A small row in its own store, written by the independent probe.
    #[myko_item]
    pub struct BProbe {
        pub name: Arc<str>,
        pub seq: u64,
        pub writer: u32,
        pub value: f64,
    }
}

pub mod part {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::Blob;

    #[myko_item]
    pub struct BPart {
        pub name: Arc<str>,
        pub revision: u64,
        pub data: Blob,
    }
}

pub mod bundle {
    use std::sync::Arc;

    use myko::prelude::*;

    #[allow(unused_imports)]
    use super::{
        part::{BPart, BPartId},
        scene::{BScene, BSceneId},
    };

    #[myko_item]
    pub struct BBundle {
        #[belongs_to(BScene)]
        pub scope_id: BSceneId,
        pub name: Arc<str>,
        #[owns_many(BPart)]
        #[serde(default)]
        pub part_ids: Vec<BPartId>,
    }
}

pub mod gauge {
    use myko::prelude::*;

    use super::emitter::{BEmitter, BEmitterId};

    /// Third entity type of the atomic-batch scenario.
    #[myko_item]
    pub struct BGauge {
        #[belongs_to(BEmitter)]
        pub emitter_id: BEmitterId,
        pub revision: u64,
        pub level: f64,
    }
}
