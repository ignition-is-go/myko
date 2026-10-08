use myko::prelude::*;
pub use myko::*;
use std::sync::Arc;

mod media {
    use myko::prelude::*;
    #[myko_item]
    pub struct SameNameStream {
        pub encoder: String,
    }
}

mod control {
    use myko::prelude::*;
    #[myko_item]
    pub struct SameNameStream {
        pub target: String,
    }
}

#[test]
fn wire_serialization_uses_the_concrete_item_when_names_collide() -> serde_json::Result<()> {
    let media = media::SameNameStream {
        id: "media".into(),
        encoder: "h264".into(),
    };
    let control = control::SameNameStream {
        id: "control".into(),
        target: "lights".into(),
    };
    let values: [(Arc<dyn AnyItem>, serde_json::Value); 2] = [
        (Arc::new(media.clone()), serde_json::to_value(media)?),
        (Arc::new(control.clone()), serde_json::to_value(control)?),
    ];
    for (item, expected) in values {
        let wire = ErasedWrappedItem {
            item,
            item_type: "SameNameStream".into(),
        };
        let encoded = serde_json::to_value(wire)?;
        assert_eq!(encoded.get("item"), Some(&expected));
        assert_eq!(
            encoded.get("itemType").and_then(serde_json::Value::as_str),
            Some("SameNameStream")
        );
    }
    Ok(())
}
