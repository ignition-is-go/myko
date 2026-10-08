use myko::prelude::*;
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
fn wire_serialization_uses_the_concrete_item_when_names_collide() {
    let media = media::SameNameStream {
        id: "media".into(),
        encoder: "h264".into(),
    };
    let control = control::SameNameStream {
        id: "control".into(),
        target: "lights".into(),
    };
    let values = [
        (
            Arc::new(media.clone()) as Arc<dyn AnyItem>,
            serde_json::to_value(media).unwrap(),
        ),
        (
            Arc::new(control.clone()) as Arc<dyn AnyItem>,
            serde_json::to_value(control).unwrap(),
        ),
    ];
    for (item, expected) in values {
        let wire = ErasedWrappedItem {
            item,
            item_type: "SameNameStream".into(),
        };
        let encoded = serde_json::to_value(wire).unwrap();
        assert_eq!(encoded["item"], expected);
        assert_eq!(encoded["itemType"], "SameNameStream");
    }
}
