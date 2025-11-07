use std::sync::Arc;

pub trait WithId {
    fn id(&self) -> Arc<str>;
}
