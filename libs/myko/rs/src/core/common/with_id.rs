use std::{any::Any, sync::Arc};

pub trait WithId: Any + Send + Sync + 'static {
    fn id(&self) -> Arc<str>;
}

pub trait WithTypedId: WithId {
    type Id: Clone + std::fmt::Debug + Eq + std::hash::Hash + Send + Sync + 'static;

    fn typed_id(&self) -> Self::Id;
}
