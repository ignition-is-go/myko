use std::{any::Any, sync::Arc};

pub trait WithHash: Any + Send + Sync + 'static {
    fn hash(&self) -> Arc<str>;
}
