use std::{any::Any, sync::Arc};

pub trait WithId: Any + Send + Sync + 'static {
    fn id(&self) -> Arc<str>;
}
