use std::{any::Any, sync::Arc};

pub trait WithTransaction: Any + Send + Sync + 'static {
    fn tx_id(&self) -> Arc<str>;
}
