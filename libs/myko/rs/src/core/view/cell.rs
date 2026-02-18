use std::sync::Arc;

use hypha::{CellImmutable, CellMap};

use crate::core::item::AnyItem;

pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;
