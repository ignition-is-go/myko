//! Query, view and report families, one per params type and request scope.
//!
//! A family's rows are the query, view or report instances keyed by their
//! params: built when a key is first demanded, shared by every reader of that
//! key, and torn down once no reader holds them. The context keeps each family
//! weakly; its readers keep it alive, and a row's own build reaches the context
//! through a strong clone, so the context and its families form no cycle.

use std::{
    any::{Any, TypeId},
    hash::Hash,
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use hyphae::{Cell, CellMutable, CellValue, Family, FamilyNode, FamilyRow, KeyedFamily, Lease, family};

use super::MykoServerContext;
use crate::request::RequestContext;

/// Params that key a family row: report, view and query params all are.
pub trait Params: CellValue + Hash + Eq {}

impl<T: CellValue + Hash + Eq> Params for T {}

/// A report or view type whose instances `ctx.family::<P>()` shares.
pub trait FamilyParams: Params {
    /// A report's output cell or a view's typed map.
    type Row: FamilyRow<Self>;

    fn family(
        ctx: &MykoServerContext,
        request: Arc<RequestContext>,
    ) -> SharedFamily<Self, Self::Row>;
}

/// The family of `P` as `ctx.family::<P>()` returns it.
pub type ParamsFamily<P> = SharedFamily<P, <P as FamilyParams>::Row>;

/// A context's family for one params type and request scope.
pub struct SharedFamily<K, R>(Arc<KeyedFamily<K, R>>);

impl<K, R> Clone for SharedFamily<K, R> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K, R> SharedFamily<K, R>
where
    K: CellValue + Hash + Eq,
{
    /// Whether the row for `key` is built.
    #[must_use]
    pub fn contains(&self, key: &K) -> bool {
        self.0.contains(key)
    }

    /// The number of rows built and not yet torn down.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K, R> Family for SharedFamily<K, R>
where
    K: CellValue + Hash + Eq,
    R: FamilyRow<K>,
{
    type Key = K;
    type Row = R;

    fn acquire(&self, key: &K) -> Lease<R> {
        self.0.acquire(key)
    }

    fn node(&self) -> FamilyNode {
        self.0.node()
    }
}

/// What a family's rows are, for diagnostics. A projection family's rows are
/// typed views of another family's rows and are not counted separately.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FamilyKind {
    Query,
    View,
    Report,
    Projection,
}

trait Slot: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// Rows of a live family, `None` once every reader dropped it.
    fn rows(&self) -> Option<usize>;
}

impl<K, R> Slot for Weak<KeyedFamily<K, R>>
where
    K: CellValue + Hash + Eq,
    R: Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn rows(&self) -> Option<usize> {
        self.upgrade().map(|family| family.len())
    }
}

type Slots = DashMap<(TypeId, String), (FamilyKind, Box<dyn Slot>), ahash::RandomState>;

pub struct Families {
    slots: Slots,
    /// The store registry's root, which every family anchors to: rows read
    /// stores, and every store's changes feed the root.
    root: Cell<(), CellMutable>,
}

impl Families {
    pub fn new(root: Cell<(), CellMutable>) -> Self {
        Self {
            slots: Slots::default(),
            root,
        }
    }

    /// The live family for this row type and `scope`, or a new one whose rows
    /// are `build(key)`.
    pub fn get_or_build<K, R>(
        &self,
        kind: FamilyKind,
        scope: String,
        build: impl Fn(&K) -> R + Send + Sync + 'static,
    ) -> SharedFamily<K, R>
    where
        K: CellValue + Hash + Eq,
        R: FamilyRow<K>,
    {
        let live = |slot: &(FamilyKind, Box<dyn Slot>)| {
            slot.1
                .as_any()
                .downcast_ref::<Weak<KeyedFamily<K, R>>>()
                .and_then(Weak::upgrade)
        };
        let entry = self.slots.entry((TypeId::of::<KeyedFamily<K, R>>(), scope));
        if let dashmap::Entry::Occupied(slot) = &entry
            && let Some(family) = live(slot.get())
        {
            return SharedFamily(family);
        }
        let family = Arc::new(family(build));
        family.anchor(&self.root);
        entry.insert((kind, Box::new(Arc::downgrade(&family))));
        SharedFamily(family)
    }

    /// Rows of every live family of `kind`.
    pub fn rows(&self, kind: FamilyKind) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.value().0 == kind)
            .filter_map(|slot| slot.value().1.rows())
            .sum()
    }

    /// Forget families that every reader dropped.
    pub fn sweep(&self) {
        self.slots.retain(|_, slot| slot.1.rows().is_some());
    }
}
