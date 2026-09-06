//! View output shapes accepted by the registration boundary.
//!
//! These wrappers describe how a lazy local Hyphae map plan or retained native
//! publication can become a registered view output.

#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;
use std::sync::Arc;

use hyphae::{MapQuery, traits::CellValue};

use super::cell::{FilteredViewCellMap, erase_typed_view_map};
use crate::item::AnyItem;

#[cfg(not(target_arch = "wasm32"))]
use myko_federation::LiveSubscription;

#[doc(hidden)]
pub mod sealed {
    pub trait ViewBuildOutputSealed {}
}

/// A lazy local view plan.
///
/// Use this wrapper for ordinary in-process Hyphae map plans. Myko materializes
/// the plan exactly once at the view registration boundary.
pub struct LocalView<Q> {
    query: Q,
}

impl<Q> LocalView<Q> {
    #[must_use]
    pub const fn new(query: Q) -> Self {
        Self { query }
    }
}

/// A retained publication from a native or durable source-backed view.
///
/// The publication already owns its snapshot/live handoff, cursor, and liveness
/// metadata. Registration erases the item type while preserving that publication
/// stream.
#[cfg(not(target_arch = "wasm32"))]
pub struct RetainedView<T>
where
    T: AnyItem + CellValue,
{
    publication: LiveSubscription<BTreeMap<Arc<str>, Arc<T>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> RetainedView<T>
where
    T: AnyItem + CellValue,
{
    #[must_use]
    pub const fn new(publication: LiveSubscription<BTreeMap<Arc<str>, Arc<T>>>) -> Self {
        Self { publication }
    }
}

/// Materialized view output stored by the registration layer.
#[doc(hidden)]
pub enum RegisteredViewOutput {
    /// Local query/view output materialized as the existing erased Hyphae map.
    LocalMap(FilteredViewCellMap),
    /// Retained native publication with item type erased but metadata retained.
    #[cfg(not(target_arch = "wasm32"))]
    RetainedPublication(LiveSubscription<BTreeMap<Arc<str>, Arc<dyn AnyItem>>>),
}

impl RegisteredViewOutput {
    /// Returns the local erased map when this output is a local view.
    ///
    /// # Errors
    ///
    /// Returns an error for retained publications because converting them to a
    /// raw map would discard their cursor, liveness, and publication sequence.
    pub fn into_local_map(self) -> Result<FilteredViewCellMap, String> {
        match self {
            Self::LocalMap(map) => Ok(map),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(_) => {
                Err("retained view output cannot be converted to a local map".to_owned())
            }
        }
    }
}

/// A view handler output accepted by the registration boundary.
///
/// This trait is intentionally implemented only for explicit wrappers. Returning
/// a raw `MapQuery` would make retained publications indistinguishable from
/// local maps and invite accidental materialization.
pub trait ViewBuildOutput: sealed::ViewBuildOutputSealed {
    type Item: AnyItem + CellValue;

    fn into_registered(self) -> RegisteredViewOutput;
}

impl<Q> sealed::ViewBuildOutputSealed for LocalView<Q> {}

impl<T, Q> ViewBuildOutput for LocalView<Q>
where
    T: AnyItem + CellValue,
    Q: MapQuery<Key = Arc<str>, Value = Arc<T>>,
{
    type Item = T;

    fn into_registered(self) -> RegisteredViewOutput {
        RegisteredViewOutput::LocalMap(erase_typed_view_map(self.query.materialize()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> sealed::ViewBuildOutputSealed for RetainedView<T> where T: AnyItem + CellValue {}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ViewBuildOutput for RetainedView<T>
where
    T: AnyItem + CellValue,
{
    type Item = T;

    fn into_registered(self) -> RegisteredViewOutput {
        RegisteredViewOutput::RetainedPublication(
            self.publication
                .map_value(|rows| rows.iter().map(erase_retained_row::<T>).collect()),
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn erase_retained_row<T>((key, value): (&Arc<str>, &Arc<T>)) -> (Arc<str>, Arc<dyn AnyItem>)
where
    T: AnyItem + CellValue,
{
    let value: Arc<T> = Arc::clone(value);
    let erased: Arc<dyn AnyItem> = value;
    (Arc::clone(key), erased)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    #[cfg(not(target_arch = "wasm32"))]
    use std::time::Duration;

    use hyphae::CellMap;
    #[cfg(not(target_arch = "wasm32"))]
    use myko_federation::LiveSubscriptionState;

    use super::*;
    use crate::common::with_id::WithId;

    #[derive(Clone, Debug, PartialEq, serde::Serialize)]
    struct TestViewItem {
        id: Arc<str>,
    }

    impl WithId for TestViewItem {
        fn id(&self) -> Arc<str> {
            Arc::clone(&self.id)
        }
    }

    impl AnyItem for TestViewItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestViewItem"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other.as_any().downcast_ref::<Self>() == Some(self)
        }
    }

    fn item(id: &str) -> Arc<TestViewItem> {
        Arc::new(TestViewItem { id: id.into() })
    }

    #[cfg(not(target_arch = "wasm32"))]
    type ErasedRetainedRows = BTreeMap<Arc<str>, Arc<dyn AnyItem>>;
    #[cfg(not(target_arch = "wasm32"))]
    type ErasedRetainedState = LiveSubscriptionState<ErasedRetainedRows>;
    #[cfg(not(target_arch = "wasm32"))]
    type ErasedRetainedPublication = myko_federation::LivePublication<ErasedRetainedState>;
    #[cfg(not(target_arch = "wasm32"))]
    type ErasedRetainedStream = myko_federation::LivePublicationStream<ErasedRetainedState>;

    #[test]
    fn local_view_materializes_to_the_existing_erased_map() {
        let map = CellMap::<Arc<str>, Arc<TestViewItem>>::new();
        map.insert("local".into(), item("local"));

        let output = LocalView::new(map.lock()).into_registered();

        assert!(matches!(&output, RegisteredViewOutput::LocalMap(_)));
        let RegisteredViewOutput::LocalMap(erased) = output else {
            return;
        };
        let snapshot = erased.snapshot();
        assert_eq!(
            snapshot
                .iter()
                .map(|(key, _)| key.as_ref())
                .collect::<Vec<_>>(),
            ["local"]
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn retained_view_refuses_lossy_local_map_conversion() {
        let initial_rows = BTreeMap::from([(Arc::<str>::from("retained"), item("retained"))]);
        let (_writer, publication) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(initial_rows),
            through: Some(myko_federation::LogPosition::new(1)),
            liveness: myko_federation::SubscriptionLiveness::Current,
        });

        let result = RetainedView::new(publication)
            .into_registered()
            .into_local_map();

        assert!(result.is_err());
        assert_eq!(
            result.err().as_deref(),
            Some("retained view output cannot be converted to a local map")
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn receive_publication(
        stream: &mut ErasedRetainedStream,
    ) -> Result<ErasedRetainedPublication, String> {
        let started = std::time::Instant::now();
        loop {
            match stream.try_recv() {
                Ok(publication) => return Ok(publication),
                Err(flume::TryRecvError::Empty) if started.elapsed() < Duration::from_secs(1) => {
                    std::thread::yield_now();
                }
                Err(error) => return Err(format!("publication was not delivered: {error}")),
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn retained_view_erases_values_without_losing_the_publication_state() {
        let initial_rows = BTreeMap::from([(Arc::<str>::from("retained"), item("retained"))]);
        let (writer, publication) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(initial_rows),
            through: Some(myko_federation::LogPosition::new(7)),
            liveness: myko_federation::SubscriptionLiveness::Current,
        });

        let output = RetainedView::new(publication).into_registered();
        assert!(matches!(
            &output,
            RegisteredViewOutput::RetainedPublication(_)
        ));
        let RegisteredViewOutput::RetainedPublication(publication) = output else {
            return;
        };
        let mut stream = publication.watch_publications();
        let initial = receive_publication(&mut stream);
        assert!(initial.is_ok());
        let Ok(initial) = initial else {
            return;
        };
        assert_eq!(initial.sequence, 0);
        let initial = initial.state;
        assert_eq!(initial.through, Some(myko_federation::LogPosition::new(7)));
        assert_eq!(
            initial.liveness,
            myko_federation::SubscriptionLiveness::Current
        );
        assert_eq!(
            initial
                .value
                .as_ref()
                .map(|rows| rows.keys().map(AsRef::as_ref).collect::<Vec<_>>()),
            Some(vec!["retained"])
        );

        writer.advance_through(Some(myko_federation::LogPosition::new(8)));
        let advanced = receive_publication(&mut stream);
        assert!(advanced.is_ok());
        let Ok(advanced) = advanced else {
            return;
        };
        assert_eq!(advanced.sequence, 1);
        let advanced = advanced.state;
        assert_eq!(advanced.through, Some(myko_federation::LogPosition::new(8)));
        assert_eq!(
            advanced
                .value
                .as_ref()
                .map(|rows| rows.keys().map(AsRef::as_ref).collect::<Vec<_>>()),
            Some(vec!["retained"])
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn retained_view_preserves_liveness_only_advancement() {
        let initial_rows = BTreeMap::from([(Arc::<str>::from("retained"), item("retained"))]);
        let (writer, publication) = myko_federation::live_subscription(LiveSubscriptionState {
            value: Some(initial_rows),
            through: Some(myko_federation::LogPosition::new(1)),
            liveness: myko_federation::SubscriptionLiveness::Current,
        });

        let output = RetainedView::new(publication).into_registered();
        assert!(matches!(
            &output,
            RegisteredViewOutput::RetainedPublication(_)
        ));
        let RegisteredViewOutput::RetainedPublication(publication) = output else {
            return;
        };
        let mut stream = publication.watch_publications();
        let initial = receive_publication(&mut stream);
        assert!(initial.is_ok());
        let Ok(initial) = initial else {
            return;
        };
        assert_eq!(initial.sequence, 0);

        writer.resynchronizing("source reconnecting");
        let resyncing = receive_publication(&mut stream);
        assert!(resyncing.is_ok());
        let Ok(resyncing) = resyncing else {
            return;
        };
        assert_eq!(resyncing.sequence, 1);
        let resyncing = resyncing.state;
        assert_eq!(
            resyncing.through,
            Some(myko_federation::LogPosition::new(1))
        );
        assert!(matches!(
            resyncing.liveness,
            myko_federation::SubscriptionLiveness::Resynchronizing { .. }
        ));
        assert_eq!(
            resyncing
                .value
                .as_ref()
                .map(|rows| rows.keys().map(AsRef::as_ref).collect::<Vec<_>>()),
            Some(vec!["retained"])
        );
    }
}
