//! Report values retain dependency lifecycle through composition and caching.

use std::sync::Arc;

use hyphae::{Cell, CellImmutable, Definite, Gettable as _, MapExt as _, Materialize};
#[cfg(not(target_arch = "wasm32"))]
use myko_federation::{LiveSubscription, SubscriptionLiveness};

use super::AnyOutput;

/// Materialized report output. Retained values cannot become raw local cells.
pub enum ReportValue<T: AnyOutput + PartialEq + ?Sized> {
    LocalCell(Cell<Arc<T>, CellImmutable>),
    #[cfg(not(target_arch = "wasm32"))]
    RetainedPublication(LiveSubscription<Arc<T>, serde_json::Value>),
}

impl<T: AnyOutput + PartialEq + ?Sized> Clone for ReportValue<T> {
    fn clone(&self) -> Self {
        match self {
            Self::LocalCell(cell) => Self::LocalCell(cell.clone()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                Self::RetainedPublication(publication.clone())
            }
        }
    }
}

impl<T: AnyOutput + PartialEq + ?Sized> ReportValue<T> {
    /// Derives a report without dropping the dependency's cursor or liveness.
    pub fn map_value<U, F>(&self, transform: F) -> ReportValue<U>
    where
        U: AnyOutput + PartialEq + ?Sized,
        F: Fn(&Arc<T>) -> Arc<U> + Send + Sync + 'static,
    {
        match self {
            Self::LocalCell(cell) => {
                ReportValue::LocalCell(cell.clone().map(transform).materialize())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                ReportValue::RetainedPublication(publication.map_value(transform))
            }
        }
    }

    /// Reads a current value, failing explicitly when its dependencies are stale.
    ///
    /// # Errors
    /// Returns an error for connecting, desynchronized, invalid, or absent output.
    pub fn read_current(&self) -> Result<Arc<T>, String> {
        match self {
            Self::LocalCell(cell) => Ok(cell.get()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                let state = publication.current();
                if state.liveness != SubscriptionLiveness::Current {
                    return Err(format!("report is not current: {:?}", state.liveness));
                }
                state
                    .value
                    .ok_or_else(|| "report has no current value".to_owned())
            }
        }
    }

    /// # Errors
    /// Rejects retained output because a raw cell cannot represent its lifecycle.
    pub fn into_local_cell(self) -> Result<Cell<Arc<T>, CellImmutable>, String> {
        match self {
            Self::LocalCell(cell) => Ok(cell),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(_) => {
                Err("retained report output cannot be converted to a local cell".to_owned())
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn into_live(self) -> LiveSubscription<Arc<T>, serde_json::Value> {
        match self {
            Self::RetainedPublication(publication) => publication,
            Self::LocalCell(cell) => LiveSubscription::from_state_cell(
                cell.map(|value| myko_federation::LiveSubscriptionState {
                    value: Some(Arc::clone(value)),
                    through: None,
                    liveness: SubscriptionLiveness::Current,
                })
                .materialize(),
            ),
        }
    }
}

/// Handler output accepted without discarding retained dependency metadata.
pub trait ReportBuildOutput<T: AnyOutput + PartialEq> {
    fn materialize_report(self) -> ReportValue<T>;
}

impl<T, P> ReportBuildOutput<T> for P
where
    T: AnyOutput + PartialEq,
    P: Materialize<Arc<T>, Definite>,
{
    fn materialize_report(self) -> ReportValue<T> {
        ReportValue::LocalCell(self.materialize())
    }
}

impl<T: AnyOutput + PartialEq> ReportBuildOutput<T> for ReportValue<T> {
    fn materialize_report(self) -> Self {
        self
    }
}

/// A report derived from a retained source, with its typed frontier intact.
#[cfg(not(target_arch = "wasm32"))]
pub struct RetainedReport<
    T: AnyOutput + PartialEq,
    C: hyphae::CellValue = myko_federation::LogPosition,
> {
    publication: LiveSubscription<Arc<T>, C>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: AnyOutput + PartialEq, C: hyphae::CellValue> RetainedReport<T, C> {
    #[must_use]
    pub const fn new(publication: LiveSubscription<Arc<T>, C>) -> Self {
        Self { publication }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: AnyOutput + PartialEq, C: hyphae::CellValue + serde::Serialize> ReportBuildOutput<T>
    for RetainedReport<T, C>
{
    fn materialize_report(self) -> ReportValue<T> {
        ReportValue::RetainedPublication(
            self.publication
                .try_map_cursor(|cursor| serde_json::to_value(cursor)),
        )
    }
}

pub enum WeakReportValue<T: AnyOutput + PartialEq> {
    LocalCell(hyphae::cell::WeakCell<Arc<T>, CellImmutable>),
    #[cfg(not(target_arch = "wasm32"))]
    RetainedPublication(myko_federation::WeakLiveSubscription<Arc<T>, serde_json::Value>),
}

impl<T: AnyOutput + PartialEq> WeakReportValue<T> {
    pub(crate) fn new(value: &ReportValue<T>) -> Self {
        match value {
            ReportValue::LocalCell(cell) => Self::LocalCell(cell.downgrade()),
            #[cfg(not(target_arch = "wasm32"))]
            ReportValue::RetainedPublication(publication) => {
                Self::RetainedPublication(publication.downgrade())
            }
        }
    }

    pub(crate) fn upgrade(&self) -> Option<ReportValue<T>> {
        match self {
            Self::LocalCell(cell) => cell.upgrade().map(ReportValue::LocalCell),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                publication.upgrade().map(ReportValue::RetainedPublication)
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use myko_federation::{
        CompositeFrontier, LiveSubscriptionState, LogPosition, live_subscription,
    };

    #[tokio::test]
    async fn retained_report_preserves_composite_frontier_and_rejects_stale_reads()
    -> Result<(), String> {
        let _serial = crate::test_util::scheduler_test_serial();
        let frontier = CompositeFrontier {
            left: Some(LogPosition::new(3)),
            right: Some(LogPosition::new(9)),
        };
        let (writer, source) = live_subscription(LiveSubscriptionState {
            value: Some(Arc::new(7_u64)),
            through: Some(frontier.clone()),
            liveness: SubscriptionLiveness::Current,
        });
        let report = RetainedReport::new(source).materialize_report();
        let nested = report.map_value(|value| Arc::new(**value + 1));
        if *nested.read_current()? != 8 || report.clone().into_local_cell().is_ok() {
            return Err(
                "retained report changed its value or allowed lifecycle erasure".to_owned(),
            );
        }
        let live = nested.clone().into_live();
        let mut publications = live.watch_publications();
        let _initial = publications.recv().map_err(|e| e.to_string())?;
        if live.current().through
            != Some(serde_json::to_value(&frontier).map_err(|e| e.to_string())?)
        {
            return Err("report lost its composite frontier".to_owned());
        }
        writer.replace(LiveSubscriptionState {
            value: Some(Arc::new(7)),
            through: Some(frontier.clone()),
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: "dependency unavailable".to_owned(),
            },
        });
        let pending =
            tokio::time::timeout(std::time::Duration::from_secs(1), publications.recv_async())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if !matches!(
            pending.state.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ) || report.read_current().is_ok()
            || nested.read_current().is_ok()
            || live.current().value != Some(Arc::new(8))
        {
            return Err("report accepted stale data or lost its retained value".to_owned());
        }
        writer.replace(LiveSubscriptionState {
            value: Some(Arc::new(7)),
            through: Some(frontier),
            liveness: SubscriptionLiveness::Current,
        });
        let recovered =
            tokio::time::timeout(std::time::Duration::from_secs(1), publications.recv_async())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if recovered.state.liveness != SubscriptionLiveness::Current || *nested.read_current()? != 8
        {
            return Err("unchanged report value did not recover".to_owned());
        }
        Ok(())
    }

    #[test]
    fn weak_report_cache_preserves_publication_identity_without_owning_it() -> Result<(), String> {
        let _serial = crate::test_util::scheduler_test_serial();
        let (_writer, source) = live_subscription(LiveSubscriptionState {
            value: Some(Arc::new(7_u64)),
            through: Some(LogPosition::new(3)),
            liveness: SubscriptionLiveness::Current,
        });
        let report = RetainedReport::new(source).materialize_report();
        let cache = WeakReportValue::new(&report);
        let cached = cache
            .upgrade()
            .ok_or_else(|| "live report cache expired".to_owned())?;
        match (&report, &cached) {
            (ReportValue::RetainedPublication(left), ReportValue::RetainedPublication(right)) => {
                if !left.shares_state_with(right)
                    || left.publication().get() != right.publication().get()
                {
                    return Err("cache recreated the retained publication".to_owned());
                }
            }
            _ => return Err("cache erased retained output".to_owned()),
        }
        drop(cached);
        drop(report);
        if cache.upgrade().is_some() {
            return Err("weak cache retained an abandoned report".to_owned());
        }
        Ok(())
    }
}
