//! Report trait definitions.

use std::{fmt::Debug, sync::Arc};

use hypha::CellImmutable;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use ts_rs::TS;

use super::{handler::ReportHandler, request::ReportRequest};
use crate::{
    cache::CacheKey, client::MykoClient, common::with_transaction::WithTransaction,
    wire::WrappedReport,
};

// ─────────────────────────────────────────────────────────────────────────────
// Core Report Traits
// ─────────────────────────────────────────────────────────────────────────────

pub trait ReportId {
    fn report_id(&self) -> Arc<str>;
}

/// Static report ID for registration
pub trait ReportIdStatic {
    fn report_id_static() -> &'static str;
}

/// Output type for a report
pub trait ReportOutputType {
    type Output: Serialize + DeserializeOwned + Clone + Debug + PartialEq + Send + Sync + 'static;
}

pub trait MykoReport<T> {
    fn watch(&self, client: &MykoClient) -> hypha::Cell<Option<T>, CellImmutable>;
}

/// Output from a report - either a value or an error
#[derive(Debug, Clone)]
pub enum ReportOutput {
    Value(Value),
    Error(String),
}

/// Type-erased report trait for dynamic dispatch.
/// All reports implement this via the `#[myko_report]` macro.
pub trait AnyReport: WithTransaction + ReportId + Debug + Send + Sync + 'static {
    /// Serialize this report to a JSON Value.
    fn to_value(&self) -> Value;
}

// Conversion from Arc<dyn AnyReport> to WrappedReport
impl From<&dyn AnyReport> for WrappedReport {
    fn from(report: &dyn AnyReport) -> Self {
        WrappedReport {
            report: report.to_value(),
            report_id: report.report_id().to_string(),
        }
    }
}

impl From<Arc<dyn AnyReport>> for WrappedReport {
    fn from(report: Arc<dyn AnyReport>) -> Self {
        WrappedReport::from(report.as_ref())
    }
}

impl From<&Arc<dyn AnyReport>> for WrappedReport {
    fn from(report: &Arc<dyn AnyReport>) -> Self {
        WrappedReport::from(report.as_ref())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReportParams - Marker trait for report parameter structs (inner type)
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for report parameter structs.
///
/// This is implemented by the user-defined report struct (e.g., `ServerStats`).
/// It combines identity traits without requiring transaction metadata.
///
/// The full `Report` trait is implemented on `ReportRequest<R>` where `R: ReportParams`.
pub trait ReportParams:
    CacheKey
    + Serialize
    + DeserializeOwned
    + Clone
    + Send
    + Sync
    + ReportId
    + ReportIdStatic
    + ReportOutputType
    + ReportHandler
    + Debug
    + 'static
{
}

impl<T> ReportParams for T where
    T: Serialize
        + CacheKey
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + ReportId
        + ReportIdStatic
        + ReportOutputType
        + ReportHandler
        + Debug
        + 'static
{
}

// ─────────────────────────────────────────────────────────────────────────────
// Report - Full trait implemented on ReportRequest<R>
// ─────────────────────────────────────────────────────────────────────────────

/// Full report trait implemented on `ReportRequest<R>`.
///
/// This provides the `watch` method for client-side subscriptions.
/// For server-side registration, use `R::register()` on the params type.
pub trait Report:
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + ReportId
    + ReportIdStatic
    + ReportOutputType
    + WithTransaction
    + AnyReport
    + 'static
{
    /// The inner report params type
    type Params: ReportParams;

    /// Watch this report on a client connection
    fn watch(
        &self,
        client: &MykoClient,
    ) -> hypha::Cell<Option<<Self as ReportOutputType>::Output>, CellImmutable>;
}

// Blanket impl of Report for ReportRequest<R>
impl<R: ReportParams> Report for ReportRequest<R> {
    type Params = R;

    fn watch(
        &self,
        client: &MykoClient,
    ) -> hypha::Cell<Option<<Self as ReportOutputType>::Output>, CellImmutable> {
        client.watch_report::<R, <R as ReportOutputType>::Output>(self)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper types
// ─────────────────────────────────────────────────────────────────────────────

/// Wrapper struct for count report outputs.
/// Using a struct instead of a primitive ensures consistent TypeScript type generation via ts-rs.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct CountResult {
    pub count: usize,
}
