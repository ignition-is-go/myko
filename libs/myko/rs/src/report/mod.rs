mod handler;
pub mod stream;

use std::{fmt::Debug, pin::Pin, sync::Arc};

use futures::Stream;

/// Type alias for boxed report streams, reducing verbosity in handler signatures.
pub type ReportStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
use serde::{Deserialize, Serialize, de::DeserializeOwned, ser::Error};
use serde_json::Value;
use ts_rs::TS;
use uuid::Uuid;

use crate::{
    actors::report::report_manager::RegisterReportData,
    client::MykoClient,
    common::with_transaction::WithTransaction,
};

pub use handler::{ReportContext, ReportHandler};
pub use stream::StateTransitionExt;

// ─────────────────────────────────────────────────────────────────────────────
// ReportRequest<R> - Generic wrapper that adds tx to any report params
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps report parameters with transaction metadata.
///
/// This type adds `tx` (transaction ID) to any report parameters struct.
/// Uses `#[serde(flatten)]` to serialize as a flat structure.
///
/// # Example
///
/// ```ignore
/// // Report params (what user defines):
/// #[myko_report(ServerStatsOutput)]
/// pub struct ServerStats {}
///
/// // Create a request:
/// let request = ReportRequest::new(ServerStats {});
///
/// // Serializes to: { "tx": "...", ...params }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ReportRequest<R> {
    pub tx: Arc<str>,
    #[serde(flatten)]
    pub report: R,
}

impl<R> ReportRequest<R> {
    /// Create a new report request with auto-generated tx.
    pub fn new(report: R) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            report,
        }
    }
}

impl<R: Default> Default for ReportRequest<R> {
    fn default() -> Self {
        Self::new(R::default())
    }
}

/// Convert report params directly into a ReportRequest.
/// This only works for types that implement ReportParams (actual report param structs),
/// not for ReportRequest itself, which avoids ambiguity with From<&ReportRequest<R>>.
impl<R: ReportParams> From<R> for ReportRequest<R> {
    fn from(report: R) -> Self {
        Self::new(report)
    }
}

/// Convert a reference to a ReportRequest into an owned ReportRequest by cloning.
impl<R: Clone> From<&ReportRequest<R>> for ReportRequest<R> {
    fn from(request: &ReportRequest<R>) -> Self {
        request.clone()
    }
}

impl<R: Send + Sync + 'static> WithTransaction for ReportRequest<R> {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone()
    }
}

impl<R: ReportId> ReportId for ReportRequest<R> {
    fn report_id(&self) -> Arc<str> {
        self.report.report_id()
    }
}

impl<R: ReportIdStatic> ReportIdStatic for ReportRequest<R> {
    fn report_id_static() -> &'static str {
        R::report_id_static()
    }
}

impl<R: ReportOutputType> ReportOutputType for ReportRequest<R> {
    type Output = R::Output;
}

impl<R: ReportId + Serialize + Debug + Send + Sync + 'static> AnyReport for ReportRequest<R> {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("ReportRequest should serialize to JSON")
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
    Serialize
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

// Blanket impl for any type that satisfies the bounds
impl<T> ReportParams for T where
    T: Serialize
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

/// Convenience macro for creating report streams.
///
/// This macro wraps `Box::pin(async_stream::stream! { ... })` to reduce boilerplate
/// in `ReportHandler::compute` implementations.
///
/// # Example
///
/// ```ignore
/// impl ReportHandler for MyReport {
///     type Output = MyOutput;
///
///     fn compute(ctx: ReportContext) -> Pin<Box<dyn Stream<Item = Self::Output> + Send>> {
///         report_stream! {
///             let data_stream = ctx.query(MyQuery::default());
///             futures::pin_mut!(data_stream);
///             while let Some(items) = data_stream.next().await {
///                 yield MyOutput { count: items.len() };
///             }
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! report_stream {
    ($($body:tt)*) => {
        Box::pin(async_stream::stream! { $($body)* })
    };
}

inventory::collect!(ReportRegistration);

/// Type alias for the report factory function pointer.
/// Returns the data needed to register a report (compute_fn).
pub type ReportFactoryFn = fn() -> RegisterReportData;

pub struct ReportRegistration {
    pub report_id: &'static str,
    pub output_type: &'static str,
    pub crate_name: &'static str,
    /// The crate where the output type is defined (for filtering imports)
    pub output_type_crate: &'static str,
    /// Factory function that creates the registration data
    pub factory: ReportFactoryFn,
}

impl std::fmt::Debug for ReportRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportRegistration")
            .field("report_id", &self.report_id)
            .field("output_type", &self.output_type)
            .field("crate_name", &self.crate_name)
            .field("output_type_crate", &self.output_type_crate)
            .finish()
    }
}

/// Wrapper struct for count report outputs.
/// Using a struct instead of a primitive ensures consistent TypeScript type generation via ts-rs.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct CountResult {
    pub count: usize,
}

/// Static report ID for registration
pub trait ReportIdStatic {
    fn report_id_static() -> &'static str;
}

/// Output type for a report
pub trait ReportOutputType {
    type Output: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
}

pub trait MykoReport<T> {
    fn watch(&self, client: &MykoClient) -> impl tokio_stream::Stream<Item = T>;
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ReportResponse {
    pub response: Value,
    pub tx: String,
}

impl ReportResponse {
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WrappedReport {
    pub report: Value,
    pub report_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ReportError {
    pub tx: String,
    pub report_id: String,
    pub message: String,
}

/// Output from a report - either a value or an error
#[derive(Debug, Clone)]
pub enum ReportOutput {
    Value(Value),
    Error(String),
}

pub trait ReportId {
    fn report_id(&self) -> Arc<str>;
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

pub fn wrap_report<Q: ReportId + Serialize + Clone>(
    tx: String,
    report: &Q,
) -> Result<WrappedReport, serde_json::Error> {
    let mut json = serde_json::to_value(report.clone())?;

    let obj_mut = json.as_object_mut();

    if obj_mut.is_none() {
        return Err(serde_json::Error::custom("Could not convert to object"));
    }

    let obj = obj_mut.unwrap();

    obj.insert("tx".to_string(), tx.into());

    Ok(WrappedReport {
        report: json,
        report_id: report.report_id().to_string(),
    })
}

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
    ) -> impl tokio_stream::Stream<Item = <Self as ReportOutputType>::Output>;
}

// Blanket impl of Report for ReportRequest<R>
impl<R: ReportParams> Report for ReportRequest<R> {
    type Params = R;

    fn watch(
        &self,
        client: &MykoClient,
    ) -> impl tokio_stream::Stream<Item = <Self as ReportOutputType>::Output> {
        client.watch_report::<R, <R as ReportOutputType>::Output>(self)
    }
}

/// Extension trait for ReportParams to provide a factory function.
///
/// This is implemented for all ReportParams types via blanket impl.
pub trait ReportFactory: ReportParams {
    /// Create the registration data (compute_fn) for this report type.
    fn create_registration() -> RegisterReportData;
}

impl<R: ReportParams> ReportFactory for R {
    fn create_registration() -> RegisterReportData {
        let compute_fn = Arc::new(
            |ctx: ReportContext,
             _report_value: Value|
             -> Result<Pin<Box<dyn Stream<Item = Value> + Send>>, String> {
                // Parse args - framework handles parse errors
                let args: R = serde_json::from_value(ctx.report_args.clone())
                    .map_err(|e| format!("Failed to parse report args: {}", e))?;

                // Call compute with parsed args as &self
                let stream = args.compute(ctx);

                // Map the output to Value
                Ok(Box::pin(futures::stream::unfold(stream, |mut s| async move {
                    use futures::StreamExt;
                    match s.next().await {
                        Some(output) => match serde_json::to_value(&output) {
                            Ok(value) => {
                                log::trace!("Report serialized output: {}", value);
                                Some((value, s))
                            }
                            Err(e) => {
                                log::error!("Failed to serialize report output: {}", e);
                                None
                            }
                        },
                        None => None,
                    }
                })))
            },
        );

        RegisterReportData {
            report_id: <R as ReportIdStatic>::report_id_static().into(),
            compute_fn,
        }
    }
}
