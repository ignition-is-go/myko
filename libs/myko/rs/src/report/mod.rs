mod handler;

use std::{pin::Pin, sync::Arc};

use futures::Stream;
use log::error;
use serde::{Deserialize, Serialize, de::DeserializeOwned, ser::Error};
use serde_json::Value;
use ts_rs::TS;

use crate::{
    actors::{
        report::report_manager::{RegisterReportData, ReportManagerMsg},
        server::ServerMsg,
    },
    client::MykoClient,
    common::with_transaction::WithTransaction,
    server::MykoServer,
};

pub use handler::{ReportContext, ReportHandler, ReportRunnerHandle, SubscriptionRequest};

inventory::collect!(ReportRegistration);

#[derive(Debug)]
pub struct ReportRegistration {
    pub report_id: &'static str,
    pub output_type: &'static str,
    pub crate_name: &'static str,
    /// The crate where the output type is defined (for filtering imports)
    pub output_type_crate: &'static str,
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
    pub message: String,
}

pub trait ReportId {
    fn report_id(&self) -> String;
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
        report_id: report.report_id(),
    })
}

/// Main trait for reports that can be registered and watched.
///
/// A Report combines:
/// - ReportId: runtime report ID
/// - ReportIdStatic: static report ID for registration
/// - ReportOutputType: the output type of the report
/// - ReportHandler: the compute logic
/// - WithTransaction: transaction ID handling
pub trait Report:
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + ReportId
    + ReportIdStatic
    + ReportOutputType
    + ReportHandler
    + WithTransaction
    + 'static
{
    /// Watch this report on a client connection
    fn watch(
        &self,
        client: &MykoClient,
    ) -> impl tokio_stream::Stream<Item = <Self as ReportOutputType>::Output>;

    /// Register this report handler with the server
    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        let compute_fn = Arc::new(
            |ctx: ReportContext, _report_value: Value| -> Pin<Box<dyn Stream<Item = Value> + Send>> {
                // The report args are available in ctx.report_args
                // Call the handler's compute function
                let stream = <Self as ReportHandler>::compute(ctx);

                // Map the output to Value
                Box::pin(futures::stream::unfold(stream, |mut s| async move {
                    use futures::StreamExt;
                    match s.next().await {
                        Some(output) => {
                            let value = serde_json::to_value(&output).ok()?;
                            Some((value, s))
                        }
                        None => None,
                    }
                }))
            },
        );

        match server
            .server
            .send_message(ServerMsg::ReportManagerMsg(ReportManagerMsg::RegisterReport(
                RegisterReportData {
                    report_id: <Self as ReportIdStatic>::report_id_static().into(),
                    compute_fn,
                },
            )))
        {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to register report: {}", err);
            }
        };

        Ok(())
    }
}
