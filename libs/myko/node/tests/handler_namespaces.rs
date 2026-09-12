use std::{error::Error, sync::Arc};

use myko::{ApplicationHost, CommandContext, CommandError, CommandHandler, MykoApplication};
use myko_federation::{AllowAllAccessPolicy, Node, NodeId, PrincipalId, ScopeId};
use myko_local::{LocalClientSession, LocalNodeServer};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[path = "handler_namespaces/nullable.rs"]
mod nullable;

macro_rules! service_fixture {
    ($module:ident) => {
        mod $module {
            use super::*;

            #[myko_items::myko_service(Record)]
            pub struct Service;

            #[myko::myko_item(service = Service, scope_root)]
            pub struct Record {
                pub label: String,
            }

            #[myko_items::myko_command((), item = Record)]
            pub struct Store {
                pub record: Record,
            }

            impl CommandHandler for Store {
                fn scope(&self, _node: NodeId) -> RecordId {
                    self.record.id.clone()
                }

                fn execute(self, context: CommandContext) -> Result<(), CommandError> {
                    context.emit_set(&self.record)?;
                    Ok(())
                }
            }

            #[myko_items::myko_command((), item = Record)]
            pub struct Remove {
                pub id: RecordId,
            }

            impl CommandHandler for Remove {
                fn scope(&self, _node: NodeId) -> RecordId {
                    self.id.clone()
                }

                fn execute(self, context: CommandContext) -> Result<(), CommandError> {
                    context.emit_delete::<Record>(&self.id)?;
                    Ok(())
                }
            }

            #[myko::myko_query(Record, item = Record)]
            #[derive(PartialEq, Eq)]
            pub struct RecordsQuery {}

            impl myko::query::QueryHandler for RecordsQuery {
                fn scope_id(&self, _node: NodeId) -> Option<ScopeId> {
                    Some(ScopeId::for_item::<Record>(&RecordId::from("record")))
                }

                fn build_view(
                    context: myko::query::QueryBuildArgs<Self>,
                ) -> Result<Option<impl myko::query::QueryBuildOutput>, String> {
                    Ok(Some(myko::query::RetainedQuery::new(
                        context.federated_items::<Record>()?,
                    )))
                }
            }

            #[myko::myko_report(Option<String>, item = Record)]
            pub struct RecordLabel {}

            impl myko::report::ReportHandler for RecordLabel {
                type Output = Option<String>;

                fn scope_id(&self, _node: NodeId) -> Option<ScopeId> {
                    Some(ScopeId::for_item::<Record>(&RecordId::from("record")))
                }

                fn compute(
                    &self,
                    context: myko::report::ReportContext,
                ) -> Result<impl myko::report::ReportBuildOutput<Self::Output>, String> {
                    Ok(myko::report::RetainedReport::new(
                        context.federated_items::<Record>()?.map_value(|rows| {
                            Arc::new(rows.get("record").map(|record| record.label.clone()))
                        }),
                    ))
                }
            }

            #[myko::myko_report(String, item = Record)]
            pub struct ServiceLabel {}

            impl myko::report::ReportHandler for ServiceLabel {
                type Output = String;

                fn compute(
                    &self,
                    _context: myko::report::ReportContext,
                ) -> Result<impl myko::report::ReportBuildOutput<Self::Output>, String> {
                    Ok(hyphae::Cell::new(Arc::new(stringify!($module).to_owned())).lock())
                }
            }

            #[myko::myko_view(Record, item = Record)]
            pub struct Records {}

            impl myko::view::ViewHandler for Records {
                fn scope_id(&self, _node: NodeId) -> Option<ScopeId> {
                    Some(ScopeId::for_item::<Record>(&RecordId::from("record")))
                }

                fn build_cell(
                    context: myko::view::ViewBuildArgs<Self>,
                ) -> Result<impl myko::view::ViewBuildOutput<Item = Self::Item>, String> {
                    Ok(myko::view::RetainedView::new(
                        context.federated_items::<Record>()?,
                    ))
                }
            }
        }
    };
}

service_fixture!(left);
service_fixture!(right);

fn verify_wire_identity<Q, R, V>(
    query: Q,
    report: R,
    view: V,
    service: myko::ServiceTypeId,
) -> TestResult
where
    Q: myko::query::QueryParams,
    R: myko::report::ReportParams,
    V: myko::view::ViewParams,
{
    use myko::{
        query::{QueryIdStatic, QueryRequest},
        report::{ReportIdStatic, ReportRequest},
        view::{ViewIdStatic, ViewRequest},
        wire,
    };
    if QueryRequest::<Q>::SERVICE_ID != Some(service)
        || ReportRequest::<R>::SERVICE_ID != Some(service)
        || ViewRequest::<V>::SERVICE_ID != Some(service)
    {
        return Err("transaction wrapper lost its generated service identity".into());
    }
    let query = QueryRequest::new(query);
    let report = ReportRequest::new(report);
    let view = ViewRequest::new(view);
    let erased_query: &dyn myko::query::AnyQuery = &query;
    let erased_report: &dyn myko::report::AnyReport = &report;
    let erased_view: &dyn myko::view::AnyView = &view;
    for encoded in [
        serde_json::to_value(wire::wrap_query(Arc::from("typed"), &query)?)?,
        serde_json::to_value(wire::wrap_report("typed".to_owned(), &report)?)?,
        serde_json::to_value(wire::wrap_view(Arc::from("typed"), &view)?)?,
        serde_json::to_value(wire::WrappedQuery::from(erased_query))?,
        serde_json::to_value(wire::WrappedReport::from(erased_report))?,
        serde_json::to_value(wire::WrappedView::from(erased_view))?,
    ] {
        if encoded.get("serviceId").and_then(serde_json::Value::as_str) != Some(service.as_str()) {
            return Err("wire wrapper lost its generated service identity".into());
        }
    }
    Ok(())
}

#[test]
fn typed_and_erased_wire_wrappers_preserve_service_identity() -> TestResult {
    verify_wire_identity(
        left::RecordsQuery {},
        left::RecordLabel {},
        left::Records {},
        <left::Record as myko::MykoItem>::SERVICE_ID,
    )?;
    verify_wire_identity(
        right::RecordsQuery {},
        right::RecordLabel {},
        right::Records {},
        <right::Record as myko::MykoItem>::SERVICE_ID,
    )
}

struct Fixture {
    directory: tempfile::TempDir,
    server: LocalNodeServer,
    client: myko::client::MykoClient,
    host: ApplicationHost,
    node: NodeId,
    left: left::Record,
    right: right::Record,
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let socket = directory.path().join("namespaces.sock");
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<left::Service>()
            .service::<right::Service>()
            .build();
        let host = ApplicationHost::new(node.clone(), application)?
            .with_access_policy(Arc::new(AllowAllAccessPolicy))?;
        let left = left::Record {
            id: left::RecordId::from("record"),
            label: "left".to_owned(),
        };
        let right = right::Record {
            id: right::RecordId::from("record"),
            label: "right".to_owned(),
        };
        host.exec_command(left::Store {
            record: left.clone(),
        })?;
        host.exec_command(right::Store {
            record: right.clone(),
        })?;
        let server = LocalNodeServer::spawn_application(
            &socket,
            host.clone(),
            PrincipalId::new("local:namespace-test"),
            Arc::new(AllowAllAccessPolicy),
        )
        .await?;
        let client = LocalClientSession::new(&socket)
            .handler_connector()
            .client();
        Ok(Self {
            directory,
            server,
            client,
            host,
            node: node.node_id(),
            left,
            right,
        })
    }
}

#[tokio::test]
async fn same_named_queries_keep_both_services_over_the_socket() -> TestResult {
    let fixture = Fixture::start().await?;
    let left = fixture
        .client
        .follow_query(
            Some(fixture.node),
            ScopeId::for_item::<left::Record>(&fixture.left.id),
            &left::RecordsQuery {},
        )
        .await?;
    let right = fixture
        .client
        .follow_query(
            Some(fixture.node),
            ScopeId::for_item::<right::Record>(&fixture.right.id),
            &right::RecordsQuery {},
        )
        .await?;
    if left.current().value != Some(vec![fixture.left])
        || right.current().value != Some(vec![fixture.right])
    {
        return Err("same-named query selected another service's rows".into());
    }
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn same_named_reports_keep_both_services_over_the_socket() -> TestResult {
    let fixture = Fixture::start().await?;
    let left = fixture.client.follow_report(&left::RecordLabel {}).await?;
    let right = fixture.client.follow_report(&right::RecordLabel {}).await?;
    if left.current().value != Some(Some(fixture.left.label))
        || right.current().value != Some(Some(fixture.right.label))
    {
        return Err(format!(
            "same-named report selected another service's result: left={:?}, right={:?}",
            left.current().value,
            right.current().value,
        )
        .into());
    }
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn same_named_views_keep_both_services_over_the_socket() -> TestResult {
    let fixture = Fixture::start().await?;
    let left = fixture.client.follow_view(&left::Records {}).await?;
    let right = fixture.client.follow_view(&right::Records {}).await?;
    if left.current().value != Some(vec![fixture.left])
        || right.current().value != Some(vec![fixture.right])
    {
        return Err("same-named view selected another service's rows".into());
    }
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn same_named_reports_with_identical_output_types_do_not_share_cache_entries() -> TestResult {
    let fixture = Fixture::start().await?;
    let left = fixture.client.follow_report(&left::ServiceLabel {}).await?;
    let right = fixture
        .client
        .follow_report(&right::ServiceLabel {})
        .await?;
    if left.current().value.as_deref() != Some("left")
        || right.current().value.as_deref() != Some("right")
    {
        return Err("report cache reused another service's handler".into());
    }
    fixture.server.shutdown().await?;
    Ok(())
}

#[cfg(feature = "schema")]
#[test]
fn service_contracts_keep_same_named_reactive_handlers() -> TestResult {
    use myko::application::ServiceHandlerKind;
    use myko_items::{
        MykoService,
        schema::{HandlerResultSchema, TypeSchema},
    };
    let app = MykoApplication::builder()
        .service::<left::Service>()
        .service::<right::Service>()
        .build();
    for (service, row_type) in [
        (left::Service::SERVICE_ID, TypeSchema::of::<left::Record>()),
        (
            right::Service::SERVICE_ID,
            TypeSchema::of::<right::Record>(),
        ),
    ] {
        let contract = app.service_contract(service)?;
        for (kind, name, result) in [
            (
                ServiceHandlerKind::Query,
                "RecordsQuery",
                HandlerResultSchema::Rows(row_type.clone()),
            ),
            (
                ServiceHandlerKind::View,
                "Records",
                HandlerResultSchema::Rows(row_type.clone()),
            ),
            (
                ServiceHandlerKind::Report,
                "RecordLabel",
                HandlerResultSchema::Value(TypeSchema::of::<Option<String>>()),
            ),
        ] {
            let handler = contract
                .handlers
                .get(&(kind, Arc::from(name)))
                .ok_or("service contract lost a same-named handler")?;
            if handler.result != result {
                return Err("service contract used another service's result schema".into());
            }
        }
    }
    Ok(())
}
