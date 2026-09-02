//! Optional WebSocket edge adapter for the transport-neutral Myko node.
//!
//! Mesh nodes, backends, and services do not depend on this crate. It exists
//! for browsers and other short-lived clients that need a conventional socket
//! entry point. [`WebSocketGateway::spawn`] provides a supervised listener with
//! an explicit shutdown boundary; embedding applications remain responsible
//! for authentication and safe bind-address policy.

#![forbid(unsafe_code)]

use std::{net::SocketAddr, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use myko_app::ApplicationNode;
use myko_federation::{AllowAllAccessPolicy, Node, PrincipalId};
use myko_session::NodeSessionService;
pub use myko_wire::{
    NodeFrame as ServerMessage, NodeRequest as ClientRequest, NodeRequestEnvelope, WireEnvelope,
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Failures that stop a gateway listener or connection.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("gateway I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("gateway JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("gateway task error: {0}")]
    Task(String),
}

/// Supervised optional WebSocket edge listener.
#[derive(Debug)]
pub struct WebSocketGatewayServer {
    local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl WebSocketGatewayServer {
    /// Returns the actual listener address, including an OS-selected port.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Subscribes to listener completion without polling the join handle.
    #[must_use]
    pub fn subscribe_finished(&self) -> watch::Receiver<bool> {
        self.finished.clone()
    }

    /// Stops accepting clients, closes supervised connections, and waits for
    /// the listener task to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener or its supervisor failed.
    pub async fn shutdown(self) -> Result<(), GatewayError> {
        let _ = self.shutdown.send(true);
        self.task
            .await
            .map_err(|error| GatewayError::Task(error.to_string()))?
    }
}

/// Opt-in WebSocket view of a Myko node.
#[derive(Clone)]
pub struct WebSocketGateway {
    sessions: NodeSessionService,
}

impl std::fmt::Debug for WebSocketGateway {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketGateway")
            .field("node_id", &self.sessions.node().node_id())
            .finish()
    }
}

impl WebSocketGateway {
    /// Wraps a node without changing its storage or mesh transports.
    #[must_use]
    pub fn new(node: Node) -> Self {
        Self {
            sessions: NodeSessionService::new(node, Arc::new(AllowAllAccessPolicy)),
        }
    }

    /// Exposes a node together with its registered query, report, and view
    /// application through the compatibility gateway.
    #[must_use]
    pub fn for_application(application: ApplicationNode) -> Self {
        Self {
            sessions: NodeSessionService::for_application(
                application,
                Arc::new(AllowAllAccessPolicy),
            ),
        }
    }

    /// Exposes an existing transport-neutral semantic endpoint.
    ///
    /// This is the preferred constructor when native and local transports are
    /// already serving the same node.
    #[must_use]
    pub const fn for_sessions(sessions: NodeSessionService) -> Self {
        Self { sessions }
    }

    /// Binds and serves the gateway until the listener fails or is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot bind or accept a connection.
    pub async fn serve(&self, bind_addr: SocketAddr) -> Result<(), GatewayError> {
        let listener = TcpListener::bind(bind_addr).await?;
        self.serve_listener(listener).await
    }

    /// Binds and starts a supervised optional edge listener.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener cannot bind or inspect its address.
    pub async fn spawn(
        &self,
        bind_addr: SocketAddr,
    ) -> Result<WebSocketGatewayServer, GatewayError> {
        let listener = TcpListener::bind(bind_addr).await?;
        self.spawn_listener(listener)
    }

    /// Starts a supervised server over an already-bound listener.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener's local address is unavailable.
    pub fn spawn_listener(
        &self,
        listener: TcpListener,
    ) -> Result<WebSocketGatewayServer, GatewayError> {
        let local_addr = listener.local_addr()?;
        let gateway = self.clone();
        let (shutdown, shutdown_requested) = watch::channel(false);
        let (finished_sender, finished) = watch::channel(false);
        let task = tokio::spawn(async move {
            let result = gateway
                .serve_listener_inner(listener, Some(shutdown_requested))
                .await;
            finished_sender.send_replace(true);
            result
        });
        Ok(WebSocketGatewayServer {
            local_addr,
            shutdown,
            finished,
            task,
        })
    }

    /// Serves an already-bound listener, which is useful for supervised nodes.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot accept a connection.
    pub async fn serve_listener(&self, listener: TcpListener) -> Result<(), GatewayError> {
        self.serve_listener_inner(listener, None).await
    }

    async fn serve_listener_inner(
        &self,
        listener: TcpListener,
        mut shutdown: Option<watch::Receiver<bool>>,
    ) -> Result<(), GatewayError> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let gateway = self.clone();
                    connections.spawn(async move { gateway.handle_stream(stream).await });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    match joined {
                        Some(Ok(Ok(()))) | None => {}
                        Some(Ok(Err(error))) => tracing_fallback(&error),
                        Some(Err(error)) => return Err(GatewayError::Task(error.to_string())),
                    }
                }
                () = wait_for_shutdown(&mut shutdown) => break,
            }
        }
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                return Err(GatewayError::Task(error.to_string()));
            }
        }
        Ok(())
    }

    async fn handle_stream(&self, stream: TcpStream) -> Result<(), GatewayError> {
        self.sessions.node().wait_until_ready().await;
        let socket = accept_async(stream).await?;
        let (mut write, mut read) = socket.split();
        while let Some(frame) = read.next().await {
            let frame = frame?;
            let Message::Text(text) = frame else {
                if frame.is_close() {
                    break;
                }
                continue;
            };
            let request = match serde_json::from_str::<WireEnvelope<NodeRequestEnvelope>>(&text)
                .map_err(GatewayError::from)
                .and_then(|envelope| {
                    envelope
                        .into_current()
                        .map_err(|error| GatewayError::Task(error.to_string()))
                }) {
                Ok(request) => request,
                Err(error) => {
                    send(
                        &mut write,
                        &ServerMessage::Error {
                            message: error.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
            };
            let principal = PrincipalId::new("websocket:compatibility-client");
            let mut frames = self.sessions.open(principal, request).await;
            while let Some(frame) = frames.recv().await {
                send(&mut write, &frame).await?;
            }
        }
        Ok(())
    }
}

async fn wait_for_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
    let Some(shutdown) = shutdown else {
        return std::future::pending().await;
    };
    loop {
        if *shutdown.borrow() || shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn send<S>(sink: &mut S, message: &ServerMessage) -> Result<(), GatewayError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let json = serde_json::to_string(&WireEnvelope::new(message.clone()))?;
    sink.send(Message::Text(json.into())).await?;
    Ok(())
}

fn tracing_fallback(error: &GatewayError) {
    eprintln!("Myko WebSocket gateway connection failed: {error}");
}

/// Shareable gateway handle for node supervisors.
pub type SharedWebSocketGateway = Arc<WebSocketGateway>;

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use myko_app::capability::Querying as _;
    use myko_app::{
        AppError, CommandContext, CommandError, CommandHandler, HandlerRequest, MykoApplication,
        ReportContext, ReportHandler, myko_report,
    };
    use myko_federation::{
        BatchId, ChangeBatch, CommandId, CommandRequest, CommandSnapshot, CommandSubmission,
        ItemMutation, LiveSubscription, LogPosition, PrincipalId, ScopeId, ServiceId,
    };
    use myko_items::{myko_command, myko_item, myko_service};
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    #[myko_service(GatewayRecord)]
    pub struct GatewayService;

    #[myko_item(service = GatewayService, scope_root)]
    pub struct GatewayRecord {
        pub value: String,
    }

    #[myko_command((), item = GatewayRecord)]
    struct QueueGatewayCommand;

    impl CommandHandler for QueueGatewayCommand {
        fn scope(&self, _node_id: myko_federation::NodeId) -> GatewayRecordId {
            GatewayRecordId::from("gateway")
        }

        fn execute(
            self,
            _context: CommandContext<GatewayService, GatewayRecord>,
        ) -> Result<(), CommandError> {
            Ok(())
        }
    }

    #[myko_report(u64, item = GatewayRecord)]
    #[derive(Copy)]
    struct GatewayCount {
        source_node: myko_federation::NodeId,
    }

    impl ReportHandler for GatewayCount {
        type Output = u64;
        type Cursor = LogPosition;

        fn build(
            &self,
            context: &ReportContext,
        ) -> Result<LiveSubscription<Self::Output>, AppError> {
            Ok(context
                .query(
                    self.source_node,
                    ScopeId::new("gateway"),
                    GetAllGatewayRecords,
                )?
                .map_value(|items| u64::try_from(items.len()).unwrap_or(u64::MAX)))
        }
    }

    async fn round_trip(
        client: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        request: &ClientRequest,
    ) -> Result<ServerMessage, String> {
        client
            .send(Message::Text(
                serde_json::to_string(&WireEnvelope::new(NodeRequestEnvelope::connected(
                    request.clone(),
                )))
                .map_err(|error| error.to_string())?
                .into(),
            ))
            .await
            .map_err(|error| error.to_string())?;
        let Some(frame) = client.next().await else {
            return Err("gateway closed without a response".to_owned());
        };
        let frame = frame.map_err(|error| error.to_string())?;
        let Message::Text(text) = frame else {
            return Err("gateway returned a non-text response".to_owned());
        };
        serde_json::from_str::<WireEnvelope<ServerMessage>>(&text)
            .map_err(|error| error.to_string())?
            .into_current()
            .map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn loopback_client_can_submit_without_claiming_execution() -> Result<(), String> {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<GatewayService>()
            .map_err(|error| error.to_string())?
            .build();
        let gateway =
            WebSocketGateway::for_application(ApplicationNode::new(node.clone(), application));
        let server = gateway
            .spawn(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .map_err(|error| error.to_string())?;
        let (mut client, _) = connect_async(format!("ws://{}", server.local_addr()))
            .await
            .map_err(|error| error.to_string())?;
        let command = CommandSubmission::for_command(&QueueGatewayCommand)
            .map_err(|error| error.to_string())?;
        let command_id = command.id;
        let request = ClientRequest::Submit { command };
        let response = round_trip(&mut client, &request).await?;
        if !matches!(
            response,
            ServerMessage::Command { response }
                if matches!(
                    response.command,
                    Some(CommandSnapshot {
                        state: myko_federation::CommandState::Submitted,
                        ..
                    })
                )
        ) {
            return Err("gateway did not return a submitted command".to_owned());
        }
        let Some(snapshot) = node
            .command(command_id)
            .map_err(|error| error.to_string())?
        else {
            return Err("submitted command was not stored".to_owned());
        };
        if !matches!(snapshot.state, myko_federation::CommandState::Submitted) {
            return Err("short-lived client unexpectedly claimed execution".to_owned());
        }
        let response = round_trip(
            &mut client,
            &ClientRequest::Cancel {
                command_id,
                reason: "client cancelled".to_owned(),
            },
        )
        .await?;
        if !matches!(
            response,
            ServerMessage::Command { response }
                if matches!(
                    response.command,
                    Some(CommandSnapshot {
                        state: myko_federation::CommandState::Cancelled { .. },
                        ..
                    })
                )
        ) {
            return Err("gateway did not durably cancel the submitted command".to_owned());
        }
        client
            .close(None)
            .await
            .map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn handler_protocol_matches_native_application_lifecycle() -> Result<(), String> {
        let node = Node::in_memory();
        let application = MykoApplication::builder()
            .service::<GatewayService>()
            .map_err(|error| error.to_string())?
            .build();
        let gateway =
            WebSocketGateway::for_application(ApplicationNode::new(node.clone(), application));
        let server = gateway
            .spawn(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .map_err(|error| error.to_string())?;
        let (mut client, _) = connect_async(format!("ws://{}", server.local_addr()))
            .await
            .map_err(|error| error.to_string())?;
        let initial = round_trip(
            &mut client,
            &ClientRequest::FollowHandler {
                request: HandlerRequest {
                    kind: myko_app::HandlerKind::Report,
                    handler_id: GatewayCount::REPORT_ID.to_owned(),
                    source_node: None,
                    scope_id: None,
                    params: serde_json::to_value(GatewayCount {
                        source_node: node.node_id(),
                    })
                    .map_err(|error| error.to_string())?,
                },
            },
        )
        .await?;
        if !matches!(
            initial,
            ServerMessage::HandlerState { state }
                if state.value == Some(serde_json::Value::from(0_u64))
        ) {
            return Err("WebSocket handler omitted its initial lifecycle state".to_owned());
        }
        let command = CommandRequest {
            id: CommandId::new(),
            service_id: ServiceId::new(
                <GatewayService as myko_federation::MykoService>::SERVICE_ID,
            ),
            scope_id: ScopeId::new("gateway"),
            principal_id: PrincipalId::new("test:gateway"),
            command_type: "gateway.test.set".to_owned(),
            payload: Vec::new(),
        };
        let admission = node
            .admit(command.clone())
            .map_err(|error| error.to_string())?;
        let record = GatewayRecord {
            id: GatewayRecordId::from("record"),
            value: "live".to_owned(),
        };
        node.commit(
            command.id,
            ChangeBatch {
                id: BatchId::new(),
                command_id: command.id,
                service_id: command.service_id,
                scope_id: command.scope_id,
                causal_parents: vec![admission.snapshot().updated_at],
                changes: vec![ItemMutation::set(&record).map_err(|error| error.to_string())?],
            },
            Vec::new(),
        )
        .map_err(|error| error.to_string())?;
        let updated = tokio::time::timeout(std::time::Duration::from_secs(5), client.next())
            .await
            .map_err(|_| "WebSocket handler did not update".to_owned())?
            .ok_or_else(|| "WebSocket handler stream closed".to_owned())?
            .map_err(|error| error.to_string())?;
        let Message::Text(updated) = updated else {
            return Err("WebSocket handler returned a non-text update".to_owned());
        };
        let updated: ServerMessage = serde_json::from_str::<WireEnvelope<ServerMessage>>(&updated)
            .map_err(|error| error.to_string())?
            .into_current()
            .map_err(|error| error.to_string())?;
        if !matches!(
            updated,
            ServerMessage::HandlerState { state }
                if state.value == Some(serde_json::Value::from(1_u64))
        ) {
            return Err("WebSocket handler lifecycle diverged from Myko".to_owned());
        }
        client
            .close(None)
            .await
            .map_err(|error| error.to_string())?;
        server.shutdown().await.map_err(|error| error.to_string())
    }
}
