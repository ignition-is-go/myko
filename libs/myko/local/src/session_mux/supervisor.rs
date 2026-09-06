use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::{
    sync::{Notify, mpsc, oneshot, watch},
    task::JoinHandle,
};

use super::{
    endpoint::{LogicalLeaseState, MuxRouteEvent, MuxSubscription, RouteTerminal},
    protocol::{
        CONTROL_CAPACITY, ClientMuxFrame, CloseReason, LOCAL_SESSION_MUX_VERSION,
        LocalConnectionHello, MAX_LOGICAL_STREAMS, OpenReject, PER_STREAM_FRAME_CAPACITY,
        ServerMuxFrame, StreamId,
    },
};
use crate::{
    Envelope, HandlerClientError, LocalPeerError, NodeRequestEnvelope, PeerFrame, PeerRequest,
    ReconnectPolicy,
    transport::{connect_local_peer, read_frame, write_frame},
};

impl OpenReject {
    fn terminal(&self) -> RouteTerminal {
        match self {
            Self::Capacity => RouteTerminal::Transport(Arc::from(
                "local session multiplexer reached its stream capacity",
            )),
            Self::DuplicateId => {
                RouteTerminal::Protocol(Arc::from("local session stream ID was duplicated"))
            }
            Self::InvalidRequest(message) => RouteTerminal::Protocol(Arc::from(message.as_str())),
        }
    }
}

impl CloseReason {
    fn terminal(&self) -> RouteTerminal {
        match self {
            Self::Completed => {
                RouteTerminal::Completed(Arc::from("local session stream completed"))
            }
            Self::Cancelled => {
                RouteTerminal::Transport(Arc::from("local session stream was cancelled"))
            }
            Self::ServerShutdown => {
                RouteTerminal::Transport(Arc::from("local session server shut down"))
            }
            Self::Protocol(message) => RouteTerminal::Protocol(Arc::from(message.as_str())),
        }
    }
}

#[derive(Debug)]
pub struct LocalSessionMux {
    command_tx: mpsc::Sender<ClientCommand>,
    cancel_wake: Arc<Notify>,
    next_stream_id: Arc<AtomicU64>,
    supervisor: JoinHandle<()>,
}

impl LocalSessionMux {
    pub(super) fn spawn(socket_path: PathBuf, reconnect_policy: ReconnectPolicy) -> Self {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_CAPACITY);
        let cancel_wake = Arc::new(Notify::new());
        let next_stream_id = Arc::new(AtomicU64::new(1));
        let supervisor = tokio::spawn(run_client_supervisor(
            socket_path,
            reconnect_policy,
            command_rx,
            Arc::clone(&cancel_wake),
            Arc::clone(&next_stream_id),
        ));
        Self {
            command_tx,
            cancel_wake,
            next_stream_id,
            supervisor,
        }
    }

    fn allocate_stream_id(&self) -> Result<StreamId, HandlerClientError> {
        self.next_stream_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map(StreamId)
            .map_err(|_| {
                HandlerClientError::Protocol("local session stream IDs are exhausted".to_owned())
            })
    }

    pub async fn open(
        self: &Arc<Self>,
        request: NodeRequestEnvelope,
    ) -> Result<MuxSubscription, HandlerClientError> {
        let stream_id = self.allocate_stream_id()?;
        let lease = Arc::new(LogicalLeaseState::new());
        let mut opening_guard = OpeningGuard {
            lease: Arc::clone(&lease),
            cancel_wake: Arc::clone(&self.cancel_wake),
            armed: true,
        };
        let (frames_tx, frames) = mpsc::channel(PER_STREAM_FRAME_CAPACITY);
        let (terminal_tx, terminal) = watch::channel(None);
        let (reply, opened) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Open {
                stream_id,
                request,
                lease: Arc::clone(&lease),
                frames: frames_tx,
                terminal: terminal_tx,
                reply,
            })
            .await
            .map_err(|_| {
                HandlerClientError::Transport(
                    "local session multiplexer supervisor stopped".to_owned(),
                )
            })?;
        opened.await.map_err(|_| {
            HandlerClientError::Transport("local handler open was interrupted".to_owned())
        })??;
        opening_guard.armed = false;
        Ok(MuxSubscription::new(
            Arc::clone(self),
            lease,
            Arc::clone(&self.cancel_wake),
            frames,
            terminal,
        ))
    }
}

impl Drop for LocalSessionMux {
    fn drop(&mut self) {
        self.supervisor.abort();
    }
}

enum ClientCommand {
    Open {
        stream_id: StreamId,
        request: NodeRequestEnvelope,
        lease: Arc<LogicalLeaseState>,
        frames: mpsc::Sender<MuxRouteEvent>,
        terminal: watch::Sender<Option<RouteTerminal>>,
        reply: oneshot::Sender<Result<(), HandlerClientError>>,
    },
}

struct OpeningGuard {
    lease: Arc<LogicalLeaseState>,
    cancel_wake: Arc<Notify>,
    armed: bool,
}

impl Drop for OpeningGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lease.cancelled.store(true, Ordering::Release);
            self.cancel_wake.notify_one();
        }
    }
}

struct ClientRoute {
    request: NodeRequestEnvelope,
    lease: Arc<LogicalLeaseState>,
    frames: mpsc::Sender<MuxRouteEvent>,
    terminal: watch::Sender<Option<RouteTerminal>>,
    reply: Option<oneshot::Sender<Result<(), HandlerClientError>>>,
    phase: ClientStreamPhase,
    reopening: bool,
}

impl ClientRoute {
    fn observe_progress(&mut self, frame: &PeerFrame) {
        Self::observe_history_progress(&mut self.request.request, frame);
        Self::observe_command_progress(&mut self.request.request, frame);
        Self::observe_item_progress(&mut self.request.request, frame);
    }

    fn observe_history_progress(request: &mut PeerRequest, frame: &PeerFrame) {
        match (request, frame) {
            (PeerRequest::Follow { after }, PeerFrame::Batch { batch }) => {
                if let Some(through) = batch.through {
                    *after = Some(through);
                }
            }
            (PeerRequest::FollowScope { after, .. }, PeerFrame::ScopedBatch { batch }) => {
                if let Some(through) = batch.through {
                    *after = Some(through);
                }
            }
            (PeerRequest::FollowSelected { after, .. }, PeerFrame::SelectedBatch { batch }) => {
                if let Some(through) = batch.through {
                    *after = Some(through);
                }
            }
            _ => {}
        }
    }

    fn observe_command_progress(request: &mut PeerRequest, frame: &PeerFrame) {
        if let (PeerRequest::WatchCommands { request }, PeerFrame::CommandUpdate { update }) =
            (request, frame)
        {
            // A CommandUpdate is one atomic causal release, even when it
            // contains several commands. Persist its cursor only after the
            // complete frame has reached the route.
            request.after = Some(update.through);
        }
    }

    fn observe_item_progress(request: &mut PeerRequest, frame: &PeerFrame) {
        if let (PeerRequest::FollowItems { request }, PeerFrame::ItemUpdate { update }) =
            (request, frame)
        {
            request.after = Some(update.through);
        }
    }

    const fn suppress_reopen_control(&mut self, frame: &PeerFrame) -> bool {
        if !self.reopening || matches!(frame, PeerFrame::Authorization { .. }) {
            return false;
        }
        self.reopening = false;
        matches!(
            frame,
            PeerFrame::CommandWatchReady { .. } | PeerFrame::ItemFollowReady { .. }
        )
    }
}

#[derive(Clone, Copy)]
enum ClientStreamPhase {
    Queued,
    OpenWritten,
    Opened,
}

struct ClientGeneration {
    id: u64,
    writer_tx: mpsc::Sender<ClientMuxFrame>,
    reader: OwnedTask,
    writer: OwnedTask,
}

impl ClientGeneration {
    async fn stop(mut self) {
        self.reader.stop().await;
        self.writer.stop().await;
    }
}

struct OwnedTask(Option<JoinHandle<()>>);

impl OwnedTask {
    const fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    async fn join(&mut self) {
        if let Some(task) = self.0.take() {
            let _ignored = task.await;
        }
    }

    async fn stop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ignored = task.await;
        }
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

struct ConnectingGeneration {
    id: u64,
    task: OwnedTask,
}

enum ClientIoEvent {
    Connected(ClientGeneration),
    ConnectFailed {
        generation: u64,
        terminal: RouteTerminal,
    },
    Frame {
        generation: u64,
        frame: ServerMuxFrame,
    },
    Lost {
        generation: u64,
        terminal: RouteTerminal,
    },
}

struct ClientSupervisor {
    socket_path: PathBuf,
    reconnect_policy: ReconnectPolicy,
    routes: HashMap<StreamId, ClientRoute>,
    pending_opens: VecDeque<StreamId>,
    generation: Option<ClientGeneration>,
    connecting: Option<ConnectingGeneration>,
    next_generation: u64,
    event_tx: mpsc::Sender<ClientIoEvent>,
    next_stream_id: Arc<AtomicU64>,
}

async fn run_client_supervisor(
    socket_path: PathBuf,
    reconnect_policy: ReconnectPolicy,
    mut command_rx: mpsc::Receiver<ClientCommand>,
    cancel_wake: Arc<Notify>,
    next_stream_id: Arc<AtomicU64>,
) {
    let (event_tx, mut event_rx) = mpsc::channel(CONTROL_CAPACITY);
    let mut supervisor = ClientSupervisor {
        socket_path,
        reconnect_policy,
        routes: HashMap::new(),
        pending_opens: VecDeque::new(),
        generation: None,
        connecting: None,
        next_generation: 1,
        event_tx,
        next_stream_id,
    };

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                supervisor.handle_command(command);
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                supervisor.handle_event(event).await;
            }
            () = cancel_wake.notified() => {}
        }
        supervisor.maintain().await;
    }
    supervisor.shutdown().await;
}

impl ClientSupervisor {
    fn handle_command(&mut self, command: ClientCommand) {
        match command {
            ClientCommand::Open {
                stream_id,
                request,
                lease,
                frames,
                terminal,
                reply,
            } => {
                if lease.cancelled.load(Ordering::Acquire) {
                    let _ignored = reply.send(Err(HandlerClientError::Transport(
                        "local handler open was cancelled".to_owned(),
                    )));
                    return;
                }
                if self.routes.len() >= MAX_LOGICAL_STREAMS {
                    let _ignored = reply.send(Err(HandlerClientError::Transport(
                        "local session multiplexer reached its stream capacity".to_owned(),
                    )));
                    return;
                }
                self.routes.insert(
                    stream_id,
                    ClientRoute {
                        request,
                        lease,
                        frames,
                        terminal,
                        reply: Some(reply),
                        phase: ClientStreamPhase::Queued,
                        reopening: false,
                    },
                );
                self.pending_opens.push_back(stream_id);
            }
        }
    }

    async fn handle_event(&mut self, event: ClientIoEvent) {
        match event {
            ClientIoEvent::Connected(generation) => self.handle_connected(generation).await,
            ClientIoEvent::ConnectFailed {
                generation,
                terminal,
            } => self.handle_connect_failed(generation, terminal).await,
            ClientIoEvent::Frame { generation, frame } => {
                if self.generation.as_ref().map(|current| current.id) != Some(generation) {
                    return;
                }
                self.handle_server_frame(frame).await;
            }
            ClientIoEvent::Lost {
                generation,
                terminal,
            } => self.handle_lost(generation, terminal).await,
        }
    }

    async fn handle_connected(&mut self, generation: ClientGeneration) {
        let expected = self.connecting.as_ref().map(|connecting| connecting.id);
        if expected != Some(generation.id) {
            generation.stop().await;
            return;
        }
        if let Some(mut connecting) = self.connecting.take() {
            connecting.task.join().await;
        }
        self.generation = Some(generation);
    }

    async fn handle_connect_failed(&mut self, generation: u64, terminal: RouteTerminal) {
        if self.connecting.as_ref().map(|connecting| connecting.id) != Some(generation) {
            return;
        }
        if let Some(mut connecting) = self.connecting.take() {
            connecting.task.join().await;
        }
        match terminal {
            RouteTerminal::Transport(message) => {
                tracing::debug!(
                    generation,
                    error = %message,
                    "local Myko session handshake failed; reconnecting"
                );
                self.queue_routes_for_reconnect(&message);
            }
            terminal => self.fail_routes(&terminal),
        }
    }

    async fn handle_lost(&mut self, generation: u64, terminal: RouteTerminal) {
        if self.generation.as_ref().map(|current| current.id) != Some(generation) {
            return;
        }
        if let Some(generation) = self.generation.take() {
            generation.stop().await;
        }
        tracing::debug!(
            generation,
            error = ?terminal,
            "local Myko session connection lost; reopening live requests"
        );
        let reason = match terminal {
            RouteTerminal::Completed(message)
            | RouteTerminal::Transport(message)
            | RouteTerminal::Protocol(message) => message,
        };
        self.queue_routes_for_reconnect(&reason);
    }

    async fn handle_server_frame(&mut self, frame: ServerMuxFrame) {
        if let Err(terminal) = self.validate_server_frame(&frame) {
            self.fail_generation(terminal).await;
        } else {
            self.dispatch_server_frame(frame).await;
        }
    }

    fn validate_server_frame(&self, frame: &ServerMuxFrame) -> Result<(), RouteTerminal> {
        let stream_id = frame.stream_id().ok_or_else(|| {
            RouteTerminal::Protocol(Arc::from("local session server repeated its ready frame"))
        })?;
        if stream_id.0 >= self.next_stream_id.load(Ordering::Acquire) {
            return Err(RouteTerminal::Protocol(Arc::from(
                "local session server used a stream ID the client never issued",
            )));
        }
        Ok(())
    }

    async fn dispatch_server_frame(&mut self, frame: ServerMuxFrame) {
        match frame {
            ServerMuxFrame::Ready { .. } => {}
            ServerMuxFrame::Opened { stream_id } => {
                self.handle_opened(stream_id).await;
            }
            ServerMuxFrame::Rejected { stream_id, reason } => {
                if let Some(route) = self.routes.remove(&stream_id) {
                    finish_client_route(route, reason.terminal());
                }
            }
            ServerMuxFrame::Data { stream_id, frame } => {
                self.handle_data(stream_id, frame).await;
            }
            ServerMuxFrame::Closed { stream_id, reason } => {
                self.handle_closed(stream_id, reason).await;
            }
        }
    }

    async fn handle_closed(&mut self, stream_id: StreamId, reason: CloseReason) {
        if matches!(reason, CloseReason::ServerShutdown) {
            self.reconnect_generation(Arc::from("local session server shut down"))
                .await;
        } else if let Some(route) = self.routes.remove(&stream_id) {
            finish_client_route(route, reason.terminal());
        }
    }

    async fn handle_opened(&mut self, stream_id: StreamId) {
        let Some(route) = self.routes.get_mut(&stream_id) else {
            return;
        };
        if !matches!(route.phase, ClientStreamPhase::OpenWritten) {
            self.fail_generation(RouteTerminal::Protocol(Arc::from(
                "local session stream opened out of order",
            )))
            .await;
            return;
        }
        route.phase = ClientStreamPhase::Opened;
        if let Some(reply) = route.reply.take() {
            let _ignored = reply.send(Ok(()));
        }
    }

    async fn handle_data(&mut self, stream_id: StreamId, frame: PeerFrame) {
        let Some(route) = self
            .routes
            .get_mut(&stream_id)
            .filter(|route| matches!(route.phase, ClientStreamPhase::Opened))
        else {
            return;
        };
        route.observe_progress(&frame);
        if route.suppress_reopen_control(&frame) {
            return;
        }
        let send_result = route.frames.try_send(MuxRouteEvent::Frame(frame));
        match send_result {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.cancel_route(stream_id).await;
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(route) = self.routes.remove(&stream_id) {
                    finish_client_route(
                        route,
                        RouteTerminal::Transport(Arc::from("local handler consumer fell behind")),
                    );
                }
                self.write_cancel(stream_id).await;
            }
        }
    }

    async fn maintain(&mut self) {
        self.remove_cancelled_routes().await;
        if self.generation.is_none() && self.connecting.is_none() && !self.routes.is_empty() {
            self.start_connecting();
        }
        if self.generation.is_some() {
            self.flush_pending_opens().await;
        }
        if self.generation.is_none()
            && self.routes.is_empty()
            && let Some(mut connecting) = self.connecting.take()
        {
            connecting.task.stop().await;
        }
    }

    fn start_connecting(&mut self) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        let socket_path = self.socket_path.clone();
        let reconnect_policy = self.reconnect_policy;
        let event_tx = self.event_tx.clone();
        let task = tokio::spawn(async move {
            match connect_client_generation(
                &socket_path,
                reconnect_policy,
                generation,
                event_tx.clone(),
            )
            .await
            {
                Ok(connected) => {
                    let _ignored = event_tx.send(ClientIoEvent::Connected(connected)).await;
                }
                Err(terminal) => {
                    let _ignored = event_tx
                        .send(ClientIoEvent::ConnectFailed {
                            generation,
                            terminal,
                        })
                        .await;
                }
            }
        });
        self.connecting = Some(ConnectingGeneration {
            id: generation,
            task: OwnedTask::new(task),
        });
    }

    async fn flush_pending_opens(&mut self) {
        while let Some(stream_id) = self.pending_opens.pop_front() {
            let Some(route) = self.routes.get_mut(&stream_id) else {
                continue;
            };
            if !matches!(route.phase, ClientStreamPhase::Queued) {
                continue;
            }
            let request = route.request.clone();
            route.phase = ClientStreamPhase::OpenWritten;
            let Some(writer_tx) = self
                .generation
                .as_ref()
                .map(|generation| generation.writer_tx.clone())
            else {
                return;
            };
            if writer_tx
                .send(ClientMuxFrame::Open {
                    stream_id,
                    request: Box::new(request),
                })
                .await
                .is_err()
            {
                self.reconnect_generation(Arc::from("local session writer stopped"))
                    .await;
                return;
            }
        }
    }

    async fn remove_cancelled_routes(&mut self) {
        let cancelled = self
            .routes
            .iter()
            .filter_map(|(stream_id, route)| {
                route
                    .lease
                    .cancelled
                    .load(Ordering::Acquire)
                    .then_some(*stream_id)
            })
            .collect::<Vec<_>>();
        for stream_id in cancelled {
            self.cancel_route(stream_id).await;
        }
    }

    async fn cancel_route(&mut self, stream_id: StreamId) {
        let Some(route) = self.routes.remove(&stream_id) else {
            return;
        };
        let written = !matches!(route.phase, ClientStreamPhase::Queued);
        finish_client_route(
            route,
            RouteTerminal::Transport(Arc::from("local handler open was cancelled")),
        );
        if written {
            self.write_cancel(stream_id).await;
        }
    }

    async fn write_cancel(&mut self, stream_id: StreamId) {
        let Some(writer_tx) = self
            .generation
            .as_ref()
            .map(|generation| generation.writer_tx.clone())
        else {
            return;
        };
        if writer_tx
            .send(ClientMuxFrame::Cancel { stream_id })
            .await
            .is_err()
        {
            self.reconnect_generation(Arc::from("local session writer stopped"))
                .await;
        }
    }

    async fn reconnect_generation(&mut self, reason: Arc<str>) {
        if let Some(generation) = self.generation.take() {
            generation.stop().await;
        }
        self.queue_routes_for_reconnect(&reason);
    }

    fn queue_routes_for_reconnect(&mut self, reason: &Arc<str>) {
        self.pending_opens.clear();
        let mut stream_ids = self.routes.keys().copied().collect::<Vec<_>>();
        stream_ids.sort_unstable_by_key(|stream_id| stream_id.0);
        for stream_id in stream_ids {
            if let Some(route) = self.routes.get_mut(&stream_id) {
                let reopening = route.reply.is_none();
                if reopening && !route.reopening {
                    let _ignored = route.frames.try_send(MuxRouteEvent::Reconnecting {
                        reason: Arc::clone(reason),
                    });
                }
                route.reopening = reopening;
                route.phase = ClientStreamPhase::Queued;
            }
            self.pending_opens.push_back(stream_id);
        }
    }

    async fn fail_generation(&mut self, terminal: RouteTerminal) {
        if let Some(generation) = self.generation.take() {
            generation.stop().await;
        }
        self.fail_routes(&terminal);
    }

    fn fail_routes(&mut self, terminal: &RouteTerminal) {
        self.pending_opens.clear();
        for (_, route) in self.routes.drain() {
            finish_client_route(route, terminal.clone());
        }
    }

    async fn shutdown(&mut self) {
        if let Some(mut connecting) = self.connecting.take() {
            connecting.task.stop().await;
        }
        if let Some(generation) = self.generation.take() {
            generation.stop().await;
        }
        self.fail_routes(&RouteTerminal::Transport(Arc::from(
            "local session multiplexer stopped",
        )));
    }
}

fn finish_client_route(mut route: ClientRoute, terminal: RouteTerminal) {
    route.terminal.send_replace(Some(terminal.clone()));
    if let Some(reply) = route.reply.take() {
        let _ignored = reply.send(Err(terminal.into_error()));
    }
}

async fn connect_client_generation(
    socket_path: &Path,
    reconnect_policy: ReconnectPolicy,
    generation: u64,
    event_tx: mpsc::Sender<ClientIoEvent>,
) -> Result<ClientGeneration, RouteTerminal> {
    let mut stream = connect_local_peer(socket_path, reconnect_policy).await;
    write_frame(
        &mut stream,
        &Envelope::new(LocalConnectionHello::SessionMux {
            version: LOCAL_SESSION_MUX_VERSION,
        }),
    )
    .await
    .map_err(|error| RouteTerminal::Transport(Arc::from(error.to_string())))?;
    let ready: Envelope<ServerMuxFrame> = read_frame(&mut stream)
        .await
        .map_err(|error| RouteTerminal::Transport(Arc::from(error.to_string())))?;
    let ready = ready
        .into_current()
        .map_err(|error| RouteTerminal::Protocol(Arc::from(error.to_string())))?;
    match ready {
        ServerMuxFrame::Ready {
            version,
            max_streams,
        } if version == LOCAL_SESSION_MUX_VERSION
            && usize::try_from(max_streams).ok() == Some(MAX_LOGICAL_STREAMS) => {}
        ServerMuxFrame::Ready { version, .. } => {
            return Err(RouteTerminal::Protocol(Arc::from(format!(
                "unsupported local session mux version {version}; expected {LOCAL_SESSION_MUX_VERSION}"
            ))));
        }
        _ => {
            return Err(RouteTerminal::Protocol(Arc::from(
                "local session server did not begin with a ready frame",
            )));
        }
    }

    let (mut reader, mut writer) = stream.into_split();
    let (writer_tx, mut writer_rx) = mpsc::channel(CONTROL_CAPACITY);
    let reader_events = event_tx.clone();
    let reader_task = tokio::spawn(async move {
        loop {
            let result = async {
                let envelope: Envelope<ServerMuxFrame> = read_frame(&mut reader).await?;
                envelope
                    .into_current()
                    .map_err(|error| LocalPeerError::Protocol(error.to_string()))
            }
            .await;
            match result {
                Ok(frame) => {
                    if reader_events
                        .send(ClientIoEvent::Frame { generation, frame })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    let _ignored = reader_events
                        .send(ClientIoEvent::Lost {
                            generation,
                            terminal: RouteTerminal::Transport(Arc::from(error.to_string())),
                        })
                        .await;
                    return;
                }
            }
        }
    });
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = writer_rx.recv().await {
            if let Err(error) = write_frame(&mut writer, &Envelope::new(frame)).await {
                let _ignored = event_tx
                    .send(ClientIoEvent::Lost {
                        generation,
                        terminal: RouteTerminal::Transport(Arc::from(error.to_string())),
                    })
                    .await;
                return;
            }
        }
    });
    Ok(ClientGeneration {
        id: generation,
        writer_tx,
        reader: OwnedTask::new(reader_task),
        writer: OwnedTask::new(writer_task),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use myko_federation::{
        AuthorityPresentation, CommandId, CommandRequest, CommandStateEntry, CommandStateUpdate,
        CommandWatchRequest, LogPosition, Node, PrincipalId, ResourceClaim, ResourceClaimKind,
        ScopeId, ServiceId,
    };

    fn admitted_command(
        node: &Node,
        principal: &PrincipalId,
        scope: &ScopeId,
    ) -> Result<CommandStateEntry, myko_federation::NodeError> {
        let command = node
            .admit(CommandRequest {
                id: CommandId::new(),
                service_id: ServiceId::new("test.local"),
                scope_id: scope.clone(),
                principal_id: principal.clone(),
                authority: AuthorityPresentation::direct_node(principal.clone()),
                resource_claims: vec![ResourceClaim::scope(
                    scope.clone(),
                    ResourceClaimKind::Primary,
                )],
                application_capabilities: Vec::new(),
                arguments_digest: None,
                command_type: "test.command".to_owned(),
                payload: Vec::new(),
            })?
            .snapshot()
            .clone();
        Ok(CommandStateEntry {
            admitted_at: command.updated_at.sequence,
            last_changed_at: command.updated_at.sequence,
            command,
        })
    }

    #[test]
    fn command_catalog_batch_advances_reopen_cursor_once() -> Result<(), myko_federation::NodeError>
    {
        let node = Node::in_memory();
        let principal = PrincipalId::new("node:local-client");
        let scope = ScopeId::new("scope:local-catalog");
        let through = LogPosition::new(42);
        let mut request = PeerRequest::WatchCommands {
            request: CommandWatchRequest {
                serving_node: node.node_id(),
                source_node: node.node_id(),
                service_id: ServiceId::new("test.local"),
                scope_id: scope.clone(),
                command_type: "test.command".to_owned(),
                after: None,
            },
        };
        let update = CommandStateUpdate {
            through,
            commands: vec![
                admitted_command(&node, &principal, &scope)?,
                admitted_command(&node, &principal, &scope)?,
            ],
        };

        ClientRoute::observe_command_progress(
            &mut request,
            &PeerFrame::CommandUpdate {
                update: Box::new(update),
            },
        );

        if !matches!(
            request,
            PeerRequest::WatchCommands { request } if request.after == Some(through)
        ) {
            return Err(myko_federation::NodeError::InvalidCommandState(
                "batch did not advance the reconnect cursor".to_owned(),
            ));
        }
        Ok(())
    }
}
