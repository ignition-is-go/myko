use std::collections::{HashMap, VecDeque};

use tokio::{
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
};

use super::protocol::{
    CONTROL_CAPACITY, ClientMuxFrame, CloseReason, LOCAL_SESSION_MUX_VERSION, LocalConnectionHello,
    MAX_LOGICAL_STREAMS, OpenReject, ServerMuxFrame, StreamId,
};
use crate::{
    Envelope, FederatedSession, LocalPeerError, Principal,
    transport::{read_frame, write_frame},
};

enum ServerLogicalPhase {
    Opening,
    Opened,
}

struct ServerLogicalStream {
    phase: ServerLogicalPhase,
    abort: AbortHandle,
}

enum ServerEvent {
    Opened {
        stream_id: StreamId,
        frames: myko::server::NodeFrameStream,
    },
    Output {
        stream_id: StreamId,
        frame: ServerMuxFrame,
        written: oneshot::Sender<Result<(), String>>,
    },
    Finished {
        stream_id: StreamId,
    },
}

struct ServerWriteJob {
    frame: ServerMuxFrame,
    written: Option<oneshot::Sender<Result<(), String>>>,
}

struct PendingServerOutput {
    frame: ServerMuxFrame,
    written: oneshot::Sender<Result<(), String>>,
}

struct IoTask(Option<JoinHandle<()>>);

impl IoTask {
    const fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    async fn stop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ignored = task.await;
        }
    }
}

impl Drop for IoTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

struct ServerMux {
    sessions: FederatedSession,
    principal: Principal,
    writer_tx: mpsc::Sender<ServerWriteJob>,
    event_tx: mpsc::Sender<ServerEvent>,
    streams: HashMap<StreamId, ServerLogicalStream>,
    tasks: JoinSet<()>,
    pending: HashMap<StreamId, PendingServerOutput>,
    ready_ids: VecDeque<StreamId>,
}

impl ServerMux {
    fn new(
        sessions: FederatedSession,
        principal: Principal,
        writer_tx: mpsc::Sender<ServerWriteJob>,
        event_tx: mpsc::Sender<ServerEvent>,
    ) -> Self {
        Self {
            sessions,
            principal,
            writer_tx,
            event_tx,
            streams: HashMap::new(),
            tasks: JoinSet::new(),
            pending: HashMap::new(),
            ready_ids: VecDeque::new(),
        }
    }

    async fn run(
        &mut self,
        mut input_rx: mpsc::Receiver<ClientMuxFrame>,
        mut event_rx: mpsc::Receiver<ServerEvent>,
        mut failure_rx: mpsc::Receiver<String>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), LocalPeerError> {
        loop {
            tokio::select! {
                input = input_rx.recv() => match input {
                    Some(input) => self.handle_input(input).await?,
                    None => return Ok(()),
                },
                event = event_rx.recv() => match event {
                    Some(event) => self.handle_event(event).await?,
                    None => return Ok(()),
                },
                permit = self.writer_tx.clone().reserve_owned(), if !self.ready_ids.is_empty() => {
                    let permit = permit.map_err(|_| writer_stopped())?;
                    self.write_next_ready(permit);
                }
                failure = failure_rx.recv() => {
                    return Err(LocalPeerError::Protocol(
                        failure.unwrap_or_else(|| "local session mux I/O stopped".to_owned()),
                    ));
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(Err(error)) = joined
                        && !error.is_cancelled()
                    {
                        return Err(LocalPeerError::Protocol(format!(
                            "local session stream task failed: {error}"
                        )));
                    }
                }
            }
        }
    }

    async fn handle_input(&mut self, input: ClientMuxFrame) -> Result<(), LocalPeerError> {
        match input {
            ClientMuxFrame::Open { stream_id, request } => {
                let request = *request;
                if let Some(reason) = self.reject_open(stream_id) {
                    return self.reject(stream_id, reason).await;
                }
                let sessions = self.sessions.clone();
                let principal = self.principal.clone();
                let events = self.event_tx.clone();
                let abort = self.tasks.spawn(async move {
                    let frames = sessions.open_authenticated(principal, request).await;
                    let _ignored = events.send(ServerEvent::Opened { stream_id, frames }).await;
                });
                self.streams.insert(
                    stream_id,
                    ServerLogicalStream {
                        phase: ServerLogicalPhase::Opening,
                        abort,
                    },
                );
                Ok(())
            }
            ClientMuxFrame::Cancel { stream_id } => self.cancel(stream_id).await,
        }
    }

    fn reject_open(&self, stream_id: StreamId) -> Option<OpenReject> {
        if !stream_id.is_valid() {
            return Some(OpenReject::InvalidRequest(
                "local session stream ID must be nonzero".to_owned(),
            ));
        }
        if self.streams.contains_key(&stream_id) {
            return Some(OpenReject::DuplicateId);
        }
        if self.streams.len() >= MAX_LOGICAL_STREAMS {
            return Some(OpenReject::Capacity);
        }
        None
    }

    async fn reject(&self, stream_id: StreamId, reason: OpenReject) -> Result<(), LocalPeerError> {
        send_control(
            &self.writer_tx,
            ServerMuxFrame::Rejected { stream_id, reason },
        )
        .await
    }

    async fn cancel(&mut self, stream_id: StreamId) -> Result<(), LocalPeerError> {
        let Some(stream) = self.streams.remove(&stream_id) else {
            return Ok(());
        };
        stream.abort.abort();
        if let Some(output) = self.pending.remove(&stream_id) {
            let _ignored = output
                .written
                .send(Err("local session stream was cancelled".to_owned()));
        }
        send_control(
            &self.writer_tx,
            ServerMuxFrame::Closed {
                stream_id,
                reason: CloseReason::Cancelled,
            },
        )
        .await
    }

    async fn handle_event(&mut self, event: ServerEvent) -> Result<(), LocalPeerError> {
        match event {
            ServerEvent::Opened { stream_id, frames } => {
                let Some(stream) = self.streams.get(&stream_id) else {
                    return Ok(());
                };
                if !matches!(stream.phase, ServerLogicalPhase::Opening) {
                    return Ok(());
                }
                send_control(&self.writer_tx, ServerMuxFrame::Opened { stream_id }).await?;
                let abort = self
                    .tasks
                    .spawn(pump_stream(stream_id, frames, self.event_tx.clone()));
                self.streams.insert(
                    stream_id,
                    ServerLogicalStream {
                        phase: ServerLogicalPhase::Opened,
                        abort,
                    },
                );
            }
            ServerEvent::Output {
                stream_id,
                frame,
                written,
            } => {
                if self.streams.contains_key(&stream_id) && !self.pending.contains_key(&stream_id) {
                    self.pending
                        .insert(stream_id, PendingServerOutput { frame, written });
                    self.ready_ids.push_back(stream_id);
                } else {
                    let _ignored =
                        written.send(Err("local session stream was cancelled".to_owned()));
                }
            }
            ServerEvent::Finished { stream_id } => {
                self.streams.remove(&stream_id);
                self.pending.remove(&stream_id);
            }
        }
        Ok(())
    }

    fn write_next_ready(&mut self, permit: mpsc::OwnedPermit<ServerWriteJob>) {
        while let Some(stream_id) = self.ready_ids.pop_front() {
            let Some(output) = self.pending.remove(&stream_id) else {
                continue;
            };
            permit.send(ServerWriteJob {
                frame: output.frame,
                written: Some(output.written),
            });
            break;
        }
    }

    async fn stop(&mut self) {
        for (_, stream) in self.streams.drain() {
            stream.abort.abort();
        }
        for (_, output) in self.pending.drain() {
            let _ignored = output
                .written
                .send(Err("local session mux stopped".to_owned()));
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

pub async fn serve_session_mux(
    mut stream: UnixStream,
    sessions: FederatedSession,
    principal: Principal,
    shutdown: watch::Receiver<bool>,
    hello: LocalConnectionHello,
) -> Result<(), LocalPeerError> {
    write_ready(&mut stream, hello).await?;

    let (reader, writer) = stream.into_split();
    let (input_tx, input_rx) = mpsc::channel(CONTROL_CAPACITY);
    let (failure_tx, failure_rx) = mpsc::channel::<String>(2);
    let mut reader_task = spawn_reader(reader, input_tx, failure_tx.clone());
    let (writer_tx, writer_rx) = mpsc::channel::<ServerWriteJob>(CONTROL_CAPACITY);
    let mut writer_task = spawn_writer(writer, writer_rx, failure_tx);
    let (event_tx, event_rx) = mpsc::channel(CONTROL_CAPACITY);
    let mut mux = ServerMux::new(sessions, principal, writer_tx, event_tx);
    let result = mux.run(input_rx, event_rx, failure_rx, shutdown).await;
    mux.stop().await;
    reader_task.stop().await;
    writer_task.stop().await;
    result
}

async fn write_ready(
    stream: &mut UnixStream,
    hello: LocalConnectionHello,
) -> Result<(), LocalPeerError> {
    let LocalConnectionHello::SessionMux { version } = hello;
    if version != LOCAL_SESSION_MUX_VERSION {
        return Err(LocalPeerError::Protocol(format!(
            "unsupported local session mux version {version}; expected {LOCAL_SESSION_MUX_VERSION}"
        )));
    }
    let max_streams = u32::try_from(MAX_LOGICAL_STREAMS).map_err(|error| {
        LocalPeerError::Protocol(format!("local handler capacity is invalid: {error}"))
    })?;
    write_frame(
        stream,
        &Envelope::new(ServerMuxFrame::Ready {
            version: LOCAL_SESSION_MUX_VERSION,
            max_streams,
        }),
    )
    .await
}

fn spawn_reader(
    mut reader: OwnedReadHalf,
    input_tx: mpsc::Sender<ClientMuxFrame>,
    failure_tx: mpsc::Sender<String>,
) -> IoTask {
    IoTask::new(tokio::spawn(async move {
        loop {
            let result = async {
                let envelope: Envelope<ClientMuxFrame> = read_frame(&mut reader).await?;
                envelope
                    .into_current()
                    .map_err(|error| LocalPeerError::Protocol(error.to_string()))
            }
            .await;
            match result {
                Ok(frame) => {
                    if input_tx.send(frame).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ignored = failure_tx.send(error.to_string()).await;
                    return;
                }
            }
        }
    }))
}

fn spawn_writer(
    mut writer: OwnedWriteHalf,
    mut writer_rx: mpsc::Receiver<ServerWriteJob>,
    failure_tx: mpsc::Sender<String>,
) -> IoTask {
    IoTask::new(tokio::spawn(async move {
        while let Some(job) = writer_rx.recv().await {
            let result = write_frame(&mut writer, &Envelope::new(job.frame))
                .await
                .map_err(|error| error.to_string());
            if let Some(written) = job.written {
                let _ignored = written.send(result.clone());
            }
            if let Err(error) = result {
                let _ignored = failure_tx.send(error).await;
                return;
            }
        }
    }))
}

async fn send_control(
    writer_tx: &mpsc::Sender<ServerWriteJob>,
    frame: ServerMuxFrame,
) -> Result<(), LocalPeerError> {
    writer_tx
        .send(ServerWriteJob {
            frame,
            written: None,
        })
        .await
        .map_err(|_| LocalPeerError::Protocol("local session mux writer stopped".to_owned()))
}

fn writer_stopped() -> LocalPeerError {
    LocalPeerError::Protocol("local session mux writer stopped".to_owned())
}

async fn pump_stream(
    stream_id: StreamId,
    mut frames: myko::server::NodeFrameStream,
    events: mpsc::Sender<ServerEvent>,
) {
    loop {
        let (frame, finished) = frames.recv().await.map_or_else(
            || {
                (
                    ServerMuxFrame::Closed {
                        stream_id,
                        reason: CloseReason::Completed,
                    },
                    true,
                )
            },
            |frame| (ServerMuxFrame::Data { stream_id, frame }, false),
        );
        let (written, receipt) = oneshot::channel();
        if events
            .send(ServerEvent::Output {
                stream_id,
                frame,
                written,
            })
            .await
            .is_err()
        {
            return;
        }
        if !matches!(receipt.await, Ok(Ok(()))) {
            return;
        }
        if finished {
            let _ignored = events.send(ServerEvent::Finished { stream_id }).await;
            return;
        }
    }
}
