use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use hyphae::{Cell, CellImmutable, CellMutable, Gettable, Mutable};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig};
use url::Url;

use crate::{SocketConnectionStatus, SocketTransport, WsFrame};

const OUTGOING_QUEUE_CAPACITY: usize = 1024;
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

struct OutgoingQueue {
    tx: flume::Sender<WsFrame>,
    rx: flume::Receiver<WsFrame>,
}

impl OutgoingQueue {
    fn new() -> Self {
        let (tx, rx) = flume::bounded(OUTGOING_QUEUE_CAPACITY);
        Self { tx, rx }
    }
}

#[derive(Clone)]
struct DesiredConnection {
    generation: u64,
    addr: Option<String>,
    outgoing_rx: Option<flume::Receiver<WsFrame>>,
    credentials: Option<Arc<dyn myko_auth::CredentialProvider>>,
}

struct ConnectionControl {
    generation: u64,
    addr: Option<String>,
    outgoing: OutgoingQueue,
    credentials: Option<Arc<dyn myko_auth::CredentialProvider>>,
}

enum DelayOutcome {
    Changed,
    Elapsed,
    Closed,
}

#[derive(Clone, PartialEq, Eq)]
struct ObservedConnection {
    generation: u64,
    attempt: u64,
    status: SocketConnectionStatus,
}

struct StatusPublisher {
    latest: Arc<Mutex<ObservedConnection>>,
    wake: flume::Sender<()>,
}

impl StatusPublisher {
    #[allow(clippy::expect_used)]
    fn new(status: Cell<SocketConnectionStatus, CellMutable>) -> (Self, thread::JoinHandle<()>) {
        let latest = Arc::new(Mutex::new(ObservedConnection {
            generation: 0,
            attempt: 0,
            status: SocketConnectionStatus::Idle,
        }));
        let (wake, pending) = flume::bounded(1);
        let observed = Arc::clone(&latest);
        let worker = thread::Builder::new()
            .name("autosocket-status".into())
            .spawn(move || {
                let mut previous = ObservedConnection {
                    generation: 0,
                    attempt: 0,
                    status: status.get(),
                };
                while pending.recv().is_ok() {
                    let current = observed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    if current == previous {
                        continue;
                    }
                    if matches!(previous.status, SocketConnectionStatus::Connected(_))
                        && (previous.generation, previous.attempt)
                            != (current.generation, current.attempt)
                        && matches!(
                            current.status,
                            SocketConnectionStatus::Connected(_)
                                | SocketConnectionStatus::Connecting(_)
                                | SocketConnectionStatus::Reconnecting(_)
                        )
                    {
                        status.set(SocketConnectionStatus::Disconnected);
                    }
                    status.set(current.status.clone());
                    previous = current;
                }
            })
            .expect("spawn autosocket status publisher");
        (Self { latest, wake }, worker)
    }

    fn publish(&self, generation: u64, attempt: u64, status: SocketConnectionStatus) {
        *self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ObservedConnection {
            generation,
            attempt,
            status,
        };
        let _ = self.wake.try_send(());
    }
}

pub struct AutoReconnectSocket {
    intended_status: Cell<SocketConnectionStatus, CellMutable>,
    actual_status: Cell<SocketConnectionStatus, CellMutable>,
    control: Arc<Mutex<ConnectionControl>>,
    desired_tx: watch::Sender<DesiredConnection>,
    _incoming_tx: flume::Sender<WsFrame>,
    incoming_rx: flume::Receiver<WsFrame>,
}

impl Default for AutoReconnectSocket {
    fn default() -> Self {
        Self::new()
    }
}

fn frame_to_message(frame: WsFrame) -> Message {
    match frame {
        WsFrame::Text(text) => Message::Text(text.into()),
        WsFrame::Binary(bytes) => Message::Binary(bytes.into()),
    }
}

impl SocketTransport for AutoReconnectSocket {
    fn set_credential_provider(
        &self,
        provider: Option<Arc<dyn myko_auth::CredentialProvider>>,
    ) -> Result<(), String> {
        let addr = {
            let mut control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            control.credentials = provider;
            control.addr.clone()
        };
        self.set_addr(None);
        self.set_addr(addr);
        Ok(())
    }

    fn set_addr(&self, addr: Option<String>) {
        self.set_addr(addr);
    }

    fn close(&self) {
        self.close();
    }

    fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.intended_status.clone().lock()
    }

    fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.actual_status.clone().lock()
    }

    fn send(&self, frame: WsFrame) -> Result<(), String> {
        let tx = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outgoing
            .tx
            .clone();
        tx.send(frame).map_err(|error| error.to_string())
    }

    fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming_rx.clone()
    }
}

impl AutoReconnectSocket {
    #[must_use]
    pub fn new() -> Self {
        Self::with_auto_reconnect_and_limits(true, 64 * 1024 * 1024, 64 * 1024 * 1024)
    }

    #[must_use]
    pub fn with_auto_reconnect(auto_reconnect: bool) -> Self {
        Self::with_auto_reconnect_and_limits(auto_reconnect, 64 * 1024 * 1024, 64 * 1024 * 1024)
    }

    #[must_use]
    pub fn with_auto_reconnect_and_limits(
        auto_reconnect: bool,
        max_message_size_bytes: usize,
        max_frame_size_bytes: usize,
    ) -> Self {
        let outgoing = OutgoingQueue::new();
        let initial = DesiredConnection {
            generation: 0,
            addr: None,
            outgoing_rx: None,
            credentials: None,
        };
        let (desired_tx, desired_rx) = watch::channel(initial);
        let (incoming_tx, incoming_rx) = flume::unbounded();
        let intended_status =
            Cell::new(SocketConnectionStatus::Idle).with_name("autosocket.intended_status");
        let actual_status =
            Cell::new(SocketConnectionStatus::Idle).with_name("autosocket.actual_status");
        let control = Arc::new(Mutex::new(ConnectionControl {
            generation: 0,
            addr: None,
            outgoing,
            credentials: None,
        }));

        Self::start_driver(
            desired_rx,
            actual_status.clone(),
            incoming_tx.clone(),
            Arc::clone(&control),
            auto_reconnect,
            max_message_size_bytes,
            max_frame_size_bytes,
        );

        Self {
            intended_status,
            actual_status,
            control,
            desired_tx,
            _incoming_tx: incoming_tx,
            incoming_rx,
        }
    }

    #[must_use]
    pub fn get_status(&self) -> SocketConnectionStatus {
        self.actual_status.get()
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let current_status = self.actual_status.get();
        let generation = {
            let mut control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if control.addr == addr
                && matches!(
                    current_status,
                    SocketConnectionStatus::Connected(ref current)
                        | SocketConnectionStatus::Connecting(ref current)
                        | SocketConnectionStatus::Reconnecting(ref current)
                        if Some(current) == addr.as_ref()
                )
            {
                return;
            }
            control.generation = control.generation.wrapping_add(1);
            control.addr.clone_from(&addr);
            control.outgoing = OutgoingQueue::new();
            self.desired_tx.send_replace(DesiredConnection {
                generation: control.generation,
                addr: addr.clone(),
                outgoing_rx: addr.as_ref().map(|_| control.outgoing.rx.clone()),
                credentials: control.credentials.clone(),
            });
            control.generation
        };

        let mut published = (generation, addr);
        loop {
            self.intended_status.set(
                published
                    .1
                    .clone()
                    .map_or(SocketConnectionStatus::Idle, |addr| {
                        SocketConnectionStatus::Connected(addr)
                    }),
            );
            let control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if control.generation == published.0 {
                break;
            }
            published = (control.generation, control.addr.clone());
        }
    }

    pub fn close(&self) {
        self.set_addr(None);
    }

    #[allow(clippy::expect_used)]
    fn start_driver(
        desired_rx: watch::Receiver<DesiredConnection>,
        actual_status: Cell<SocketConnectionStatus, CellMutable>,
        incoming_tx: flume::Sender<WsFrame>,
        control: Arc<Mutex<ConnectionControl>>,
        auto_reconnect: bool,
        max_message_size_bytes: usize,
        max_frame_size_bytes: usize,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("autosocket-io")
            .build()
            .expect("build autosocket runtime");
        let (publisher, status_worker) = StatusPublisher::new(actual_status);
        thread::Builder::new()
            .name("autosocket-driver".into())
            .spawn(move || {
                runtime.block_on(Self::run_driver(
                    desired_rx,
                    publisher,
                    incoming_tx,
                    control,
                    auto_reconnect,
                    max_message_size_bytes,
                    max_frame_size_bytes,
                ));
                let _ = status_worker.join();
            })
            .expect("spawn autosocket driver");
    }

    async fn run_driver(
        mut desired_rx: watch::Receiver<DesiredConnection>,
        actual_status: StatusPublisher,
        incoming_tx: flume::Sender<WsFrame>,
        control: Arc<Mutex<ConnectionControl>>,
        auto_reconnect: bool,
        max_message_size_bytes: usize,
        max_frame_size_bytes: usize,
    ) {
        loop {
            let desired = desired_rx.borrow_and_update().clone();
            let Some(addr) = desired.addr.clone() else {
                actual_status.publish(desired.generation, 0, SocketConnectionStatus::Idle);
                if desired_rx.changed().await.is_err() {
                    return;
                }
                continue;
            };

            if Self::run_generation(
                desired,
                addr,
                &mut desired_rx,
                &actual_status,
                &incoming_tx,
                &control,
                auto_reconnect,
                max_message_size_bytes,
                max_frame_size_bytes,
            )
            .await
            {
                return;
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_generation(
        desired: DesiredConnection,
        addr: String,
        desired_rx: &mut watch::Receiver<DesiredConnection>,
        actual_status: &StatusPublisher,
        incoming_tx: &flume::Sender<WsFrame>,
        control: &Arc<Mutex<ConnectionControl>>,
        auto_reconnect: bool,
        max_message_size_bytes: usize,
        max_frame_size_bytes: usize,
    ) -> bool {
        let mut attempt = 0_u64;
        let mut connection_attempt = 0_u64;
        loop {
            attempt = attempt.saturating_add(1);
            connection_attempt = connection_attempt.saturating_add(1);
            let publish =
                |status| actual_status.publish(desired.generation, connection_attempt, status);
            publish(if attempt == 1 {
                SocketConnectionStatus::Connecting(addr.clone())
            } else {
                SocketConnectionStatus::Reconnecting(addr.clone())
            });

            let url = match Self::parse_websocket_url(&addr) {
                Ok(url) => url,
                Err(()) if auto_reconnect => {
                    match Self::wait_for_change_or_delay(desired_rx).await {
                        DelayOutcome::Changed => return false,
                        DelayOutcome::Closed => return true,
                        DelayOutcome::Elapsed => {}
                    }
                    continue;
                }
                Err(()) => {
                    publish(SocketConnectionStatus::Disconnected);
                    return Self::wait_for_change(desired_rx).await;
                }
            };
            let connect = async {
                let mut request = url.as_str().into_client_request().map_err(|_| ())?;
                if let Some(provider) = &desired.credentials {
                    let credentials = provider.credentials().await.map_err(|_| ())?;
                    let bearer = crate::connection_bearer(credentials).map_err(|_| ())?;
                    let mut header =
                        tungstenite::http::HeaderValue::from_str(&format!("Bearer {bearer}"))
                            .map_err(|_| ())?;
                    header.set_sensitive(true);
                    request
                        .headers_mut()
                        .insert(tungstenite::http::header::AUTHORIZATION, header);
                }
                let mut config = WebSocketConfig::default();
                // Frame reads clear this scratch space even when the async stream is pending.
                // Keep small-message polling cheap; message and frame limits remain independent.
                config.read_buffer_size = 16 * 1024;
                config.max_message_size = Some(max_message_size_bytes);
                config.max_frame_size = Some(max_frame_size_bytes);
                tokio_tungstenite::connect_async_with_config(request, Some(config), true)
                    .await
                    .map_err(|_| ())
            };
            let websocket = tokio::select! {
                biased;
                changed = desired_rx.changed() => {
                    return changed.is_err();
                }
                result = connect => if let Ok((websocket, _)) = result {
                    websocket
                } else {
                    error!("Failed to establish WebSocket connection to {url} (attempt {attempt})");
                    publish(SocketConnectionStatus::Disconnected);
                    if !auto_reconnect {
                        return Self::wait_for_change(desired_rx).await;
                    }
                    match Self::wait_for_change_or_delay(desired_rx).await {
                        DelayOutcome::Changed => return false,
                        DelayOutcome::Closed => return true,
                        DelayOutcome::Elapsed => {}
                    }
                    continue;
                }
            };

            if desired_rx.borrow().generation != desired.generation {
                return false;
            }
            let cancel = CancellationToken::new();
            let Some(outgoing_rx) = desired.outgoing_rx.clone() else {
                return false;
            };
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let mut session = tokio::spawn(Self::run_session(
                websocket,
                outgoing_rx,
                incoming_tx.clone(),
                Arc::clone(control),
                desired.generation,
                cancel.child_token(),
                ready_tx,
            ));
            tokio::select! {
                biased;
                changed = desired_rx.changed() => {
                    cancel.cancel();
                    let _ = session.await;
                    return changed.is_err();
                }
                ready = ready_rx => {
                    if ready.is_err() {
                        let _ = session.await;
                        publish(SocketConnectionStatus::Disconnected);
                        continue;
                    }
                }
            }
            if desired_rx.borrow().generation != desired.generation {
                cancel.cancel();
                let _ = session.await;
                return false;
            }
            attempt = 0;
            publish(SocketConnectionStatus::Connected(addr.clone()));
            info!("Autoreconnect socket connected to {url}");

            tokio::select! {
                biased;
                changed = desired_rx.changed() => {
                    cancel.cancel();
                    let _ = session.await;
                    return changed.is_err();
                }
                result = &mut session => {
                    if let Err(error) = result {
                        error!("WebSocket session task failed: {error}");
                    }
                }
            }
            publish(SocketConnectionStatus::Disconnected);
            if !auto_reconnect {
                return Self::wait_for_change(desired_rx).await;
            }
            match Self::wait_for_change_or_delay(desired_rx).await {
                DelayOutcome::Changed => return false,
                DelayOutcome::Closed => return true,
                DelayOutcome::Elapsed => {}
            }
        }
    }

    // The generation check and inbound enqueue share the control lock so a
    // readdress cannot return between them and expose a stale frame.
    #[allow(clippy::significant_drop_tightening)]
    async fn run_session<S>(
        websocket: tokio_tungstenite::WebSocketStream<S>,
        outgoing_rx: flume::Receiver<WsFrame>,
        incoming_tx: flume::Sender<WsFrame>,
        control: Arc<Mutex<ConnectionControl>>,
        generation: u64,
        cancel: CancellationToken,
        ready: tokio::sync::oneshot::Sender<()>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut writer, mut reader) = websocket.split();
        let read_cancel = cancel.child_token();
        let mut reader_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = read_cancel.cancelled() => return,
                    message = reader.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            let control = control
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if control.generation != generation {
                                return;
                            }
                            if incoming_tx.send(WsFrame::Text(text.to_string())).is_err() {
                                return;
                            }
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            let control = control
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if control.generation != generation {
                                return;
                            }
                            if incoming_tx.send(WsFrame::Binary(bytes.to_vec())).is_err() {
                                return;
                            }
                        }
                        Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                        Some(Ok(_)) => {}
                    }
                }
            }
        });
        let write_cancel = cancel.child_token();
        let mut writer_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = write_cancel.cancelled() => return,
                    frame = outgoing_rx.recv_async() => match frame {
                        Ok(frame) => {
                            tokio::select! {
                                biased;
                                () = write_cancel.cancelled() => return,
                                result = writer.send(frame_to_message(frame)) => {
                                    if result.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(_) => return,
                    }
                }
            }
        });
        let _ = ready.send(());

        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                reader_task.abort();
                writer_task.abort();
                let _ = reader_task.await;
                let _ = writer_task.await;
            }
            _ = &mut reader_task => {
                cancel.cancel();
                writer_task.abort();
                let _ = writer_task.await;
            }
            _ = &mut writer_task => {
                cancel.cancel();
                reader_task.abort();
                let _ = reader_task.await;
            }
        }
    }

    async fn wait_for_change(desired_rx: &mut watch::Receiver<DesiredConnection>) -> bool {
        desired_rx.changed().await.is_err()
    }

    async fn wait_for_change_or_delay(
        desired_rx: &mut watch::Receiver<DesiredConnection>,
    ) -> DelayOutcome {
        tokio::select! {
            biased;
            changed = desired_rx.changed() => if changed.is_err() {
                DelayOutcome::Closed
            } else {
                DelayOutcome::Changed
            },
            () = tokio::time::sleep(RECONNECT_DELAY) => DelayOutcome::Elapsed,
        }
    }

    fn parse_websocket_url(addr: &str) -> Result<String, ()> {
        let mut url = match Url::parse(addr).or_else(|_| Url::parse(&format!("ws://{addr}"))) {
            Ok(url) => url,
            Err(error) => {
                error!("Could not parse URL: {error} for {addr}");
                return Err(());
            }
        };
        if url.scheme() != "ws" && url.scheme() != "wss" {
            let _ = url.set_scheme("ws");
        }
        Ok(url.to_string())
    }

    #[must_use]
    pub fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.intended_status.clone().lock()
    }

    #[must_use]
    pub fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.actual_status.clone().lock()
    }

    pub fn write_tx(&self) -> flume::Sender<WsFrame> {
        self.control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outgoing
            .tx
            .clone()
    }

    #[must_use]
    pub fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming_rx.clone()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic_in_result_fn,
        clippy::unwrap_used
    )]
    use std::{
        net::TcpListener,
        sync::{Arc, mpsc},
        thread::JoinHandle,
        time::{Duration, Instant},
    };

    use hyphae::{Signal, Watchable};

    use super::*;

    fn websocket_receiver() -> std::io::Result<(String, mpsc::Receiver<String>, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/myko", listener.local_addr()?);
        let (message_tx, message_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let Ok(mut websocket) = tungstenite::accept(stream) else {
                return;
            };
            if let Ok(Message::Text(text)) = websocket.read() {
                let _ = message_tx.send(text.to_string());
                thread::sleep(Duration::from_millis(100));
            }
        });
        Ok((address, message_rx, handle))
    }

    fn wait_until_connected(socket: &AutoReconnectSocket, address: &str) -> bool {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            if matches!(
                socket.get_status(),
                SocketConnectionStatus::Connected(ref current) if current == address
            ) {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    struct RotatingCredentials(std::sync::atomic::AtomicUsize);

    impl myko_auth::CredentialProvider for RotatingCredentials {
        fn credentials(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<myko_auth::Credentials, myko_auth::AuthError>>
        {
            let attempt = self
                .0
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .saturating_add(1);
            Box::pin(async move {
                Ok(myko_auth::Credentials {
                    bearer: Some(format!("token-{attempt}")),
                })
            })
        }
    }

    #[test]
    fn reconnect_obtains_credentials_before_each_authenticated_upgrade()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = format!("ws://{}/myko", listener.local_addr()?);
        let (headers_tx, headers_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut authorization = String::new();
                let mut socket = tungstenite::accept_hdr(
                    stream,
                    #[allow(clippy::result_large_err)]
                    |request: &tungstenite::handshake::server::Request, response| {
                        authorization = request
                            .headers()
                            .get("Authorization")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_owned();
                        Ok(response)
                    },
                )
                .unwrap();
                socket.close(None).unwrap();
                headers_tx.send(authorization).unwrap();
            }
        });
        let socket = AutoReconnectSocket::new();
        SocketTransport::set_credential_provider(
            &socket,
            Some(Arc::new(RotatingCredentials(
                std::sync::atomic::AtomicUsize::new(0),
            ))),
        )?;
        socket.set_addr(Some(addr));
        assert_eq!(
            headers_rx.recv_timeout(Duration::from_secs(4))?,
            "Bearer token-1"
        );
        assert_eq!(
            headers_rx.recv_timeout(Duration::from_secs(4))?,
            "Bearer token-2"
        );
        socket.close();
        server.join().unwrap();
        Ok(())
    }

    struct FailedCredentials;

    impl myko_auth::CredentialProvider for FailedCredentials {
        fn credentials(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<myko_auth::Credentials, myko_auth::AuthError>>
        {
            Box::pin(async { Err(myko_auth::AuthError::Unavailable) })
        }
    }

    #[test]
    fn credential_failure_never_opens_an_unauthenticated_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        SocketTransport::set_credential_provider(&socket, Some(Arc::new(FailedCredentials)))?;
        socket.set_addr(Some(format!("ws://{}/myko", listener.local_addr()?)));
        let started = Instant::now();
        while socket.get_status() != SocketConnectionStatus::Disconnected
            && started.elapsed() < Duration::from_secs(2)
        {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(socket.get_status(), SocketConnectionStatus::Disconnected);
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        socket.close();
        Ok(())
    }

    struct PendingCredentials {
        entered: Arc<std::sync::atomic::AtomicBool>,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    }

    struct CredentialAttemptGuard(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for CredentialAttemptGuard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl myko_auth::CredentialProvider for PendingCredentials {
        fn credentials(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<myko_auth::Credentials, myko_auth::AuthError>>
        {
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let guard = CredentialAttemptGuard(self.cancelled.clone());
            Box::pin(async move {
                let _guard = guard;
                std::future::pending().await
            })
        }
    }

    #[test]
    fn disconnect_cancels_pending_credential_acquisition() -> Result<(), Box<dyn std::error::Error>>
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        SocketTransport::set_credential_provider(
            &socket,
            Some(Arc::new(PendingCredentials {
                entered: entered.clone(),
                cancelled: cancelled.clone(),
            })),
        )?;
        socket.set_addr(Some(format!("ws://{}/myko", listener.local_addr()?)));
        let started = Instant::now();
        while !entered.load(std::sync::atomic::Ordering::SeqCst)
            && started.elapsed() < Duration::from_secs(2)
        {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
        socket.close();
        let started = Instant::now();
        while socket.get_status() != SocketConnectionStatus::Idle
            && started.elapsed() < Duration::from_secs(2)
        {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(socket.get_status(), SocketConnectionStatus::Idle);
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        Ok(())
    }

    #[test]
    fn address_change_rotates_the_outgoing_queue() {
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        let stale_tx = socket.write_tx();

        socket.set_addr(None);

        assert!(stale_tx.send(WsFrame::Text("stale".into())).is_err());

        let current_tx = socket.write_tx();
        let current_rx = socket
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outgoing
            .rx
            .clone();
        assert!(current_tx.send(WsFrame::Text("current".into())).is_ok());
        assert!(matches!(
            current_rx.recv().ok(),
            Some(WsFrame::Text(text)) if text == "current"
        ));
    }

    #[test]
    fn address_change_sends_connected_callbacks_only_to_the_new_socket() -> std::io::Result<()> {
        let (first_address, _first_messages, _first_server) = websocket_receiver()?;
        let (second_address, second_messages, _second_server) = websocket_receiver()?;
        let socket = Arc::new(AutoReconnectSocket::with_auto_reconnect(false));

        socket.set_addr(Some(first_address.clone()));
        if !wait_until_connected(&socket, &first_address) {
            return Err(std::io::Error::other("first socket did not connect"));
        }

        let target = second_address.clone();
        let socket_for_status = Arc::clone(&socket);
        let _status_guard = socket.actual_connection_state().subscribe(move |signal| {
            if let Signal::Value(status) = signal
                && matches!(&**status, SocketConnectionStatus::Connected(address) if address == &target)
            {
                let _ = SocketTransport::send(
                    socket_for_status.as_ref(),
                    WsFrame::Text("new subscription".into()),
                );
            }
        });

        socket.set_addr(Some(second_address.clone()));
        if !wait_until_connected(&socket, &second_address) {
            return Err(std::io::Error::other("second socket did not connect"));
        }
        let received = second_messages
            .recv_timeout(Duration::from_secs(2))
            .map_err(std::io::Error::other)?;
        if received != "new subscription" {
            return Err(std::io::Error::other("new socket received wrong frame"));
        }
        socket.close();
        Ok(())
    }

    #[test]
    fn failed_address_can_be_retried_and_readdress_interrupts_backoff() -> std::io::Result<()> {
        let unused = TcpListener::bind("127.0.0.1:0")?;
        let retry_address = format!("ws://{}/retry", unused.local_addr()?);
        drop(unused);
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(retry_address.clone()));
        let started = Instant::now();
        while !matches!(socket.get_status(), SocketConnectionStatus::Disconnected)
            && started.elapsed() < Duration::from_secs(2)
        {
            thread::yield_now();
        }
        let retry_listener = TcpListener::bind(
            retry_address
                .strip_prefix("ws://")
                .unwrap()
                .split('/')
                .next()
                .unwrap(),
        )?;
        let retry_server = thread::spawn(move || {
            let (stream, _) = retry_listener.accept().unwrap();
            let _websocket = tungstenite::accept(stream).unwrap();
            thread::sleep(Duration::from_millis(100));
        });
        socket.set_addr(Some(retry_address.clone()));
        assert!(wait_until_connected(&socket, &retry_address));
        socket.close();
        retry_server.join().unwrap();

        let reconnecting = AutoReconnectSocket::with_auto_reconnect(true);
        reconnecting.set_addr(Some("ws://127.0.0.1:9/unreachable".into()));
        let (valid_address, _messages, valid_server) = websocket_receiver()?;
        reconnecting.set_addr(Some(valid_address.clone()));
        assert!(wait_until_connected(&reconnecting, &valid_address));
        reconnecting.close();
        drop(valid_server);
        Ok(())
    }

    #[test]
    fn generation_change_releases_a_blocked_sender() {
        let socket = AutoReconnectSocket::with_auto_reconnect(true);
        let stale = socket.write_tx();
        for id in 0..OUTGOING_QUEUE_CAPACITY {
            stale.send(WsFrame::Text(format!("event:{id}"))).unwrap();
        }
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let blocked = thread::spawn(move || {
            let result = stale.send(WsFrame::Text("blocked".into()));
            completed_tx.send(result).unwrap();
        });
        thread::sleep(Duration::from_millis(20));

        socket.close();

        assert!(
            completed_rx
                .recv_timeout(Duration::from_millis(200))
                .expect("blocked sender released")
                .is_err()
        );
        blocked.join().unwrap();
    }

    #[test]
    fn connected_callbacks_retain_tokio_runtime_context() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = format!("ws://{}/runtime-context", listener.local_addr()?);
        let (stop_tx, stop_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || -> std::io::Result<()> {
            let deadline = Instant::now() + Duration::from_secs(2);
            let stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "runtime-context fixture connection",
                            ));
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => return Err(error),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let _websocket = tungstenite::accept(stream).map_err(std::io::Error::other)?;
            let _ = stop_rx.recv_timeout(Duration::from_secs(2));
            Ok(())
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        let (context_tx, context_rx) = mpsc::sync_channel(1);
        let (task_tx, task_rx) = mpsc::sync_channel(1);
        let _guard = socket.actual_connection_state().subscribe(move |signal| {
            if let Signal::Value(status) = signal
                && matches!(&**status, SocketConnectionStatus::Connected(_))
            {
                let available = tokio::runtime::Handle::try_current().is_ok();
                context_tx.send(available).unwrap();
                if available {
                    let task_tx = task_tx.clone();
                    tokio::spawn(async move {
                        task_tx.send(()).unwrap();
                    });
                }
            }
        });
        socket.set_addr(Some(address));
        let context = context_rx.recv_timeout(Duration::from_secs(2));
        let spawned = if context == Ok(true) {
            task_rx.recv_timeout(Duration::from_secs(2))
        } else {
            Err(mpsc::RecvTimeoutError::Timeout)
        };
        socket.close();
        let _ = stop_tx.send(());
        let server_result = server.join().unwrap();
        assert_eq!(context, Ok(true), "connection subscriber retains Tokio runtime context");
        assert!(spawned.is_ok(), "connection subscriber can spawn a running Tokio task");
        server_result
    }

    #[test]
    fn close_and_readdress_release_a_connected_callback_on_the_retired_queue() -> std::io::Result<()>
    {
        for readdress in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = format!("ws://{}/retired-replay", listener.local_addr()?);
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (closed_tx, closed_rx) = flume::bounded(1);
            let server = thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let websocket = tungstenite::accept(stream).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                websocket
                    .get_ref()
                    .shutdown(std::net::Shutdown::Both)
                    .unwrap();
                drop(websocket);
                closed_tx.send(()).unwrap();
            });
            let socket = Arc::new(AutoReconnectSocket::with_auto_reconnect(true));
            let callback_socket = Arc::clone(&socket);
            let (queued_tx, queued_rx) = mpsc::sync_channel(1);
            let (finished_tx, finished_rx) = mpsc::sync_channel(1);
            let target = address.clone();
            let _guard = socket.actual_connection_state().subscribe(move |signal| {
                if let Signal::Value(status) = signal
                    && matches!(&**status, SocketConnectionStatus::Connected(current) if current == &target)
                {
                    entered_tx.send(()).unwrap();
                    closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                    let queue = callback_socket.write_tx();
                    queued_tx.send(queue.clone()).unwrap();
                    for _ in 0..OUTGOING_QUEUE_CAPACITY * 2 {
                        if let Err(error) = queue.send_timeout(WsFrame::Text("replay".into()), Duration::from_secs(2)) {
                            finished_tx.send(matches!(error, flume::SendTimeoutError::Disconnected(_))).unwrap();
                            return;
                        }
                    }
                    finished_tx.send(false).unwrap();
                }
            });
            socket.set_addr(Some(address));
            let queue = queued_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let deadline = Instant::now() + Duration::from_secs(1);
            while queue.len() != OUTGOING_QUEUE_CAPACITY && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(queue.len(), OUTGOING_QUEUE_CAPACITY);
            let replacement = if readdress {
                Some(websocket_receiver()?)
            } else {
                None
            };
            if let Some((address, _, _)) = &replacement {
                socket.set_addr(Some(address.clone()));
            } else {
                socket.close();
            }
            let released = finished_rx.recv_timeout(Duration::from_millis(500));
            if released.is_err() {
                let _ = finished_rx.recv_timeout(Duration::from_secs(3));
            }
            server.join().unwrap();
            if let Some((address, messages, server)) = replacement {
                if released == Ok(true) {
                    assert!(wait_until_connected(&socket, &address));
                    SocketTransport::send(socket.as_ref(), WsFrame::Text("fresh".into())).unwrap();
                    assert_eq!(
                        messages.recv_timeout(Duration::from_secs(2)).unwrap(),
                        "fresh"
                    );
                }
                socket.close();
                server.join().unwrap();
            }
            assert_eq!(
                released,
                Ok(true),
                "retired queue releases the blocked subscriber without waiting for its timeout"
            );
        }
        Ok(())
    }

    #[test]
    fn coalesced_connection_epochs_publish_disconnect_and_stop_worker() {
        for (generation, attempt) in [(1, 2), (3, 1)] {
            let status = Cell::new(SocketConnectionStatus::Idle);
            let (messages, received) = mpsc::channel();
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let first = std::sync::atomic::AtomicBool::new(true);
            let _guard = status.subscribe(move |signal| {
                if let Signal::Value(current) = signal {
                    messages.send((**current).clone()).unwrap();
                    if matches!(&**current, SocketConnectionStatus::Connected(_))
                        && first.swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        entered_tx.send(()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                    }
                }
            });
            let (publisher, worker) = StatusPublisher::new(status);
            let connected = SocketConnectionStatus::Connected("ws://a".into());
            publisher.publish(1, 1, connected.clone());
            entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            publisher.publish(1, 1, SocketConnectionStatus::Disconnected);
            publisher.publish(2, 1, SocketConnectionStatus::Connected("ws://b".into()));
            publisher.publish(generation, attempt, connected.clone());
            release_tx.send(()).unwrap();
            drop(publisher);
            let deadline = Instant::now() + Duration::from_secs(2);
            while !worker.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            assert!(
                worker.is_finished(),
                "status worker exits after its producer drops"
            );
            worker.join().unwrap();
            let values: Vec<_> = received.try_iter().collect();
            assert_eq!(
                values,
                [
                    SocketConnectionStatus::Idle,
                    connected.clone(),
                    SocketConnectionStatus::Disconnected,
                    connected
                ]
            );
        }
    }

    #[test]
    fn reconnects_after_peer_close_during_connected_callback() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/close-during-replay", listener.local_addr()?);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (closed_tx, closed_rx) = flume::bounded(1);
        let (received_tx, received_rx) = mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let websocket = tungstenite::accept(stream).map_err(std::io::Error::other)?;
            entered_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
            websocket.get_ref().shutdown(std::net::Shutdown::Both)?;
            drop(websocket);
            closed_tx.send(()).map_err(std::io::Error::other)?;
            listener.set_nonblocking(true)?;
            let deadline = Instant::now() + Duration::from_secs(3);
            let stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "replacement connection",
                            ));
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => return Err(error),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut websocket = tungstenite::accept(stream).map_err(std::io::Error::other)?;
            let mut replayed = 0;
            loop {
                match websocket.read().map_err(std::io::Error::other)? {
                    Message::Text(text) if text == "fresh" => break,
                    Message::Text(_) => replayed += 1,
                    _ => {}
                }
            }
            received_tx.send(replayed).map_err(std::io::Error::other)?;
            let _ = stop_rx.recv_timeout(Duration::from_secs(2));
            Ok(())
        });
        let socket = Arc::new(AutoReconnectSocket::with_auto_reconnect(true));
        let callback_socket = Arc::clone(&socket);
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let first = std::sync::atomic::AtomicBool::new(true);
        let target = address.clone();
        let _guard = socket.actual_connection_state().subscribe(move |signal| {
            if let Signal::Value(status) = signal
                && matches!(&**status, SocketConnectionStatus::Connected(current) if current == &target)
                && first.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                entered_tx.send(()).unwrap();
                closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                for _ in 0..OUTGOING_QUEUE_CAPACITY * 2 {
                    SocketTransport::send(callback_socket.as_ref(), WsFrame::Text("replay".into())).unwrap();
                }
                SocketTransport::send(callback_socket.as_ref(), WsFrame::Text("fresh".into())).unwrap();
                completed_tx.send(()).unwrap();
            }
        });
        socket.set_addr(Some(address));
        let completed = completed_rx.recv_timeout(Duration::from_secs(4));
        let queued = socket.write_tx().len();
        let status = socket.get_status();
        if completed.is_err() {
            let outgoing = socket.control.lock().unwrap().outgoing.rx.clone();
            let cleanup_deadline = Instant::now() + Duration::from_secs(1);
            loop {
                for _ in 0..256 {
                    if outgoing.try_recv().is_err() {
                        break;
                    }
                }
                if completed_rx.try_recv().is_ok() {
                    break;
                }
                assert!(
                    Instant::now() < cleanup_deadline,
                    "release blocked test callback"
                );
                thread::sleep(Duration::from_millis(1));
            }
        }
        let _ = stop_tx.send(());
        let server_result = server.join().unwrap();
        socket.close();
        assert!(
            completed.is_ok(),
            "a returning peer must unblock Connected replay; queued={queued}, status={status:?}, receiver={server_result:?}"
        );
        assert!(received_rx.recv_timeout(Duration::from_secs(1)).unwrap() > 0);
        server_result
    }

    #[test]
    fn connected_callback_can_fill_more_than_one_outgoing_queue() -> std::io::Result<()> {
        const MESSAGES: usize = OUTGOING_QUEUE_CAPACITY + 64;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/backpressure", listener.local_addr()?);
        let (received_tx, received_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            let mut ids = Vec::with_capacity(MESSAGES);
            while ids.len() < MESSAGES {
                if let Message::Text(text) = websocket.read().unwrap() {
                    ids.push(text.parse::<usize>().unwrap());
                }
            }
            received_tx.send(ids).unwrap();
        });
        let socket = Arc::new(AutoReconnectSocket::with_auto_reconnect(false));
        let callback_socket = Arc::clone(&socket);
        let target = address.clone();
        let _guard = socket.actual_connection_state().subscribe(move |signal| {
            if let Signal::Value(status) = signal
                && matches!(&**status, SocketConnectionStatus::Connected(current) if current == &target)
            {
                for id in 0..MESSAGES {
                    SocketTransport::send(
                        callback_socket.as_ref(),
                        WsFrame::Text(id.to_string()),
                    )
                    .unwrap();
                }
            }
        });

        socket.set_addr(Some(address));
        let ids = received_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(std::io::Error::other)?;
        assert_eq!(ids, (0..MESSAGES).collect::<Vec<_>>());
        socket.close();
        server.join().unwrap();
        Ok(())
    }

    #[test]
    fn address_generation_handles_a_to_b_to_a_without_stale_delivery() -> std::io::Result<()> {
        fn sender(labels: &'static [&'static str]) -> std::io::Result<(String, JoinHandle<()>)> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = format!("ws://{}/generation", listener.local_addr()?);
            let handle = thread::spawn(move || {
                for label in labels {
                    let (stream, _) = listener.accept().unwrap();
                    let mut websocket = tungstenite::accept(stream).unwrap();
                    websocket.send(Message::Text((*label).into())).unwrap();
                    thread::sleep(Duration::from_millis(50));
                }
            });
            Ok((address, handle))
        }

        let (address_a, server_a) = sender(&["a0", "a1"])?;
        let (address_b, server_b) = sender(&["b0"])?;
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        let incoming = SocketTransport::read_rx(&socket);
        for (address, expected) in [(&address_a, "a0"), (&address_b, "b0"), (&address_a, "a1")] {
            socket.set_addr(Some(address.clone()));
            let frame = incoming
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
            assert!(matches!(frame, WsFrame::Text(text) if text == expected));
        }
        socket.close();
        server_a.join().unwrap();
        server_b.join().unwrap();
        Ok(())
    }

    #[test]
    fn rapid_a_to_b_to_a_keeps_the_last_desired_address() -> std::io::Result<()> {
        let listener_a = TcpListener::bind("127.0.0.1:0")?;
        let address_a = format!("ws://{}/a", listener_a.local_addr()?);
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(2);
        let server_a = thread::spawn(move || {
            for generation in 0..2 {
                let (stream, _) = listener_a.accept().unwrap();
                let _websocket = tungstenite::accept(stream).unwrap();
                accepted_tx.send(generation).unwrap();
                thread::sleep(Duration::from_millis(100));
            }
        });
        let listener_b = TcpListener::bind("127.0.0.1:0")?;
        let address_b = format!("ws://{}/b", listener_b.local_addr()?);
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(address_a.clone()));
        assert!(wait_until_connected(&socket, &address_a));
        assert_eq!(accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);

        socket.set_addr(Some(address_b));
        socket.set_addr(Some(address_a.clone()));

        assert_eq!(
            socket
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .addr,
            Some(address_a)
        );
        assert_eq!(accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        socket.close();
        drop(listener_b);
        server_a.join().unwrap();
        Ok(())
    }

    #[test]
    fn reentrant_intended_callback_converges_to_the_latest_generation() {
        let socket = Arc::new(AutoReconnectSocket::with_auto_reconnect(false));
        let callback_socket = Arc::clone(&socket);
        let _guard = socket.intended_connection_state().subscribe(move |signal| {
            if let Signal::Value(status) = signal
                && matches!(&**status, SocketConnectionStatus::Connected(address) if address == "ws://b.invalid")
            {
                callback_socket.set_addr(Some("ws://a.invalid".into()));
            }
        });

        socket.set_addr(Some("ws://b.invalid".into()));

        assert_eq!(
            socket
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .addr
                .as_deref(),
            Some("ws://a.invalid")
        );
        assert_eq!(
            socket.intended_connection_state().get(),
            SocketConnectionStatus::Connected("ws://a.invalid".into())
        );
        socket.close();
    }

    #[test]
    fn address_change_cancels_an_incomplete_handshake() -> std::io::Result<()> {
        let stalled = TcpListener::bind("127.0.0.1:0")?;
        let stalled_address = format!("ws://{}/stalled", stalled.local_addr()?);
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let stalled_server = thread::spawn(move || {
            let (_stream, _) = stalled.accept().unwrap();
            accepted_tx.send(()).unwrap();
            thread::sleep(Duration::from_secs(1));
        });
        let (valid_address, _messages, valid_server) = websocket_receiver()?;
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(stalled_address));
        accepted_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(std::io::Error::other)?;

        let started = Instant::now();
        socket.set_addr(Some(valid_address.clone()));
        assert!(wait_until_connected(&socket, &valid_address));
        assert!(started.elapsed() < Duration::from_millis(500));
        socket.close();
        stalled_server.join().unwrap();
        drop(valid_server);
        Ok(())
    }

    #[test]
    fn close_interrupts_an_idle_session() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/idle", listener.local_addr()?);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let _websocket = tungstenite::accept(stream).unwrap();
            thread::sleep(Duration::from_secs(1));
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(address.clone()));
        assert!(wait_until_connected(&socket, &address));

        let started = Instant::now();
        socket.close();
        while !matches!(socket.get_status(), SocketConnectionStatus::Idle)
            && started.elapsed() < Duration::from_millis(500)
        {
            thread::yield_now();
        }
        assert!(matches!(socket.get_status(), SocketConnectionStatus::Idle));
        assert!(started.elapsed() < Duration::from_millis(500));
        server.join().unwrap();
        Ok(())
    }

    #[test]
    fn disconnected_session_reconnects_and_delivers_once() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/reconnect", listener.local_addr()?);
        let (disconnected_tx, disconnected_rx) = mpsc::sync_channel(1);
        let (outgoing_tx, outgoing_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (first, _) = listener.accept().unwrap();
            let mut first = tungstenite::accept(first).unwrap();
            first.close(None).unwrap();
            disconnected_tx.send(()).unwrap();
            let (second, _) = listener.accept().unwrap();
            second
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();
            let mut second = tungstenite::accept(second).unwrap();
            let received = second.read().unwrap();
            let duplicate = matches!(
                second.read(),
                Ok(Message::Text(text)) if text == "queued-during-backoff"
            );
            outgoing_tx.send((received, duplicate)).unwrap();
            second.send(Message::Text("reconnected".into())).unwrap();
            thread::sleep(Duration::from_millis(100));
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(true);
        let incoming = SocketTransport::read_rx(&socket);
        socket.set_addr(Some(address));
        disconnected_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(std::io::Error::other)?;
        let disconnected_at = Instant::now();
        while !matches!(socket.get_status(), SocketConnectionStatus::Disconnected)
            && disconnected_at.elapsed() < Duration::from_secs(1)
        {
            thread::yield_now();
        }
        assert!(matches!(
            socket.get_status(),
            SocketConnectionStatus::Disconnected
        ));
        SocketTransport::send(&socket, WsFrame::Text("queued-during-backoff".into()))
            .map_err(std::io::Error::other)?;

        let frame = incoming
            .recv_timeout(Duration::from_secs(3))
            .map_err(std::io::Error::other)?;
        assert!(matches!(frame, WsFrame::Text(text) if text == "reconnected"));
        assert!(incoming.try_recv().is_err());
        assert!(matches!(
            outgoing_rx
                .recv_timeout(Duration::from_secs(3))
                .map_err(std::io::Error::other)?,
            (Message::Text(text), false) if text == "queued-during-backoff"
        ));
        socket.close();
        server.join().unwrap();
        Ok(())
    }

    #[test]
    fn receives_large_text_and_binary_across_read_chunks() -> std::io::Result<()> {
        let text = "large-message:".repeat(20_000);
        let binary = vec![173_u8; 256 * 1024];
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/large-messages", listener.local_addr()?);
        let expected_text = text.clone();
        let expected_binary = binary.clone();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            websocket.send(Message::Text(text.into())).unwrap();
            websocket.send(Message::Binary(binary.into())).unwrap();
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        let incoming = SocketTransport::read_rx(&socket);
        socket.set_addr(Some(address));
        assert!(matches!(
            incoming.recv_timeout(Duration::from_secs(5)).map_err(std::io::Error::other)?,
            WsFrame::Text(text) if text == expected_text
        ));
        assert!(matches!(
            incoming.recv_timeout(Duration::from_secs(5)).map_err(std::io::Error::other)?,
            WsFrame::Binary(binary) if binary == expected_binary
        ));
        done_tx.send(()).map_err(std::io::Error::other)?;
        socket.close();
        server.join().unwrap();
        Ok(())
    }

    #[test]
    fn idle_ping_receives_pong_without_application_output() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/ping", listener.local_addr()?);
        let (pong_tx, pong_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            websocket.send(Message::Ping(vec![1, 2, 3].into())).unwrap();
            let pong = websocket.read().unwrap();
            let duplicate = websocket.read().is_ok();
            pong_tx.send((pong, duplicate)).unwrap();
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(address));

        assert!(matches!(
            pong_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?,
            (Message::Pong(payload), false) if payload.as_ref() == [1, 2, 3]
        ));
        socket.close();
        server.join().unwrap();
        Ok(())
    }

    #[test]
    fn dropping_socket_interrupts_idle_session() -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/drop", listener.local_addr()?);
        let (closed_tx, closed_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut websocket = tungstenite::accept(stream).unwrap();
            closed_tx.send(websocket.read().is_err()).unwrap();
        });
        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        socket.set_addr(Some(address.clone()));
        assert!(wait_until_connected(&socket, &address));

        drop(socket);

        assert!(
            closed_rx
                .recv_timeout(Duration::from_millis(500))
                .map_err(std::io::Error::other)?
        );
        server.join().unwrap();
        Ok(())
    }

    #[test]
    #[ignore = "native idle-socket timing probe; run filtered and isolated"]
    fn native_idle_receive_latency_probe() -> std::io::Result<()> {
        const EVENTS: u64 = 64;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("ws://{}/latency", listener.local_addr()?);
        let (sent_tx, sent_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (stream, _) = listener.accept()?;
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
            let mut websocket = tungstenite::accept(stream).map_err(std::io::Error::other)?;
            for event_id in 0..EVENTS {
                // Keep the reader idle between events so this measures wakeup
                // delivery rather than draining an already-readable burst.
                thread::sleep(Duration::from_millis(23));
                let sent_at = Instant::now();
                websocket
                    .send(Message::Text(format!("event:{event_id}").into()))
                    .map_err(std::io::Error::other)?;
                sent_tx
                    .send((event_id, sent_at))
                    .map_err(std::io::Error::other)?;
            }
            Ok(())
        });

        let socket = AutoReconnectSocket::with_auto_reconnect(false);
        let incoming = SocketTransport::read_rx(&socket);
        SocketTransport::set_addr(&socket, Some(address.clone()));
        if !wait_until_connected(&socket, &address) {
            return Err(std::io::Error::other("latency probe did not connect"));
        }

        let mut latencies = Vec::with_capacity(EVENTS as usize);
        for expected_id in 0..EVENTS {
            let (sent_id, sent_at) = sent_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
            let frame = incoming
                .recv_timeout(Duration::from_secs(2))
                .map_err(std::io::Error::other)?;
            let WsFrame::Text(text) = frame else {
                return Err(std::io::Error::other("latency probe received binary frame"));
            };
            let received_id = text
                .strip_prefix("event:")
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| std::io::Error::other("latency probe received invalid event id"))?;
            if sent_id != expected_id || received_id != expected_id {
                return Err(std::io::Error::other(format!(
                    "latency probe event order mismatch: expected={expected_id} sent={sent_id} received={received_id}"
                )));
            }
            latencies.push(sent_at.elapsed());
        }
        SocketTransport::close(&socket);
        server
            .join()
            .map_err(|_| std::io::Error::other("latency probe server panicked"))??;

        latencies.sort_unstable();
        let percentile =
            |percent: usize| latencies[(latencies.len() - 1) * percent / 100].as_micros();
        println!(
            "AUTOSOCKET_IDLE_RECEIVE events={} p50_us={} p95_us={} p99_us={} max_us={}",
            latencies.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            latencies.last().expect("latencies").as_micros()
        );
        assert!(
            latencies.last().expect("latencies") < &Duration::from_millis(100),
            "idle socket delivery exceeded 100ms"
        );
        Ok(())
    }
}
