#![cfg(not(target_arch = "wasm32"))]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::result_large_err
)]

use std::{
    net::TcpListener,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use autosocket::{
    AutoReconnectSocket, ConnectionRequest, RequestFactoryError, SocketConnectionStatus, WsFrame,
};
use tungstenite::{
    Message,
    handshake::server::{Request, Response},
};

fn wait_for(socket: &AutoReconnectSocket, predicate: impl Fn(SocketConnectionStatus) -> bool) {
    let started = Instant::now();
    while !predicate(socket.get_status()) {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "unexpected socket state: {:?}",
            socket.get_status()
        );
        thread::sleep(Duration::from_millis(2));
    }
}

fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}/myko", listener.local_addr().unwrap());
    (listener, address)
}

struct TokenFile(std::path::PathBuf);
impl TokenFile {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!("autosocket-{}-{nonce}", std::process::id())))
    }
}
impl Drop for TokenFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn authorize(mut request: ConnectionRequest, token: &str) -> ConnectionRequest {
    let mut header = format!("Bearer {token}")
        .parse::<tungstenite::http::HeaderValue>()
        .unwrap();
    header.set_sensitive(true);
    request.headers_mut().insert("authorization", header);
    request
}

#[test]
fn reconnect_reads_rotated_file_and_preserves_frames() {
    let token_file = TokenFile::new();
    std::fs::write(&token_file.0, "first").unwrap();
    let path = token_file.0.clone();
    let socket = AutoReconnectSocket::with_request_factory(true, 1024, 1024, move |request| {
        let path = path.clone();
        async move {
            let token = tokio::fs::read_to_string(path)
                .await
                .map_err(|_| RequestFactoryError)?;
            Ok(authorize(request, &token))
        }
    });
    let (listener, address) = listener();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (close_tx, close_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        for expected in ["first", "rotated"] {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut websocket =
                tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
                    assert_eq!(request.uri().path(), "/myko");
                    assert_eq!(
                        request.headers()["authorization"],
                        format!("Bearer {expected}")
                    );
                    Ok(response)
                })
                .unwrap();
            websocket
                .send(Message::Binary(expected.as_bytes().to_vec().into()))
                .unwrap();
            assert_eq!(websocket.read().unwrap(), Message::Text(expected.into()));
            ready_tx.send(()).unwrap();
            close_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            websocket.close(None).unwrap();
        }
    });
    socket.set_addr(Some(address));
    for expected in ["first", "rotated"] {
        let frame = socket
            .read_rx()
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(matches!(frame, WsFrame::Binary(bytes) if bytes == expected.as_bytes()));
        socket
            .write_tx()
            .send(WsFrame::Text(expected.into()))
            .unwrap();
        ready_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        std::fs::write(&token_file.0, "rotated").unwrap();
        close_tx.send(()).unwrap();
    }
    socket.close();
    server.join().unwrap();
}

#[test]
fn failed_factory_retries_without_connecting_and_recovers_after_file_appears() {
    let token_file = TokenFile::new();
    let path = token_file.0.clone();
    let (called_tx, called_rx) = mpsc::channel();
    let socket = AutoReconnectSocket::with_request_factory(true, 1024, 1024, move |request| {
        let path = path.clone();
        let called_tx = called_tx.clone();
        async move {
            let token = tokio::fs::read_to_string(path).await;
            called_tx.send(token.is_ok()).unwrap();
            Ok(authorize(request, &token.map_err(|_| RequestFactoryError)?))
        }
    });
    let (listener, address) = listener();
    listener.set_nonblocking(true).unwrap();
    socket.set_addr(Some(address));
    for _ in 0..2 {
        assert!(!called_rx.recv_timeout(Duration::from_secs(3)).unwrap());
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    std::fs::write(&token_file.0, "recovered").unwrap();
    listener.set_nonblocking(false).unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut websocket =
            tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
                assert_eq!(request.headers()["authorization"], "Bearer recovered");
                Ok(response)
            })
            .unwrap();
        websocket.send(Message::Text("recovered".into())).unwrap();
    });
    assert!(called_rx.recv_timeout(Duration::from_secs(3)).unwrap());
    assert!(
        matches!(socket.read_rx().recv_timeout(Duration::from_secs(3)).unwrap(), WsFrame::Text(text) if text == "recovered")
    );
    socket.close();
    server.join().unwrap();
}

struct CancelNotice(mpsc::Sender<()>);
impl Drop for CancelNotice {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn close_drop_and_readdress_cancel_pending_factory_without_stale_output() {
    for action in ["close", "drop", "readdress"] {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (cancelled_tx, cancelled_rx) = mpsc::channel();
        let socket = AutoReconnectSocket::with_request_factory(true, 1024, 1024, move |request| {
            let entered_tx = entered_tx.clone();
            let cancelled_tx = cancelled_tx.clone();
            async move {
                if request.uri().path() == "/pending" {
                    let _guard = CancelNotice(cancelled_tx);
                    entered_tx.send(()).unwrap();
                    std::future::pending::<()>().await;
                }
                Ok(authorize(request, "current"))
            }
        });
        let (old_listener, old_address) = listener();
        old_listener.set_nonblocking(true).unwrap();
        socket.set_addr(Some(old_address.replace("/myko", "/pending")));
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        socket
            .write_tx()
            .send(WsFrame::Text("stale".into()))
            .unwrap();
        match action {
            "close" => {
                socket.close();
                wait_for(&socket, |state| state == SocketConnectionStatus::Idle);
            }
            "drop" => drop(socket),
            _ => {
                let (new_listener, new_address) = listener();
                let server = thread::spawn(move || {
                    let (stream, _) = new_listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut websocket =
                        tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
                            assert_eq!(request.headers()["authorization"], "Bearer current");
                            Ok(response)
                        })
                        .unwrap();
                    assert_eq!(websocket.read().unwrap(), Message::Text("current".into()));
                    websocket
                        .send(Message::Text("new generation".into()))
                        .unwrap();
                });
                socket.set_addr(Some(new_address));
                socket
                    .write_tx()
                    .send(WsFrame::Text("current".into()))
                    .unwrap();
                assert!(
                    matches!(socket.read_rx().recv_timeout(Duration::from_secs(3)).unwrap(), WsFrame::Text(text) if text == "new generation")
                );
                socket.close();
                server.join().unwrap();
            }
        }
        cancelled_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            old_listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[test]
fn custom_request_preserves_receive_limits() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut websocket = tungstenite::accept(stream).unwrap();
        websocket
            .send(Message::Binary(vec![0; 128].into()))
            .unwrap();
    });
    let socket =
        AutoReconnectSocket::with_request_factory(false, 32, 32, |request| async { Ok(request) });
    socket.set_addr(Some(address));
    server.join().unwrap();
    wait_for(&socket, |state| {
        state == SocketConnectionStatus::Disconnected
    });
    assert!(socket.read_rx().try_recv().is_err());
    socket.close();
}

#[derive(Clone)]
struct LogCapture(Arc<std::sync::Mutex<String>>);
impl tracing::field::Visit for LogCapture {
    fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        write!(self.0.lock().unwrap(), "{value:?}").unwrap();
    }
}
impl tracing::Subscriber for LogCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with("autosocket")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut self.clone());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[test]
fn rejected_handshake_does_not_log_reflected_credentials_or_body() {
    let logs = LogCapture(Arc::default());
    tracing::subscriber::set_global_default(logs.clone()).unwrap();
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        assert!(
            tungstenite::accept_hdr(stream, |request: &Request, _: Response| {
                assert_eq!(
                    request.headers()["authorization"],
                    "Bearer PRIVATE_TOKEN_123"
                );
                Err(tungstenite::http::Response::builder()
                    .status(403)
                    .header("x-reflected", "PRIVATE_TOKEN_123")
                    .body(Some("PRIVATE_RESPONSE_BODY_456".into()))
                    .unwrap())
            })
            .is_err()
        );
    });
    let socket = AutoReconnectSocket::with_request_factory(false, 1024, 1024, |request| async {
        Ok(authorize(request, "PRIVATE_TOKEN_123"))
    });
    socket.set_addr(Some(address));
    server.join().unwrap();
    wait_for(&socket, |state| {
        state == SocketConnectionStatus::Disconnected
    });
    socket.close();
    let output = logs.0.lock().unwrap().clone();
    assert!(output.contains("request preparation or connection failed"));
    assert!(!output.contains("PRIVATE_TOKEN_123"));
    assert!(!output.contains("PRIVATE_RESPONSE_BODY_456"));
}
