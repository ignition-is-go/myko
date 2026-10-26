use super::*;
use crate::wire::MEventType;

struct BoundedTransport {
    status: Cell<ConnectionStatus, CellMutable>,
    sent: Mutex<Vec<WsFrame>>,
    limit: usize,
    incoming: flume::Receiver<WsFrame>,
    _sender: flume::Sender<WsFrame>,
}

impl BoundedTransport {
    fn new(limit: usize) -> Arc<Self> {
        let (sender, incoming) = flume::bounded(1);
        Arc::new(Self {
            status: Cell::new(ConnectionStatus::Disconnected),
            sent: Mutex::new(Vec::new()),
            limit,
            incoming,
            _sender: sender,
        })
    }

    fn connect(&self) {
        self.status
            .set(ConnectionStatus::Connected("fixture".into()));
    }

    fn frames(&self) -> Vec<WsFrame> {
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl SocketTransport for BoundedTransport {
    fn set_addr(&self, _addr: Option<String>) {}
    fn close(&self) {}
    fn intended_connection_state(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.status.clone().lock()
    }
    fn actual_connection_state(&self) -> Cell<ConnectionStatus, CellImmutable> {
        self.status.clone().lock()
    }
    fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming.clone()
    }
    fn send(&self, frame: WsFrame) -> Result<(), String> {
        if !matches!(self.status.get(), ConnectionStatus::Connected(_)) {
            return Err("fixture disconnected".into());
        }
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sent.len() >= self.limit {
            return Err("fixture full".into());
        }
        sent.push(frame);
        drop(sent);
        Ok(())
    }
}

fn client_without_ping(transport: &Arc<BoundedTransport>, admission: SendAdmission) -> MykoClient {
    let transport: Arc<dyn SocketTransport> = transport.clone();
    MykoClient::with_transport_and_options(
        &transport,
        MykoClientOptions {
            app_ping: false,
            ..MykoClientOptions::default()
        },
        admission,
    )
}

fn event(value: u16) -> MEvent {
    let server = Server {
        id: "admission-fixture".into(),
        version: value.to_string(),
        address: String::new(),
        port: value,
        started_at: String::new(),
    };
    MEvent::from_item(&server, MEventType::SET, "admission-fixture")
}

#[test]
fn command_results_preserve_json_and_cbor_responder_wire() {
    for protocol in [MykoProtocol::JSON, MykoProtocol::CBOR] {
        let transport = BoundedTransport::new(4);
        let client = client_without_ping(&transport, SendAdmission::Transport);
        client.set_protocol(protocol);
        transport.connect();
        let responder = CommandResponder {
            socket: transport.clone(),
            protocol: client.inner.protocol.clone(),
            tx: "result-tx".into(),
            command_id: "result-command".into(),
        };
        assert!(
            client
                .send_command_result(
                    "result-tx".into(),
                    "result-command".into(),
                    Ok(Value::Bool(true)),
                )
                .is_ok()
        );
        responder.respond_ok(Value::Bool(true));
        assert!(
            client
                .send_command_result(
                    "result-tx".into(),
                    "result-command".into(),
                    Err("rejected".into()),
                )
                .is_ok()
        );
        responder.respond_err("rejected".into());
        let frames = transport.frames();
        let decoded: Vec<Value> = frames
            .iter()
            .map(|frame| match frame {
                WsFrame::Text(text) => {
                    serde_json::from_str(text).map_err(|error| error.to_string())
                }
                WsFrame::Binary(bytes) => {
                    ciborium::de::from_reader(bytes.as_slice()).map_err(|error| error.to_string())
                }
            })
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        assert_eq!(decoded.len(), 4);
        let [ok, legacy_ok, err, legacy_err] = decoded.as_slice() else {
            return;
        };
        assert_eq!(ok, legacy_ok);
        assert_eq!(err, legacy_err);
        assert_eq!(
            ok.pointer("/event").and_then(Value::as_str),
            Some("ws:m:command-response")
        );
        assert_eq!(
            ok.pointer("/data/tx").and_then(Value::as_str),
            Some("result-tx")
        );
        assert_eq!(
            ok.pointer("/data/response").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            err.pointer("/event").and_then(Value::as_str),
            Some("ws:m:command-error")
        );
        assert_eq!(
            err.pointer("/data/tx").and_then(Value::as_str),
            Some("result-tx")
        );
        assert_eq!(
            err.pointer("/data/commandId").and_then(Value::as_str),
            Some("result-command")
        );
        assert_eq!(
            err.pointer("/data/message").and_then(Value::as_str),
            Some("rejected")
        );
        assert!(matches!(
            (&protocol, frames.first()),
            (MykoProtocol::JSON, Some(WsFrame::Text(_)))
                | (MykoProtocol::CBOR, Some(WsFrame::Binary(_)))
        ));
    }
}

#[test]
fn command_results_report_transport_rejection_without_replay() {
    let transport = BoundedTransport::new(1);
    let client = client_without_ping(&transport, SendAdmission::Transport);
    assert_eq!(
        client.send_command_result("offline".into(), "command".into(), Ok(Value::Null)),
        Err("fixture disconnected".into())
    );
    let responder = CommandResponder {
        socket: transport.clone(),
        protocol: client.inner.protocol.clone(),
        tx: "legacy-offline".into(),
        command_id: "command".into(),
    };
    responder.respond_ok(Value::Null);
    transport.connect();
    assert!(transport.frames().is_empty());
    assert_eq!(
        client.send_command_result("accepted".into(), "command".into(), Ok(Value::Null)),
        Ok(())
    );
    assert_eq!(
        client.send_command_result("full".into(), "command".into(), Err("failure".into())),
        Err("fixture full".into())
    );
    responder.respond_err("legacy-full".into());
    assert_eq!(transport.frames().len(), 1);
    assert!(
        client
            .inner
            .pending_sends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

#[test]
fn transport_owned_disconnected_publications_are_rejected_without_replay() {
    let transport = BoundedTransport::new(4);
    let client = MykoClient::with_transport_owned_admission(transport.clone());
    for value in 0..1000 {
        assert_eq!(
            client.send_event(event(value)),
            Err("fixture disconnected".into())
        );
    }
    assert!(
        client
            .inner
            .pending_sends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    transport.connect();
    assert!(transport.frames().is_empty());
}

#[test]
fn transport_owned_full_rejection_preserves_accepted_order() {
    let transport = BoundedTransport::new(2);
    let client = client_without_ping(&transport, SendAdmission::Transport);
    transport.connect();
    assert!(client.send_event(event(1)).is_ok());
    assert!(client.send_event_batch(vec![event(2)]).is_ok());
    assert_eq!(client.send_event(event(3)), Err("fixture full".into()));
    let versions: Vec<_> = transport
        .frames()
        .iter()
        .filter_map(|frame| {
            let WsFrame::Text(text) = frame else {
                return None;
            };
            let message: MykoMessage = serde_json::from_str(text).ok()?;
            let item = match message {
                MykoMessage::Event(event) => event.item,
                MykoMessage::EventBatch(events) => events.into_iter().next()?.item,
                _ => return None,
            };
            serde_json::from_value::<Server>(item)
                .ok()
                .map(|server| server.version)
        })
        .collect();
    assert_eq!(versions, ["1", "2"]);
    assert!(
        client
            .inner
            .pending_sends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

#[derive(Clone, Serialize)]
struct AdmissionCommand {
    value: u16,
}

impl CommandId for AdmissionCommand {
    fn command_id(&self) -> Arc<str> {
        "admission-fixture".into()
    }
}

#[test]
fn rejected_typed_and_raw_command_cells_resolve_and_remove_handlers() {
    let transport = BoundedTransport::new(0);
    let client = client_without_ping(&transport, SendAdmission::Transport);
    let typed = client.send_command::<_, Value>(&AdmissionCommand { value: 1 });
    assert_eq!(typed.get(), Some(Err("fixture disconnected".into())));
    assert!(
        client
            .inner
            .command_response_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    transport.connect();
    let request = CommandRequest::new(AdmissionCommand { value: 2 });
    let wrapped = wrap_command_request(&request);
    assert!(wrapped.is_ok());
    let Ok(wrapped) = wrapped else {
        return;
    };
    let raw = client.send_command_raw_result(wrapped);
    assert_eq!(raw.get(), Some(Err("fixture full".into())));
    assert!(
        client
            .inner
            .command_response_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

#[test]
fn legacy_transport_still_queues_disconnected_events_and_flushes_on_connect() {
    let transport = BoundedTransport::new(4);
    let client = client_without_ping(&transport, SendAdmission::MykoQueue);
    assert!(client.send_event(event(1)).is_ok());
    assert!(client.send_event_batch(vec![event(2)]).is_ok());
    assert_eq!(
        client
            .inner
            .pending_sends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        2
    );
    assert!(transport.frames().is_empty());
    transport.connect();
    assert_eq!(transport.frames().len(), 2);
    assert!(
        client
            .inner
            .pending_sends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert!(client.send_event(event(3)).is_ok());
}

#[test]
fn rejected_cursor_controls_replay_the_latest_desired_window_on_reconnect() {
    use crate::entities::client::GetAllClients;

    let transport = BoundedTransport::new(2);
    let client = client_without_ping(&transport, SendAdmission::Transport);
    let watch = client.watch_query_windowed(
        GetAllClients {},
        QueryWindow {
            offset: 0,
            limit: 1,
        },
    );
    let first = QueryCursorWindow::after("first", 7);
    assert_eq!(
        watch.set_cursor_window(first.clone()),
        Err("fixture disconnected".into())
    );
    transport.connect();
    let cursor_frames = || {
        transport
            .frames()
            .into_iter()
            .filter_map(|frame| {
                let WsFrame::Text(text) = frame else {
                    return None;
                };
                match serde_json::from_str::<MykoMessage>(&text).ok()? {
                    MykoMessage::QueryCursorWindow(update) => Some(update.window),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(cursor_frames(), [first]);
    let latest = QueryCursorWindow::after("latest", 9);
    assert_eq!(
        watch.set_cursor_window(latest.clone()),
        Err("fixture full".into())
    );
    transport.status.set(ConnectionStatus::Disconnected);
    transport
        .sent
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    transport.connect();
    assert_eq!(cursor_frames(), [latest]);
}
