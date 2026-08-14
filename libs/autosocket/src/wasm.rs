use std::{cell::RefCell, rc::Rc};

use hyphae::{Cell, CellImmutable, CellMutable, Mutable};
use send_wrapper::SendWrapper;
use tracing::{error, info, warn};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::{CallbackGuard, FrameCallback, SocketConnectionStatus, SocketTransport, WsFrame};

type SharedInner = Rc<RefCell<WasmSocketInner>>;
type SharedCallback = Rc<RefCell<CallbackRegistration>>;

struct WasmSocketInner {
    ws: Option<WebSocket>,
    on_message: Option<Closure<dyn FnMut(MessageEvent)>>,
    on_error: Option<Closure<dyn FnMut(ErrorEvent)>>,
    on_close: Option<Closure<dyn FnMut(CloseEvent)>>,
    on_open: Option<Closure<dyn FnMut()>>,
    reconnect_timeout: Option<(i32, Closure<dyn FnMut()>)>,
    // A timeout cannot destroy its own Closure while it is executing. Keep the
    // previous callback alive until a later scheduling/teardown turn.
    retired_reconnects: Vec<Closure<dyn FnMut()>>,
    generation: u64,
    intended_addr: Option<String>,
}

impl WasmSocketInner {
    fn is_current(&self, generation: u64, addr: &str) -> bool {
        self.generation == generation && self.intended_addr.as_deref() == Some(addr)
    }

    fn detach_socket(&mut self) {
        if let Some(ws) = self.ws.take() {
            ws.set_onclose(None);
            ws.set_onmessage(None);
            ws.set_onerror(None);
            ws.set_onopen(None);
            let _ = ws.close();
        }
        self.on_message = None;
        self.on_error = None;
        self.on_close = None;
        self.on_open = None;
    }

    fn cancel_reconnect(&mut self) {
        if let Some((timeout_id, _closure)) = self.reconnect_timeout.take()
            && let Some(window) = web_sys::window()
        {
            window.clear_timeout_with_handle(timeout_id);
        }
        self.retired_reconnects.clear();
    }

    fn retire_reconnect(&mut self) {
        // These closures were retired on an earlier browser turn and are no
        // longer executing, so they can now be released safely.
        self.retired_reconnects.clear();
        if let Some((timeout_id, closure)) = self.reconnect_timeout.take() {
            if let Some(window) = web_sys::window() {
                window.clear_timeout_with_handle(timeout_id);
            }
            self.retired_reconnects.push(closure);
        }
    }
}

struct CallbackRegistration {
    next_token: u64,
    current: Option<(u64, FrameCallback)>,
}

struct WasmCallbacks {
    intended_status: Cell<SocketConnectionStatus, CellMutable>,
    actual_status: Cell<SocketConnectionStatus, CellMutable>,
}

fn dispatch_incoming_frame(
    frame: WsFrame,
    incoming_tx: &flume::Sender<WsFrame>,
    callback: &SharedCallback,
) {
    let callback = callback.borrow().current.as_ref().map(|(_, cb)| cb.clone());
    if let Some(callback) = callback {
        callback(frame);
    } else {
        let _ = incoming_tx.send(frame);
    }
}

/// Browser WebSocket transport with generation-safe auto-reconnect.
pub struct WasmSocket {
    inner: SendWrapper<SharedInner>,
    callbacks: SendWrapper<WasmCallbacks>,
    incoming_tx: flume::Sender<WsFrame>,
    incoming_rx: flume::Receiver<WsFrame>,
    frame_callback: SendWrapper<SharedCallback>,
    auto_reconnect: bool,
}

// SAFETY: browsers execute this transport on the single WASM main thread.
unsafe impl Send for WasmSocket {}
unsafe impl Sync for WasmSocket {}

impl WasmSocket {
    #[must_use]
    pub fn new() -> Self {
        Self::with_auto_reconnect(true)
    }

    #[must_use]
    pub fn with_auto_reconnect(auto_reconnect: bool) -> Self {
        let (incoming_tx, incoming_rx) = flume::unbounded();
        Self {
            inner: SendWrapper::new(Rc::new(RefCell::new(WasmSocketInner {
                ws: None,
                on_message: None,
                on_error: None,
                on_close: None,
                on_open: None,
                reconnect_timeout: None,
                retired_reconnects: Vec::new(),
                generation: 0,
                intended_addr: None,
            }))),
            callbacks: SendWrapper::new(WasmCallbacks {
                intended_status: Cell::new(SocketConnectionStatus::Idle)
                    .with_name("autosocket.wasm.intended_status"),
                actual_status: Cell::new(SocketConnectionStatus::Idle)
                    .with_name("autosocket.wasm.actual_status"),
            }),
            incoming_tx,
            incoming_rx,
            frame_callback: SendWrapper::new(Rc::new(RefCell::new(CallbackRegistration {
                next_token: 0,
                current: None,
            }))),
            auto_reconnect,
        }
    }

    fn disconnect(&self, status: SocketConnectionStatus) -> u64 {
        let mut inner = self.inner.borrow_mut();
        inner.generation = inner.generation.wrapping_add(1);
        inner.cancel_reconnect();
        inner.detach_socket();
        self.callbacks.actual_status.set(status);
        inner.generation
    }

    fn connect(&self, addr: String, generation: u64) {
        connect(
            addr,
            generation,
            Rc::clone(&self.inner),
            self.callbacks.actual_status.clone(),
            self.incoming_tx.clone(),
            Rc::clone(&self.frame_callback),
            self.auto_reconnect,
            false,
        );
    }
}

impl Default for WasmSocket {
    fn default() -> Self {
        Self::new()
    }
}

impl SocketTransport for WasmSocket {
    fn set_addr(&self, addr: Option<String>) {
        self.callbacks.intended_status.set(addr.clone().map_or(
            SocketConnectionStatus::Idle,
            SocketConnectionStatus::Connected,
        ));
        let generation = self.disconnect(if addr.is_some() {
            SocketConnectionStatus::Disconnected
        } else {
            SocketConnectionStatus::Idle
        });
        self.inner.borrow_mut().intended_addr.clone_from(&addr);
        if let Some(addr) = addr {
            self.connect(addr, generation);
        }
    }

    fn close(&self) {
        self.callbacks
            .intended_status
            .set(SocketConnectionStatus::Idle);
        self.disconnect(SocketConnectionStatus::Idle);
        self.inner.borrow_mut().intended_addr = None;
    }

    fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.callbacks.intended_status.clone().lock()
    }

    fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
        self.callbacks.actual_status.clone().lock()
    }

    fn send(&self, frame: WsFrame) -> Result<(), String> {
        let inner = self.inner.borrow();
        let ws = inner.ws.as_ref().ok_or("WebSocket not connected")?;
        match frame {
            WsFrame::Text(s) => ws.send_with_str(&s).map_err(|e| format!("{e:?}")),
            WsFrame::Binary(b) => ws.send_with_u8_array(&b).map_err(|e| format!("{e:?}")),
        }
    }

    fn read_rx(&self) -> flume::Receiver<WsFrame> {
        self.incoming_rx.clone()
    }

    fn set_frame_callback(&self, callback: FrameCallback) -> Option<CallbackGuard> {
        let token = {
            let mut registration = self.frame_callback.borrow_mut();
            registration.next_token = registration.next_token.wrapping_add(1);
            let token = registration.next_token;
            registration.current = Some((token, callback));
            token
        };
        let registration = SendWrapper::new(Rc::clone(&self.frame_callback));
        Some(CallbackGuard::new(move || {
            let mut registration = registration.borrow_mut();
            if matches!(registration.current.as_ref(), Some((current, _)) if *current == token) {
                registration.current = None;
            }
        }))
    }
}

#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn schedule_reconnect(
    addr: String,
    generation: u64,
    inner: SharedInner,
    status: Cell<SocketConnectionStatus, CellMutable>,
    incoming_tx: flume::Sender<WsFrame>,
    frame_callback: SharedCallback,
    auto_reconnect: bool,
) {
    if !auto_reconnect || !inner.borrow().is_current(generation, &addr) {
        return;
    }
    let Some(window) = web_sys::window() else {
        error!("WasmSocket: browser window unavailable during reconnect");
        return;
    };
    inner.borrow_mut().retire_reconnect();
    let inner_for_timeout = Rc::clone(&inner);
    let closure = Closure::new(move || {
        if !inner_for_timeout.borrow().is_current(generation, &addr) {
            return;
        }
        connect(
            addr.clone(),
            generation,
            Rc::clone(&inner_for_timeout),
            status.clone(),
            incoming_tx.clone(),
            Rc::clone(&frame_callback),
            auto_reconnect,
            true,
        );
    });
    match window.set_timeout_with_callback_and_timeout_and_arguments_0(
        closure.as_ref().unchecked_ref(),
        1000,
    ) {
        Ok(timeout_id) => inner.borrow_mut().reconnect_timeout = Some((timeout_id, closure)),
        Err(e) => error!("WasmSocket: failed to schedule reconnect: {e:?}"),
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn connect(
    addr: String,
    generation: u64,
    inner: SharedInner,
    status: Cell<SocketConnectionStatus, CellMutable>,
    incoming_tx: flume::Sender<WsFrame>,
    frame_callback: SharedCallback,
    auto_reconnect: bool,
    reconnecting: bool,
) {
    if !inner.borrow().is_current(generation, &addr) {
        return;
    }
    status.set(if reconnecting {
        SocketConnectionStatus::Reconnecting(addr.clone())
    } else {
        SocketConnectionStatus::Connecting(addr.clone())
    });
    info!("WasmSocket: connecting to {addr}");

    let ws = match WebSocket::new(&addr) {
        Ok(ws) => ws,
        Err(e) => {
            error!("WasmSocket: failed to create WebSocket: {e:?}");
            if inner.borrow().is_current(generation, &addr) {
                status.set(SocketConnectionStatus::Disconnected);
                schedule_reconnect(
                    addr,
                    generation,
                    inner,
                    status,
                    incoming_tx,
                    frame_callback,
                    auto_reconnect,
                );
            }
            return;
        }
    };
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let inner_message = Rc::clone(&inner);
    let addr_message = addr.clone();
    let incoming_message = incoming_tx.clone();
    let callback_message = Rc::clone(&frame_callback);
    let on_message = Closure::new(move |e: MessageEvent| {
        if !inner_message.borrow().is_current(generation, &addr_message) {
            return;
        }
        let data = e.data();
        let frame = if let Some(text) = data.as_string() {
            WsFrame::Text(text)
        } else if data.is_instance_of::<js_sys::ArrayBuffer>() {
            WsFrame::Binary(js_sys::Uint8Array::new(&data).to_vec())
        } else {
            return;
        };
        dispatch_incoming_frame(frame, &incoming_message, &callback_message);
    });
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

    let on_error = Closure::new(move |e: ErrorEvent| {
        let msg = js_sys::Reflect::get(e.as_ref(), &wasm_bindgen::JsValue::from_str("message"))
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_default();
        error!("WasmSocket: WebSocket error: {msg}");
    });
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    let inner_close = Rc::clone(&inner);
    let addr_close = addr.clone();
    let status_close = status.clone();
    let incoming_close = incoming_tx;
    let callback_close = Rc::clone(&frame_callback);
    let on_close = Closure::new(move |_: CloseEvent| {
        if !inner_close.borrow().is_current(generation, &addr_close) {
            return;
        }
        warn!("WasmSocket: WebSocket closed");
        status_close.set(SocketConnectionStatus::Disconnected);
        schedule_reconnect(
            addr_close.clone(),
            generation,
            Rc::clone(&inner_close),
            status_close.clone(),
            incoming_close.clone(),
            Rc::clone(&callback_close),
            auto_reconnect,
        );
    });
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

    let inner_open = Rc::clone(&inner);
    let addr_open = addr.clone();
    let status_open = status;
    let on_open = Closure::new(move || {
        if !inner_open.borrow().is_current(generation, &addr_open) {
            return;
        }
        info!("WasmSocket: connected to {addr_open}");
        status_open.set(SocketConnectionStatus::Connected(addr_open.clone()));
    });
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let mut state = inner.borrow_mut();
    if !state.is_current(generation, &addr) {
        ws.set_onclose(None);
        ws.set_onmessage(None);
        ws.set_onerror(None);
        ws.set_onopen(None);
        let _ = ws.close();
        return;
    }
    state.detach_socket();
    state.ws = Some(ws);
    state.on_message = Some(on_message);
    state.on_error = Some(on_error);
    state.on_close = Some(on_close);
    state.on_open = Some(on_open);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn callback_dispatch_does_not_also_enqueue() {
        let (tx, rx) = flume::unbounded();
        let count = Arc::new(AtomicUsize::new(0));
        let handler_count = Arc::clone(&count);
        let callbacks = Rc::new(RefCell::new(CallbackRegistration {
            next_token: 1,
            current: Some((
                1,
                Arc::new(move |_| {
                    handler_count.fetch_add(1, Ordering::SeqCst);
                }),
            )),
        }));
        dispatch_incoming_frame(WsFrame::Text("snapshot".into()), &tx, &callbacks);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn callback_guard_only_clears_its_own_registration() {
        let socket = WasmSocket::with_auto_reconnect(false);
        let first = socket.set_frame_callback(Arc::new(|_| {}));
        let second = socket.set_frame_callback(Arc::new(|_| {}));
        assert!(first.is_some() && second.is_some());

        if let (Some(first), Some(second)) = (first, second) {
            drop(first);
            assert!(socket.frame_callback.borrow().current.is_some());
            drop(second);
            assert!(socket.frame_callback.borrow().current.is_none());
        }
    }

    #[test]
    fn generation_and_address_must_both_match() {
        let inner = WasmSocketInner {
            ws: None,
            on_message: None,
            on_error: None,
            on_close: None,
            on_open: None,
            reconnect_timeout: None,
            retired_reconnects: Vec::new(),
            generation: 4,
            intended_addr: Some("ws://new".into()),
        };
        assert!(inner.is_current(4, "ws://new"));
        assert!(!inner.is_current(3, "ws://new"));
        assert!(!inner.is_current(4, "ws://old"));
    }
}
