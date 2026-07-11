use std::{cell::RefCell, rc::Rc};

use hyphae::{Cell, CellImmutable, CellMutable, Mutable};
use send_wrapper::SendWrapper;
use tracing::{error, info, warn};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::{SocketConnectionStatus, SocketTransport, WsFrame};

struct WasmSocketInner {
    ws: Option<WebSocket>,
    // Hold closures to prevent GC (only for initial connect;
    // reconnected closures are .forget()'d)
    _on_message: Option<Closure<dyn FnMut(MessageEvent)>>,
    _on_error: Option<Closure<dyn FnMut(ErrorEvent)>>,
    _on_close: Option<Closure<dyn FnMut(CloseEvent)>>,
    _on_open: Option<Closure<dyn FnMut()>>,
}

/// Callback registries for WASM (single-threaded, uses Rc<RefCell>)
struct WasmCallbacks {
    intended_status: Cell<SocketConnectionStatus, CellMutable>,
    actual_status: Cell<SocketConnectionStatus, CellMutable>,
}

/// Browser WebSocket transport with auto-reconnect.
///
/// Uses `web_sys::WebSocket` under the hood. Wrapped in `SendWrapper` so it
/// satisfies `Send + Sync` for use in async contexts — WASM is single-threaded
/// so this is safe.
pub struct WasmSocket {
    inner: SendWrapper<Rc<RefCell<WasmSocketInner>>>,
    callbacks: SendWrapper<WasmCallbacks>,
    addr: SendWrapper<RefCell<Option<String>>>,
    incoming_tx: flume::Sender<WsFrame>,
    incoming_rx: flume::Receiver<WsFrame>,
    auto_reconnect: bool,
}

// SAFETY: WASM is single-threaded. SendWrapper ensures this is only accessed on the main thread.
unsafe impl Send for WasmSocket {}
unsafe impl Sync for WasmSocket {}

impl WasmSocket {
    pub fn new() -> Self {
        Self::with_auto_reconnect(true)
    }

    pub fn with_auto_reconnect(auto_reconnect: bool) -> Self {
        let (incoming_tx, incoming_rx) = flume::unbounded();
        Self {
            inner: SendWrapper::new(Rc::new(RefCell::new(WasmSocketInner {
                ws: None,
                _on_message: None,
                _on_error: None,
                _on_close: None,
                _on_open: None,
            }))),
            callbacks: SendWrapper::new(WasmCallbacks {
                intended_status: Cell::new(SocketConnectionStatus::Idle)
                    .with_name("autosocket.wasm.intended_status"),
                actual_status: Cell::new(SocketConnectionStatus::Idle)
                    .with_name("autosocket.wasm.actual_status"),
            }),
            addr: SendWrapper::new(RefCell::new(None)),
            incoming_tx,
            incoming_rx,
            auto_reconnect,
        }
    }

    /// Update status and notify all status callbacks
    fn set_status(callbacks: &WasmCallbacks, new_status: SocketConnectionStatus) {
        callbacks.actual_status.set(new_status);
    }

    fn connect(&self, addr: &str) {
        info!("WasmSocket: connecting to {addr}");
        Self::set_status(
            &self.callbacks,
            SocketConnectionStatus::Connecting(addr.to_string()),
        );

        let ws = match WebSocket::new(addr) {
            Ok(ws) => ws,
            Err(e) => {
                error!("WasmSocket: failed to create WebSocket: {e:?}");
                Self::set_status(&self.callbacks, SocketConnectionStatus::Disconnected);
                return;
            }
        };

        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let addr_string = addr.to_string();

        // onmessage: parse into WsFrame and call callbacks
        let incoming_tx = self.incoming_tx.clone();
        let on_message = Closure::wrap(Box::new(move |e: MessageEvent| {
            let data = e.data();

            let frame = if let Some(text) = data.as_string() {
                WsFrame::Text(text)
            } else if data.is_instance_of::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&data);
                WsFrame::Binary(array.to_vec())
            } else {
                return;
            };

            let _ = incoming_tx.send(frame);
        }) as Box<dyn FnMut(MessageEvent)>);

        ws.set_onmessage(Some(
            on_message.as_ref().unchecked_ref::<js_sys::Function>(),
        ));

        // onerror
        let on_error = Closure::wrap(Box::new(move |e: ErrorEvent| {
            // NOTE: `ErrorEvent::message()` calls the JS `.message` getter, which is
            // `undefined` on a bare WebSocket error event. web_sys marshals the result
            // straight into wasm, so `undefined` panics in `passStringToWasm0`
            // ("Cannot read properties of undefined (reading 'length')") and loops on
            // every reconnect attempt. Read it defensively instead.
            let msg = js_sys::Reflect::get(e.as_ref(), &wasm_bindgen::JsValue::from_str("message"))
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            error!("WasmSocket: WebSocket error: {msg}");
        }) as Box<dyn FnMut(ErrorEvent)>);

        ws.set_onerror(Some(on_error.as_ref().unchecked_ref::<js_sys::Function>()));

        // onclose: reconnect after 1s
        let inner_close = Rc::clone(&self.inner);
        let addr_close = addr.to_string();
        let actual_status = self.callbacks.actual_status.clone();
        let incoming_tx_for_reconnect = self.incoming_tx.clone();
        let auto_reconnect = self.auto_reconnect;
        let on_close = Closure::wrap(Box::new(move |_: CloseEvent| {
            if !auto_reconnect {
                warn!("WasmSocket: WebSocket closed");
                actual_status.set(SocketConnectionStatus::Disconnected);
                return;
            }
            warn!("WasmSocket: WebSocket closed, reconnecting in 1s");
            actual_status.set(SocketConnectionStatus::Disconnected);

            let actual_status = actual_status.clone();
            let incoming_tx = incoming_tx_for_reconnect.clone();
            let inner = Rc::clone(&inner_close);
            let addr = addr_close.clone();

            let window = web_sys::window().expect("no window");
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &Closure::once_into_js(move || {
                    reconnect(&addr, &actual_status, incoming_tx, inner, auto_reconnect);
                })
                .unchecked_into(),
                1000,
            );
        }) as Box<dyn FnMut(CloseEvent)>);

        ws.set_onclose(Some(on_close.as_ref().unchecked_ref::<js_sys::Function>()));

        // onopen
        let status_open = self.callbacks.actual_status.clone();
        let addr_open = addr_string.clone();
        let on_open = Closure::wrap(Box::new(move || {
            info!("WasmSocket: connected to {addr_open}");
            status_open.set(SocketConnectionStatus::Connected(addr_open.clone()));
        }) as Box<dyn FnMut()>);

        ws.set_onopen(Some(on_open.as_ref().unchecked_ref::<js_sys::Function>()));

        // Store closures and socket
        let mut inner = self.inner.borrow_mut();
        inner._on_message = Some(on_message);
        inner._on_error = Some(on_error);
        inner._on_close = Some(on_close);
        inner._on_open = Some(on_open);
        inner.ws = Some(ws);
    }

    fn disconnect(&self, new_status: SocketConnectionStatus) {
        let mut inner = self.inner.borrow_mut();
        if let Some(ws) = inner.ws.take() {
            // Clear callbacks to prevent reconnect-on-close
            ws.set_onclose(None);
            ws.set_onmessage(None);
            ws.set_onerror(None);
            ws.set_onopen(None);
            let _ = ws.close();
        }
        inner._on_message = None;
        inner._on_error = None;
        inner._on_close = None;
        inner._on_open = None;
        Self::set_status(&self.callbacks, new_status);
    }
}

impl SocketTransport for WasmSocket {
    fn set_addr(&self, addr: Option<String>) {
        self.callbacks.intended_status.set(match addr.clone() {
            Some(a) => SocketConnectionStatus::Connected(a),
            None => SocketConnectionStatus::Idle,
        });
        // Disconnect existing connection
        self.disconnect(if addr.is_some() {
            SocketConnectionStatus::Disconnected
        } else {
            SocketConnectionStatus::Idle
        });

        *self.addr.borrow_mut() = addr.clone();

        if let Some(addr) = addr {
            self.connect(&addr);
        }
    }

    fn close(&self) {
        self.callbacks
            .intended_status
            .set(SocketConnectionStatus::Idle);
        *self.addr.borrow_mut() = None;
        self.disconnect(SocketConnectionStatus::Idle);
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
}

/// Standalone reconnect function used in the onclose callback.
/// Updates `inner.ws` so that `send()` uses the new socket.
fn reconnect(
    addr: &str,
    status: &Cell<SocketConnectionStatus, CellMutable>,
    incoming_tx: flume::Sender<WsFrame>,
    inner: Rc<RefCell<WasmSocketInner>>,
    auto_reconnect: bool,
) {
    if !auto_reconnect {
        status.set(SocketConnectionStatus::Disconnected);
        return;
    }
    info!("WasmSocket: reconnecting to {addr}");
    status.set(SocketConnectionStatus::Reconnecting(addr.to_string()));

    let ws = match WebSocket::new(addr) {
        Ok(ws) => ws,
        Err(e) => {
            error!("WasmSocket: failed to reconnect: {e:?}");
            // Try again in 1s
            let status = status.clone();
            let incoming_tx = incoming_tx.clone();
            let addr = addr.to_string();
            let window = web_sys::window().expect("no window");
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &Closure::once_into_js(move || {
                    reconnect(&addr, &status, incoming_tx, inner, auto_reconnect);
                })
                .unchecked_into(),
                1000,
            );
            return;
        }
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let addr_string = addr.to_string();

    // onmessage
    let incoming_tx_for_message = incoming_tx.clone();
    let on_message = Closure::wrap(Box::new(move |e: MessageEvent| {
        let data = e.data();
        let frame = if let Some(text) = data.as_string() {
            WsFrame::Text(text)
        } else if data.is_instance_of::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&data);
            WsFrame::Binary(array.to_vec())
        } else {
            return;
        };
        let _ = incoming_tx_for_message.send(frame);
    }) as Box<dyn FnMut(MessageEvent)>);
    ws.set_onmessage(Some(
        on_message.as_ref().unchecked_ref::<js_sys::Function>(),
    ));
    on_message.forget();

    // onerror
    let on_error = Closure::wrap(Box::new(move |e: ErrorEvent| {
        error!("WasmSocket: WebSocket error: {:?}", e.message());
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref::<js_sys::Function>()));
    on_error.forget();

    // onclose: reconnect again
    let status_close = status.clone();
    let inner_close = Rc::clone(&inner);
    let addr_close = addr.to_string();
    let incoming_tx_for_close = incoming_tx.clone();
    let on_close = Closure::wrap(Box::new(move |_: CloseEvent| {
        warn!("WasmSocket: WebSocket closed, reconnecting in 1s");
        status_close.set(SocketConnectionStatus::Disconnected);

        let status = status_close.clone();
        let incoming_tx = incoming_tx_for_close.clone();
        let inner = Rc::clone(&inner_close);
        let addr = addr_close.clone();

        let window = web_sys::window().expect("no window");
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            &Closure::once_into_js(move || {
                reconnect(&addr, &status, incoming_tx, inner, auto_reconnect);
            })
            .unchecked_into(),
            1000,
        );
    }) as Box<dyn FnMut(CloseEvent)>);
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref::<js_sys::Function>()));
    on_close.forget();

    // onopen
    let status_open = status.clone();
    let addr_open = addr_string;
    let on_open = Closure::wrap(Box::new(move || {
        info!("WasmSocket: connected to {addr_open}");
        status_open.set(SocketConnectionStatus::Connected(addr_open.clone()));
    }) as Box<dyn FnMut()>);
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref::<js_sys::Function>()));
    on_open.forget();

    // Update inner.ws so send() uses the new socket
    inner.borrow_mut().ws = Some(ws);
}
