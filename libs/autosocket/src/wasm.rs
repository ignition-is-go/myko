use std::{cell::RefCell, rc::Rc};

use log::{error, info, warn};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::{CallbackGuard, SocketConnectionStatus, SocketTransport, WsFrame, next_callback_id};

type MessageCallback = Box<dyn Fn(WsFrame)>;
type StatusCallback = Box<dyn Fn(SocketConnectionStatus)>;

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
    message_callbacks: Rc<RefCell<Vec<(u64, MessageCallback)>>>,
    status_callbacks: Rc<RefCell<Vec<(u64, StatusCallback)>>>,
    status: Rc<RefCell<SocketConnectionStatus>>,
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
        Self {
            inner: SendWrapper::new(Rc::new(RefCell::new(WasmSocketInner {
                ws: None,
                _on_message: None,
                _on_error: None,
                _on_close: None,
                _on_open: None,
            }))),
            callbacks: SendWrapper::new(WasmCallbacks {
                message_callbacks: Rc::new(RefCell::new(Vec::new())),
                status_callbacks: Rc::new(RefCell::new(Vec::new())),
                status: Rc::new(RefCell::new(SocketConnectionStatus::Idle)),
            }),
            addr: SendWrapper::new(RefCell::new(None)),
            auto_reconnect,
        }
    }

    /// Update status and notify all status callbacks
    fn set_status(callbacks: &WasmCallbacks, new_status: SocketConnectionStatus) {
        *callbacks.status.borrow_mut() = new_status.clone();
        let cbs = callbacks.status_callbacks.borrow();
        for (_, cb) in cbs.iter() {
            cb(new_status.clone());
        }
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
        let msg_cbs = self.callbacks.message_callbacks.clone();
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

            let cbs = msg_cbs.borrow();
            for (_, cb) in cbs.iter() {
                cb(frame.clone());
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        ws.set_onmessage(Some(
            on_message.as_ref().unchecked_ref::<js_sys::Function>(),
        ));

        // onerror
        let on_error = Closure::wrap(Box::new(move |e: ErrorEvent| {
            error!("WasmSocket: WebSocket error: {:?}", e.message());
        }) as Box<dyn FnMut(ErrorEvent)>);

        ws.set_onerror(Some(on_error.as_ref().unchecked_ref::<js_sys::Function>()));

        // onclose: reconnect after 1s
        let status_cbs = self.callbacks.status_callbacks.clone();
        let status = self.callbacks.status.clone();
        let msg_cbs_close = self.callbacks.message_callbacks.clone();
        let inner_close = Rc::clone(&self.inner);
        let addr_close = addr.to_string();
        let auto_reconnect = self.auto_reconnect;
        let on_close = Closure::wrap(Box::new(move |_: CloseEvent| {
            if !auto_reconnect {
                warn!("WasmSocket: WebSocket closed");
                set_status_raw(&status, &status_cbs, SocketConnectionStatus::Disconnected);
                return;
            }
            warn!("WasmSocket: WebSocket closed, reconnecting in 1s");
            set_status_raw(&status, &status_cbs, SocketConnectionStatus::Disconnected);

            let status = status.clone();
            let status_cbs = status_cbs.clone();
            let msg_cbs = msg_cbs_close.clone();
            let inner = Rc::clone(&inner_close);
            let addr = addr_close.clone();

            let window = web_sys::window().expect("no window");
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &Closure::once_into_js(move || {
                    reconnect(&addr, &status, &status_cbs, &msg_cbs, inner, auto_reconnect);
                })
                .unchecked_into(),
                1000,
            );
        }) as Box<dyn FnMut(CloseEvent)>);

        ws.set_onclose(Some(on_close.as_ref().unchecked_ref::<js_sys::Function>()));

        // onopen
        let status_cbs_open = self.callbacks.status_callbacks.clone();
        let status_open = self.callbacks.status.clone();
        let addr_open = addr_string.clone();
        let on_open = Closure::wrap(Box::new(move || {
            info!("WasmSocket: connected to {addr_open}");
            set_status_raw(
                &status_open,
                &status_cbs_open,
                SocketConnectionStatus::Connected(addr_open.clone()),
            );
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
        *self.addr.borrow_mut() = None;
        self.disconnect(SocketConnectionStatus::Idle);
    }

    fn get_status(&self) -> SocketConnectionStatus {
        self.callbacks.status.borrow().clone()
    }

    fn send(&self, frame: WsFrame) -> Result<(), String> {
        let inner = self.inner.borrow();
        let ws = inner.ws.as_ref().ok_or("WebSocket not connected")?;

        match frame {
            WsFrame::Text(s) => ws.send_with_str(&s).map_err(|e| format!("{e:?}")),
            WsFrame::Binary(b) => ws.send_with_u8_array(&b).map_err(|e| format!("{e:?}")),
        }
    }

    fn on_message(&self, cb: Box<dyn Fn(WsFrame) + Send + Sync>) -> CallbackGuard {
        let id = next_callback_id();
        self.callbacks.message_callbacks.borrow_mut().push((id, cb));
        // NOTE(ts): CallbackGuard removal is a no-op in WASM since we can't
        // safely access the SendWrapper from the guard's Drop. Callbacks
        // are cleaned up when the socket is dropped. For the use cases here
        // (app-lifetime subscriptions), this is fine.
        CallbackGuard::noop()
    }

    fn on_status_change(
        &self,
        cb: Box<dyn Fn(SocketConnectionStatus) + Send + Sync>,
    ) -> CallbackGuard {
        // Call immediately with current status
        let current = self.callbacks.status.borrow().clone();
        cb(current);

        let id = next_callback_id();
        self.callbacks.status_callbacks.borrow_mut().push((id, cb));
        CallbackGuard::noop()
    }
}

/// Helper to update status and notify callbacks (used from closures that capture raw Rcs)
fn set_status_raw(
    status: &Rc<RefCell<SocketConnectionStatus>>,
    status_callbacks: &Rc<RefCell<Vec<(u64, StatusCallback)>>>,
    new_status: SocketConnectionStatus,
) {
    *status.borrow_mut() = new_status.clone();
    let cbs = status_callbacks.borrow();
    for (_, cb) in cbs.iter() {
        cb(new_status.clone());
    }
}

/// Standalone reconnect function used in the onclose callback.
/// Updates `inner.ws` so that `send()` uses the new socket.
fn reconnect(
    addr: &str,
    status: &Rc<RefCell<SocketConnectionStatus>>,
    status_callbacks: &Rc<RefCell<Vec<(u64, StatusCallback)>>>,
    message_callbacks: &Rc<RefCell<Vec<(u64, MessageCallback)>>>,
    inner: Rc<RefCell<WasmSocketInner>>,
    auto_reconnect: bool,
) {
    if !auto_reconnect {
        set_status_raw(
            status,
            status_callbacks,
            SocketConnectionStatus::Disconnected,
        );
        return;
    }
    info!("WasmSocket: reconnecting to {addr}");
    set_status_raw(
        status,
        status_callbacks,
        SocketConnectionStatus::Reconnecting(addr.to_string()),
    );

    let ws = match WebSocket::new(addr) {
        Ok(ws) => ws,
        Err(e) => {
            error!("WasmSocket: failed to reconnect: {e:?}");
            // Try again in 1s
            let status = status.clone();
            let status_callbacks = status_callbacks.clone();
            let message_callbacks = message_callbacks.clone();
            let addr = addr.to_string();
            let window = web_sys::window().expect("no window");
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &Closure::once_into_js(move || {
                    reconnect(
                        &addr,
                        &status,
                        &status_callbacks,
                        &message_callbacks,
                        inner,
                        auto_reconnect,
                    );
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
    let msg_cbs = message_callbacks.clone();
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
        let cbs = msg_cbs.borrow();
        for (_, cb) in cbs.iter() {
            cb(frame.clone());
        }
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
    let status_cbs_close = status_callbacks.clone();
    let msg_cbs_close = message_callbacks.clone();
    let inner_close = Rc::clone(&inner);
    let addr_close = addr.to_string();
    let on_close = Closure::wrap(Box::new(move |_: CloseEvent| {
        warn!("WasmSocket: WebSocket closed, reconnecting in 1s");
        set_status_raw(
            &status_close,
            &status_cbs_close,
            SocketConnectionStatus::Disconnected,
        );

        let status = status_close.clone();
        let status_cbs = status_cbs_close.clone();
        let msg_cbs = msg_cbs_close.clone();
        let inner = Rc::clone(&inner_close);
        let addr = addr_close.clone();

        let window = web_sys::window().expect("no window");
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            &Closure::once_into_js(move || {
                reconnect(&addr, &status, &status_cbs, &msg_cbs, inner, auto_reconnect);
            })
            .unchecked_into(),
            1000,
        );
    }) as Box<dyn FnMut(CloseEvent)>);
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref::<js_sys::Function>()));
    on_close.forget();

    // onopen
    let status_open = status.clone();
    let status_cbs_open = status_callbacks.clone();
    let addr_open = addr_string;
    let on_open = Closure::wrap(Box::new(move || {
        info!("WasmSocket: connected to {addr_open}");
        set_status_raw(
            &status_open,
            &status_cbs_open,
            SocketConnectionStatus::Connected(addr_open.clone()),
        );
    }) as Box<dyn FnMut()>);
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref::<js_sys::Function>()));
    on_open.forget();

    // Update inner.ws so send() uses the new socket
    inner.borrow_mut().ws = Some(ws);
}
