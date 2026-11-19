use gpui::{App, Global};
use myko::client::{MykoClient, MykoProtocol};

/// Application-global Myko client used by the GPUI bridge.
#[derive(Clone)]
pub struct Myko {
    client: MykoClient,
}

impl Global for Myko {}

impl Myko {
    /// Construct a bridge client without installing it as a GPUI global.
    #[must_use]
    pub fn new(address: impl Into<String>) -> Self {
        let client = MykoClient::new();
        // Browser WebSocket delivery stays textual. This avoids relying on
        // shared-memory `ArrayBuffer` views crossing Wasm worker boundaries;
        // native clients retain the compact CBOR transport.
        #[cfg(not(target_arch = "wasm32"))]
        client.set_protocol(MykoProtocol::CBOR);
        #[cfg(target_arch = "wasm32")]
        client.set_protocol(MykoProtocol::JSON);
        client.set_last_message_capture(false);
        client.set_address(Some(address.into()));
        Self { client }
    }

    /// A bridge client over a transport that starts no thread.
    ///
    /// The network transport runs a driver thread that writes its status
    /// cells for as long as it lives, and whichever thread delivers a write
    /// may deliver a GPUI test's write with it. GPUI's test scheduler rejects
    /// a wake-up from any thread but its own, so tests in this crate must not
    /// start that driver.
    #[cfg(test)]
    pub(crate) fn offline() -> Self {
        Self {
            client: offline_client(),
        }
    }

    #[must_use]
    pub const fn client(&self) -> &MykoClient {
        &self.client
    }

    pub fn connect(&self, address: impl Into<String>) {
        self.client.set_address(Some(address.into()));
    }

    pub fn disconnect(&self) {
        self.client.set_address(None);
    }
}

/// Install the platform-default Myko client in GPUI global state.
pub fn provide_myko(address: impl Into<String>, cx: &mut App) {
    cx.set_global(Myko::new(address));
}

/// Obtain the installed Myko bridge client.
///
/// Panics consistently with GPUI's other global accessors if [`provide_myko`]
/// has not been called.
#[must_use]
pub fn myko(cx: &App) -> &Myko {
    cx.global::<Myko>()
}

pub fn disconnect_myko(cx: &App) {
    myko(cx).disconnect();
}

#[cfg(test)]
pub use offline::offline_client;

#[cfg(test)]
mod offline {
    use std::sync::Arc;

    use autosocket::{SocketConnectionStatus, SocketTransport, WsFrame};
    use myko::{
        client::MykoClient,
        hyphae::{Cell, CellImmutable},
    };

    pub fn offline_client() -> MykoClient {
        let (incoming_tx, incoming_rx) = flume::unbounded();
        MykoClient::with_transport(Arc::new(OfflineTransport {
            status: Cell::new(SocketConnectionStatus::Idle).lock(),
            _incoming_tx: incoming_tx,
            incoming_rx,
        }))
    }

    struct OfflineTransport {
        status: Cell<SocketConnectionStatus, CellImmutable>,
        _incoming_tx: flume::Sender<WsFrame>,
        incoming_rx: flume::Receiver<WsFrame>,
    }

    impl SocketTransport for OfflineTransport {
        fn set_addr(&self, _addr: Option<String>) {}

        fn close(&self) {}

        fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
            self.status.clone()
        }

        fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
            self.status.clone()
        }

        fn send(&self, _frame: WsFrame) -> Result<(), String> {
            Err("offline".into())
        }

        fn read_rx(&self) -> flume::Receiver<WsFrame> {
            self.incoming_rx.clone()
        }
    }
}
