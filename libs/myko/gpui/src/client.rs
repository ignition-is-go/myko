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
