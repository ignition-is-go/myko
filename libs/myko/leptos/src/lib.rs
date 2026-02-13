use leptos::prelude::*;

/// Initialize the myko-leptos bridge.
///
/// Creates a `MykoClient` with WASM WebSocket transport and stores it in Leptos context.
/// Call this once in your root `App` component.
///
/// `address` should be the myko server WebSocket address (e.g. `"localhost:5155"`).
pub fn provide_myko(address: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::client::MykoClient;
        let client = MykoClient::new();
        client.set_protocol(myko_rs::client::MykoProtocol::JSON);
        client.set_address(Some(address.to_string()));
        provide_context(client);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = address;
}

/// Disconnect the Myko client (clears the address).
pub fn disconnect_myko() {
    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::client::MykoClient;
        let client = expect_context::<MykoClient>();
        client.set_address(None);
    }
}

/// Returns a reactive signal tracking the Myko connection status.
pub fn use_connection_status() -> ReadSignal<bool> {
    let (read, write) = signal(false);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::{ConnectionStatus, MykoClient},
            hypha::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.connection_status();
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(status) = signal {
                write.set(matches!(&**status, ConnectionStatus::Connected(_)));
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = write;

    read
}

/// Returns a reactive signal of query results that updates when the server pushes data.
///
/// Subscribes to the query via `MykoClient` and updates the signal on each change.
///
/// `Q` is the query type (e.g. `GetAllServers`).
/// `T` is the output type that `Q::Item` converts into via `Into<T>`.
pub fn live_query<Q, T>(query: Q) -> ReadSignal<Vec<T>>
where
    Q: myko_rs::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko_rs::core::item::Eventable
        + myko_rs::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Into<T>
        + Send
        + 'static,
    T: Clone + Send + Sync + 'static,
{
    let (read, write) = signal(vec![]);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::MykoClient,
            hypha::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.watch_query(query);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(items) = signal {
                write.set((**items).iter().cloned().map(Into::into).collect());
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = (query, write);

    read
}

/// Send a command to the Myko server and return a reactive signal with the result.
///
/// The returned signal starts as `None` and resolves to `Some(Ok(response))` or
/// `Some(Err(message))` when the server responds.
pub fn send_command<C, R>(cmd: C) -> ReadSignal<Option<Result<R, String>>>
where
    C: serde::Serialize + Clone + myko_rs::core::command::CommandId + 'static,
    R: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let (read, write) = signal(None);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::MykoClient,
            hypha::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.send_command::<C, R>(&cmd);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(result) = signal {
                write.set((**result).clone());
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = (cmd, write);

    read
}
