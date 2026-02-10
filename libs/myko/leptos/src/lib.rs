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
