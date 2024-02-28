use myko_wasm::{
    event::MEvent,
    query::{Query, QueryResponse},
};
use tokio::sync::broadcast::Receiver;

use async_trait::async_trait;

#[async_trait]
pub trait Module {
    fn new() -> Self
    where
        Self: Sized;

    async fn handle_query(
        &mut self,
        query: Query,
    ) -> Option<std::sync::mpsc::Receiver<QueryResponse>>;

    async fn process_event(&mut self, event: MEvent);

    async fn start(&self, events: Receiver<MEvent>) -> ();
}
