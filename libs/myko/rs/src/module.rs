use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::event::MEvent;

#[async_trait]
pub trait Module {
    fn new() -> Self
    where
        Self: Sized;

    // async fn handle_query(
    //     &mut self,
    //     query: Query,
    // ) -> Option<tokio::sync::mpsc::Receiver<QueryResponse>>;

    async fn process_event(&mut self, event: MEvent, persist: bool);

    fn entity_name(&self) -> String;

    async fn start_kafka(&mut self, brokers: &[&str], from_kafka_tx: Sender<MEvent>);
}
