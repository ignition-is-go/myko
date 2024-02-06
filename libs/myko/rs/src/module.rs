use futures_util::Future;
use tokio::sync::broadcast::Receiver;

use crate::{
    event::MEvent,
    query::{AllQueries, QueryResponse},
};

pub trait Module {
    fn new() -> Self;

    fn handle_query(
        &mut self,
        query: AllQueries,
    ) -> impl Future<Output = Option<std::sync::mpsc::Receiver<QueryResponse>>>;

    fn start(&self, events: Receiver<MEvent>) -> impl Future<Output = ()>;
}
