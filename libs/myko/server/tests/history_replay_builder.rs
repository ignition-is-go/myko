//! `with_history_replay` serves history from a provider other than Postgres.

use std::sync::Arc;

use myko::{
    server::{HandlerRegistry, HistoryReplayProvider},
    store::StoreRegistry,
};
use myko_server::MykoServer;

struct Fixture;

impl HistoryReplayProvider for Fixture {
    fn history_identity(&self) -> Option<String> {
        Some("fixture".into())
    }

    fn replay_to_store(
        &self,
        _until: &str,
        _handlers: &HandlerRegistry,
    ) -> Result<Arc<StoreRegistry>, String> {
        Err("a fixture has no history to replay".into())
    }
}

#[test]
fn the_server_context_uses_the_given_history_provider() {
    let server = MykoServer::builder()
        .with_history_replay(Arc::new(Fixture))
        .build();
    let ctx = server.ctx();
    let identity = ctx
        .history_replay()
        .and_then(|provider| provider.history_identity());
    assert_eq!(identity.as_deref(), Some("fixture"));
}
