#![allow(clippy::panic_in_result_fn)]

//! Entity events a client sends back to back, as one websocket read delivers them.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use myko::{
    entities::demo::{DemoStatusId, DemoTask, DemoTaskId},
    wire::{MEvent, MEventType, MykoMessage, PingData},
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_access};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn serve(server: Arc<MykoServer>) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let ctx = Arc::new(server.ctx());
            let info = server.server_info();
            tokio::spawn(async move {
                route_connection_with_access(stream, peer, ctx, info, GatewayAccess::Full).await
            });
        }
    });
    Ok((addr, task))
}

fn task(id: &str, title: &str) -> DemoTask {
    DemoTask {
        id: DemoTaskId::from(id),
        title: title.to_owned(),
        completed: false,
        status_id: DemoStatusId::from("todo"),
    }
}

fn event(item: &DemoTask, change: MEventType) -> MykoMessage {
    MykoMessage::Event(MEvent::from_item(item, change, "inbound"))
}

fn titles(server: &MykoServer) -> Vec<(String, String)> {
    let mut rows: Vec<_> = server
        .ctx()
        .registry
        .get_or_create("DemoTask")
        .snapshot()
        .into_iter()
        .filter_map(|(_, item)| {
            let task = item.as_any().downcast_ref::<DemoTask>()?;
            Some((task.id.to_string(), task.title.clone()))
        })
        .collect();
    rows.sort();
    rows
}

/// Frames sent together end in the state of applying each in order: a key
/// written twice keeps its later value, a delete after a set removes the row,
/// and a batch between single events keeps its place.
#[tokio::test]
async fn back_to_back_events_apply_in_arrival_order() -> Result {
    let server = Arc::new(MykoServer::builder().build());
    let (addr, accept) = serve(server.clone()).await?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/myko")).await?;
    let burst = [
        event(&task("a", "a1"), MEventType::SET),
        event(&task("b", "b1"), MEventType::SET),
        event(&task("a", "a2"), MEventType::SET),
        MykoMessage::EventBatch(vec![
            MEvent::from_item(&task("c", "c1"), MEventType::SET, "inbound"),
            MEvent::from_item(&task("a", "a3"), MEventType::SET, "inbound"),
        ]),
        event(&task("b", "b1"), MEventType::DEL),
        event(&task("c", "c2"), MEventType::SET),
        MykoMessage::Ping(PingData {
            id: "after-burst".into(),
            timestamp: 0,
        }),
    ];
    for message in &burst {
        socket
            .feed(Message::Text(serde_json::to_string(message)?.into()))
            .await?;
    }
    socket.flush().await?;
    loop {
        let frame = timeout(Duration::from_secs(3), socket.next())
            .await?
            .ok_or("closed")??;
        if let Message::Text(text) = frame
            && matches!(serde_json::from_str(&text)?, MykoMessage::Ping(_))
        {
            break;
        }
    }
    assert_eq!(
        titles(&server),
        vec![("a".into(), "a3".into()), ("c".into(), "c2".into())],
        "the events applied before the ping that followed them, in order"
    );
    accept.abort();
    Ok(())
}
