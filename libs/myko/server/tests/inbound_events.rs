//! Client events arrive the same in JSON, whose items are decoded straight to
//! their types, and CBOR, whose items are decoded through `serde_json::Value`:
//! null bytes are stripped, framework-owned fields are filled, and deletes apply.

#![allow(clippy::panic_in_result_fn)]

use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use myko::wire::{MEvent, MEventType, MykoMessage, PingData};
use myko_server::{MykoServer, NullPersister, router::route_connection};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;

pub mod probe {
    use std::sync::Arc;

    use myko::prelude::*;

    #[myko_item]
    #[myko_external(SET, DEL)]
    pub struct InboundProbe {
        pub label: String,
        #[myko_client_id]
        pub client_id: Option<String>,
        #[server_owned]
        pub owner: Arc<str>,
        #[fallback_to_id]
        pub cluster_id: Option<String>,
    }
}

use probe::{InboundProbe, InboundProbeId};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn probe(id: &str, label: &str, owner: &str, cluster_id: Option<&str>) -> InboundProbe {
    InboundProbe {
        id: InboundProbeId::from(id),
        label: label.to_owned(),
        client_id: Some("spoofed".to_owned()),
        owner: owner.into(),
        cluster_id: cluster_id.map(str::to_owned),
    }
}

fn stored(server: &MykoServer, id: &str) -> Option<InboundProbe> {
    server
        .ctx()
        .registry
        .get_or_create("InboundProbe")
        .get_value(&Arc::from(id))
        .and_then(|item| item.as_any().downcast_ref::<InboundProbe>().cloned())
}

/// Send `messages` and wait until the server has handled them.
async fn send_all(addr: std::net::SocketAddr, messages: &[MykoMessage], binary: bool) -> Result {
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/myko")).await?;
    let ping = MykoMessage::Ping(PingData {
        id: "done".into(),
        timestamp: 0,
    });
    for message in messages.iter().chain([&ping]) {
        let frame = if binary {
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(message, &mut bytes)?;
            Message::Binary(bytes.into())
        } else {
            Message::Text(serde_json::to_string(message)?.into())
        };
        socket.send(frame).await?;
    }
    loop {
        match timeout(Duration::from_secs(3), socket.next())
            .await?
            .ok_or("closed")??
        {
            Message::Text(text) if matches!(serde_json::from_str(&text)?, MykoMessage::Ping(_)) => {
                return Ok(());
            }
            Message::Binary(bytes)
                if matches!(
                    ciborium::de::from_reader(bytes.as_ref())?,
                    MykoMessage::Ping(_)
                ) =>
            {
                return Ok(());
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn json_and_cbor_events_are_prepared_and_applied_alike() -> Result {
    let server = Arc::new(
        MykoServer::builder()
            .with_default_persister(Arc::new(NullPersister))
            .build(),
    );
    let host = server.ctx().host_id.to_string();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let accept = {
        let server = server.clone();
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                let ctx = Arc::new(server.ctx());
                tokio::spawn(route_connection(stream, peer, ctx, server.server_info()));
            }
        })
    };

    for (binary, prefix) in [(false, "json"), (true, "cbor")] {
        let empty = format!("{prefix}-empty");
        let owned = format!("{prefix}-owned");
        let nul = format!("{prefix}-nul");
        let gone = format!("{prefix}-gone");
        let set = |probe: InboundProbe| MEvent::from_item(&probe, MEventType::SET, "test");
        let batch = MykoMessage::EventBatch(vec![
            set(probe(&empty, "plain", "", None)),
            set(probe(&owned, "plain", "peer", Some("cluster"))),
            set(probe(&nul, "nul\0byte", "", None)),
            set(probe(&gone, "plain", "", None)),
        ]);
        let single = MykoMessage::Event(MEvent::del(&probe(&gone, "plain", "", None), "test"));
        send_all(addr, &[batch, single], binary).await?;

        let empty = stored(&server, &empty).ok_or("empty probe not applied")?;
        assert!(
            empty
                .client_id
                .as_deref()
                .is_some_and(|id| !id.is_empty() && id != "spoofed"),
            "{prefix}: {:?}",
            empty.client_id
        );
        assert_eq!(&*empty.owner, host, "{prefix}");
        assert_eq!(empty.cluster_id.as_deref(), Some(&*empty.id.0), "{prefix}");

        let owned = stored(&server, &owned).ok_or("owned probe not applied")?;
        assert_eq!(&*owned.owner, "peer", "{prefix}");
        assert_eq!(owned.cluster_id.as_deref(), Some("cluster"), "{prefix}");
        assert_eq!(owned.client_id, empty.client_id, "{prefix}");

        let nul = stored(&server, &nul).ok_or("nul probe not applied")?;
        assert_eq!(nul.label, "nulbyte", "{prefix}");
        assert_eq!(&*nul.owner, host, "{prefix}");
        assert_eq!(nul.client_id, empty.client_id, "{prefix}");

        assert!(stored(&server, &gone).is_none(), "{prefix}");
    }
    accept.abort();
    Ok(())
}
