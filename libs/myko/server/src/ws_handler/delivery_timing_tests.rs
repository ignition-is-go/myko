#![allow(clippy::panic_in_result_fn)]

use std::time::SystemTime;

use myko_auth::{AuthError, AuthenticatedIdentity, Authorizer, Principal, PrincipalKind};

use super::*;

struct PausingPolicy {
    pause_next: AtomicBool,
}

impl Authorizer for PausingPolicy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    fn revision(&self) -> u64 {
        if self.pause_next.swap(false, Ordering::AcqRel) {
            thread::sleep(Duration::from_millis(200));
        }
        0
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorization_time_is_not_serialization_or_socket_send_time()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let accepted = async {
        let (stream, _) = listener.accept().await?;
        tokio_tungstenite::accept_async(stream).await
    };
    let connected = tokio_tungstenite::connect_async(format!("ws://{address}"));
    let (server, client) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(accepted, connected)
    })
    .await?;
    let (mut write, _read) = server?.split();
    let (mut client, _) = client?;
    let diagnostics = DeliveryDiagnostics::for_timing_test("timing-client", address);
    let policy = Arc::new(PausingPolicy {
        pause_next: AtomicBool::new(false),
    });
    let auth = AuthSession::new(
        AuthenticatedIdentity {
            user_id: None,
            principal: Principal {
                issuer: "fixture".into(),
                subject: "timing-client".into(),
                kind: PrincipalKind::User,
            },
            client_id: None,
            scopes: Vec::new(),
            delegation: Vec::new(),
            claims: serde_json::Value::Null,
            expires_at: SystemTime::now()
                .checked_add(Duration::from_mins(1))
                .ok_or_else(|| std::io::Error::other("session expiry overflow"))?,
        },
        policy.clone(),
    );
    let (cancel, _) = mpsc::unbounded_channel();
    let subscription_auth = SubscriptionAuthorization::new(cancel);
    let commands = Arc::new(Mutex::new(HashMap::new()));
    policy.pause_next.store(true, Ordering::Release);
    assert!(
        WsHandler::send_outbound(
            &mut write,
            OutboundMessage::Message(MykoMessage::Ping(myko::wire::PingData {
                id: "timing-ping".into(),
                timestamp: 0,
            })),
            "timing-client",
            address,
            &AtomicU8::new(MykoProtocol::JSON.into()),
            &commands,
            &diagnostics,
            Some(&auth),
            &subscription_auth,
        )
        .await
    );
    assert!(!policy.pause_next.load(Ordering::Acquire));
    let received = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await?
        .ok_or_else(|| std::io::Error::other("missing WebSocket frame"))??;
    let message: MykoMessage = serde_json::from_str(received.to_text()?)?;
    assert!(matches!(message, MykoMessage::Ping(ping) if ping.id == "timing-ping"));
    let (_, serialize_ms, send_ms) = diagnostics
        .timing_maxima_ms()
        .ok_or_else(|| std::io::Error::other("timing diagnostics are disabled"))?;
    assert!(
        serialize_ms < 100.0 && send_ms < 100.0,
        "200 ms authorization pause leaked into phase timings: serialize={serialize_ms} ms, send={send_ms} ms"
    );
    Ok(())
}

