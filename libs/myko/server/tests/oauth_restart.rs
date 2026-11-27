//! The server keeps the built-in OAuth authority's state in its Postgres database.
#![allow(clippy::panic_in_result_fn)]
use std::{sync::Arc, time::Duration};

use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Authorizer, Credentials,
    MykoAuthProvider, oauth::OAuthServer,
};
use myko_server::{MykoServer, postgres::PostgresConfig};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Policy;
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}
struct Reject;
impl Authenticator for Reject {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> futures_util::future::BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async { Err(AuthError::Unauthenticated) })
    }
}

struct Running {
    base: String,
    task: tokio::task::JoinHandle<()>,
}
impl Running {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}
async fn start(postgres: &PostgresConfig, addr: std::net::SocketAddr) -> TestResult<Running> {
    let base = format!("http://{addr}");
    let oauth = OAuthServer::new(
        &format!("{base}/oauth"),
        &format!("{base}/myko/mcp"),
        "http://localhost:8081",
    )?
    .with_dynamic_registration();
    let provider = MykoAuthProvider::new(Arc::new(Reject), Arc::new(Policy))
        .with_oauth_server(Arc::new(oauth));
    let server = MykoServer::builder()
        .with_bind_addr(addr)
        .with_postgres(postgres.clone())
        .with_auth_provider(Arc::new(provider))
        .build();
    let task = tokio::spawn(async move {
        if let Err(error) = server.run().await {
            eprintln!("server stopped: {error}");
        }
    });
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(Running { base, task });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("server did not start listening".into())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
async fn a_registered_client_can_authorize_after_the_server_restarts() -> TestResult {
    let table = format!("oauth_{}", uuid::Uuid::new_v4().simple());
    let postgres = PostgresConfig {
        url: std::env::var("MYKO_HISTORY_TEST_URL")?,
        channel: table.clone(),
        table,
    };
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let authorize = |base: &str, client: &str| {
        http.get(format!("{base}/oauth/authorize")).query(&[
            ("client_id", client),
            ("redirect_uri", "http://127.0.0.1:9000/callback"),
            ("response_type", "code"),
            ("resource", &format!("{base}/myko/mcp")),
            ("code_challenge_method", "S256"),
            (
                "code_challenge",
                "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
            ),
        ])
    };

    // The issuer names the stored records, so both runs serve the same address.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let first = start(&postgres, addr).await?;
    let registered: Value = http
        .post(format!("{}/oauth/register", first.base))
        .json(&json!({"client_name": "Agent", "redirect_uris": ["http://127.0.0.1:9000/callback"]}))
        .send()
        .await?
        .json()
        .await?;
    let client = registered
        .get("client_id")
        .and_then(Value::as_str)
        .ok_or("client_id missing")?;
    assert_eq!(authorize(&first.base, client).send().await?.status(), 302);
    assert_eq!(
        authorize(&first.base, "unknown").send().await?.status(),
        400
    );
    first.stop().await;

    let second = start(&postgres, addr).await?;
    assert_eq!(authorize(&second.base, client).send().await?.status(), 302);
    second.stop().await;
    Ok(())
}
