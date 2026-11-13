#![allow(clippy::panic_in_result_fn)]

use base64::Engine as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Request {
    head: String,
    body: String,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

/// Serves one canned JSON body per accepted connection and returns the requests seen.
async fn serve(
    responses: impl FnOnce(&str) -> Vec<String> + Send + 'static,
) -> Result<(String, JoinHandle<Result<Vec<Request>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let bodies = responses(&origin);
    let handle = tokio::spawn(async move {
        let mut seen = Vec::new();
        for reply in bodies {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            let split = loop {
                let n = stream.read(&mut chunk).await?;
                if n == 0 {
                    return Err("incomplete request".into());
                }
                bytes.extend_from_slice(chunk.get(..n).ok_or("chunk")?);
                if let Some(at) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break at;
                }
            };
            let head = String::from_utf8(bytes.get(..split).ok_or("head")?.to_vec())?;
            let mut request = Request {
                head,
                body: String::new(),
            };
            let length = request
                .header("content-length")
                .map(str::parse::<usize>)
                .transpose()?
                .unwrap_or(0);
            let mut body = bytes.get(split.saturating_add(4)..).ok_or("body")?.to_vec();
            while body.len() < length {
                let n = stream.read(&mut chunk).await?;
                if n == 0 {
                    return Err("incomplete body".into());
                }
                body.extend_from_slice(chunk.get(..n).ok_or("chunk")?);
            }
            request.body = String::from_utf8(body)?;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                         Connection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                )
                .await?;
            seen.push(request);
        }
        Ok(seen)
    });
    Ok((origin, handle))
}

fn token(access_token: &str, token_type: &str, expires_in: u64) -> String {
    serde_json::json!({
        "access_token": access_token,
        "token_type": token_type,
        "expires_in": expires_in,
    })
    .to_string()
}

fn discovery(origin: &str) -> String {
    serde_json::json!({
        "issuer": origin,
        "authorization_endpoint": format!("{origin}authorize"),
        "token_endpoint": format!("{origin}token"),
        "jwks_uri": format!("{origin}jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    })
    .to_string()
}

async fn bearer_of(provider: &ClientCredentials) -> Result<Option<String>> {
    Ok(provider.credentials().await?.bearer)
}

#[tokio::test]
async fn discovered_endpoint_uses_basic_auth_and_resource_indicator() -> Result {
    let (origin, server) = serve(|origin| {
        vec![
            discovery(origin),
            r#"{"keys":[]}"#.to_owned(),
            token("machine-token", "Bearer", 3600),
        ]
    })
    .await?;
    let provider = ClientCredentials::new(
        TokenEndpoint::Discover(origin),
        "machine-1".into(),
        "fixture-secret".into(),
    )?
    .with_target(TargetResource::Resource("https://api.example/".into()))
    .with_scope("read:cameras write:cameras");

    assert_eq!(
        bearer_of(&provider).await?.as_deref(),
        Some("machine-token")
    );
    assert_eq!(
        bearer_of(&provider.clone()).await?.as_deref(),
        Some("machine-token")
    );

    let requests = server.await??;
    assert_eq!(
        requests.len(),
        3,
        "discovery, keys and one token request; the reconnect hit the cache"
    );
    let token_request = requests.get(2).ok_or("token request")?;
    assert!(token_request.head.starts_with("POST /token "));
    let credentials = base64::engine::general_purpose::STANDARD.encode("machine-1:fixture-secret");
    assert_eq!(
        token_request.header("authorization"),
        Some(format!("Basic {credentials}").as_str())
    );
    assert!(token_request.body.contains("grant_type=client_credentials"));
    assert!(
        token_request
            .body
            .contains("resource=https%3A%2F%2Fapi.example%2F")
    );
    assert!(
        token_request
            .body
            .contains("scope=read%3Acameras+write%3Acameras")
    );
    assert!(
        !token_request.body.contains("client_secret"),
        "basic auth keeps the secret out of the body"
    );
    Ok(())
}

#[tokio::test]
async fn fixed_endpoint_posts_credentials_with_audience() -> Result {
    let (origin, server) = serve(|_| vec![token("machine-token", "bearer", 3600)]).await?;
    let provider = ClientCredentials::new(
        TokenEndpoint::Fixed(format!("{origin}oauth/token")),
        "machine-1".into(),
        "fixture-secret".into(),
    )?
    .with_client_auth(ClientAuth::Post)
    .with_target(TargetResource::Audience("myko-api".into()));

    assert_eq!(
        bearer_of(&provider).await?.as_deref(),
        Some("machine-token")
    );

    let requests = server.await??;
    let request = requests.first().ok_or("token request")?;
    assert!(request.head.starts_with("POST /oauth/token "));
    assert_eq!(request.header("authorization"), None);
    assert!(request.body.contains("client_id=machine-1"));
    assert!(request.body.contains("client_secret=fixture-secret"));
    assert!(request.body.contains("audience=myko-api"));
    assert!(!request.body.contains("resource="));
    Ok(())
}

#[tokio::test]
async fn tokens_inside_renewal_margin_are_replaced() -> Result {
    let (origin, server) =
        serve(|_| vec![token("first", "Bearer", 1), token("second", "Bearer", 1)]).await?;
    let provider = ClientCredentials::new(
        TokenEndpoint::Fixed(format!("{origin}token")),
        "machine-1".into(),
        "fixture-secret".into(),
    )?;

    assert_eq!(bearer_of(&provider).await?.as_deref(), Some("first"));
    assert_eq!(bearer_of(&provider).await?.as_deref(), Some("second"));
    assert_eq!(server.await??.len(), 2);
    Ok(())
}

#[tokio::test]
async fn unusable_token_responses_are_rejected() -> Result {
    for body in [
        token("t", "Bearer", 0),
        token("t", "mac", 3600),
        token("", "Bearer", 3600),
    ] {
        let (origin, server) = serve(move |_| vec![body]).await?;
        let provider = ClientCredentials::new(
            TokenEndpoint::Fixed(format!("{origin}token")),
            "machine-1".into(),
            "fixture-secret".into(),
        )?;
        assert!(matches!(
            provider.credentials().await,
            Err(AuthError::Unavailable)
        ));
        server.await??;
    }
    Ok(())
}

#[test]
fn insecure_or_incomplete_configuration_is_rejected() {
    let reject = |endpoint: TokenEndpoint, id: &str, secret: &str| {
        matches!(
            ClientCredentials::new(endpoint, id.into(), secret.into()),
            Err(AuthError::Unavailable)
        )
    };
    assert!(reject(
        TokenEndpoint::Fixed("http://auth.example/token".into()),
        "id",
        "secret"
    ));
    assert!(reject(
        TokenEndpoint::Discover("http://auth.example/".into()),
        "id",
        "secret"
    ));
    assert!(reject(
        TokenEndpoint::Fixed("https://user:pw@auth.example/token".into()),
        "id",
        "secret"
    ));
    assert!(reject(
        TokenEndpoint::Fixed("https://auth.example/token".into()),
        "",
        "secret"
    ));
    assert!(reject(
        TokenEndpoint::Fixed("https://auth.example/token".into()),
        "id",
        ""
    ));
    assert!(!reject(
        TokenEndpoint::Fixed("https://auth.example/token".into()),
        "id",
        "secret"
    ));
    assert!(!reject(
        TokenEndpoint::Fixed("http://127.0.0.1:9/token".into()),
        "id",
        "secret"
    ));
}
