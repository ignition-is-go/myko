#![allow(clippy::panic_in_result_fn)]
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use myko_auth0::login::{LoginConfig, LoginError, LoginProvider};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the HTTP fixture drives discovery, exchange and validation as one sequence"
)]
async fn discovery_pkce_exchange_and_id_token_binding_use_shared_verification() -> Result {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let issuer = format!("http://{}/", listener.local_addr()?);
    let configured = issuer.clone();
    let key = EncodingKey::from_rsa_pem(include_bytes!("fixtures/test-key.pem"))?;
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("fixture-key".into());
    let token = encode(
        &header,
        &json!({"iss":issuer,"aud":"browser-client","azp":"browser-client","sub":"person",
        "nonce":"login-nonce","exp":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()+300}),
        &key,
    )?;
    let signed = token.clone();
    let verifier = "a".repeat(64);
    let sent_verifier = verifier.clone();
    let server = tokio::spawn(async move {
        for index in 0..3 {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 4096];
            let request = loop {
                let count = stream.read(&mut buffer).await?;
                if count == 0 {
                    return Err("closed HTTP request".into());
                }
                bytes.extend_from_slice(buffer.get(..count).ok_or("request length")?);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let head = std::str::from_utf8(bytes.get(..end).ok_or("headers")?)?;
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break String::from_utf8(bytes)?;
                    }
                }
            };
            let body = match index {
                0 => {
                    assert!(request.starts_with("GET /.well-known/openid-configuration "));
                    json!({"issuer":issuer,"jwks_uri":format!("{issuer}jwks")}).to_string()
                }
                1 => {
                    assert!(request.starts_with("GET /jwks "));
                    include_str!("fixtures/jwks.json").into()
                }
                _ => {
                    assert!(request.starts_with("POST /oauth/token "));
                    assert!(request.contains(&format!("code_verifier={sent_verifier}")));
                    assert!(request.contains("grant_type=authorization_code"));
                    assert!(request.contains("client_id=browser-client"));
                    json!({"id_token":signed}).to_string()
                }
            };
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await?;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let provider = LoginProvider::discover(
        LoginConfig {
            issuer: configured,
            client_id: "browser-client".into(),
            client_secret: "fixture-secret".into(),
        },
        http,
    )
    .await?;
    assert_eq!(
        provider
            .exchange_code("code", "bad", "http://localhost/callback", "login-nonce")
            .await,
        Err(LoginError::InvalidRequest)
    );
    let identity = provider
        .exchange_code(
            "code",
            &verifier,
            "http://localhost/callback",
            "login-nonce",
        )
        .await?;
    assert_eq!(identity.get("sub"), Some(&json!("person")));
    server.await??;
    assert!(
        provider
            .verify_id_token(&token, Some("other-login"))
            .await
            .is_err()
    );
    assert!(
        provider
            .verify_access_token(&token, "another-resource")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines, clippy::indexing_slicing)]
async fn resource_exchange_binds_token_to_login_and_redacts_secrets() -> Result {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let issuer = format!("http://{}/", listener.local_addr()?);
    let key = EncodingKey::from_rsa_pem(include_bytes!("fixtures/test-key.pem"))?;
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("fixture-key".into());
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let id = encode(
        &header,
        &json!({"iss":issuer,"aud":"browser-client","azp":"browser-client","sub":"person","nonce":"nonce","exp":now+300}),
        &key,
    )?;
    let access = json!({"iss":issuer,"aud":"https://inventory.fixture","azp":"browser-client","sub":"person","exp":now+300,"scope":"inventory:read inventory:enroll"});
    let signed = encode(&header, &access, &key)?;
    let response = Arc::new(Mutex::new(
        json!({"id_token":id,"access_token":signed,"token_type":"Bearer"}),
    ));
    let replies = response.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let exchanges = count.clone();
    let source = issuer.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 4096];
            let request = loop {
                let n = stream.read(&mut buffer).await?;
                if n == 0 {
                    return Err("closed request".into());
                }
                bytes.extend_from_slice(buffer.get(..n).ok_or("length")?);
                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(bytes.get(..end).ok_or("headers")?)?;
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break String::from_utf8(bytes)?;
                    }
                }
            };
            let body = if request.starts_with("GET /.well-known/openid-configuration ") {
                json!({"issuer":source,"jwks_uri":format!("{source}jwks")}).to_string()
            } else if request.starts_with("GET /jwks ") {
                include_str!("fixtures/jwks.json").to_owned()
            } else {
                assert!(request.starts_with("POST /oauth/token "));
                assert!(request.contains(&format!("code_verifier={}", "a".repeat(64))));
                exchanges.fetch_add(1, Ordering::SeqCst);
                replies.lock().map_err(|_| "poisoned")?.to_string()
            };
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let provider = LoginProvider::discover(
        LoginConfig {
            issuer,
            client_id: "browser-client".into(),
            client_secret: "secret".into(),
        },
        reqwest::Client::new(),
    )
    .await?;
    let verifier = "a".repeat(64);
    let granted = provider
        .exchange_code_for_resource(
            "code",
            &verifier,
            "http://localhost/callback",
            "nonce",
            "https://inventory.fixture",
        )
        .await?;
    assert_eq!(granted.access_token(), signed);
    assert_eq!(granted.identity().get("sub"), Some(&json!("person")));
    assert_eq!(granted.audience(), "https://inventory.fixture");
    assert_eq!(granted.expires_at(), now + 300);
    assert_eq!(granted.scopes(), ["inventory:read", "inventory:enroll"]);
    assert!(!format!("{granted:?}").contains(&signed));
    assert!(!format!("{granted:?}").contains(&id));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut cases = Vec::new();
    for (field, value) in [
        ("aud", json!("https://wrong.fixture")),
        ("sub", json!("other")),
        ("azp", json!("other-client")),
        ("client_id", json!("other-client")),
        ("client_id", json!(42)),
        ("exp", json!(now.saturating_sub(1))),
        ("scope", json!(42)),
    ] {
        let mut claims = access.clone();
        claims[field] = value;
        cases.push(encode(&header, &claims, &key)?);
    }
    let mut missing = access.clone();
    missing.as_object_mut().ok_or("claims")?.remove("azp");
    cases.push(encode(&header, &missing, &key)?);
    let total = cases.len();
    for invalid in cases {
        *response.lock().map_err(|_| "poisoned")? =
            json!({"id_token":id,"access_token":invalid,"token_type":"Bearer"});
        assert!(
            provider
                .exchange_code_for_resource(
                    "code",
                    &verifier,
                    "http://localhost/callback",
                    "nonce",
                    "https://inventory.fixture"
                )
                .await
                .is_err()
        );
    }
    *response.lock().map_err(|_| "poisoned")? =
        json!({"id_token":id,"access_token":signed,"token_type":"Bearer"});
    assert!(
        provider
            .exchange_code_for_resource(
                "code",
                &verifier,
                "http://localhost/callback",
                "wrong-nonce",
                "https://inventory.fixture"
            )
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), total + 2);
    server.abort();
    let _ = server.await;
    Ok(())
}
