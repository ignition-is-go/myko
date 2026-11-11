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
