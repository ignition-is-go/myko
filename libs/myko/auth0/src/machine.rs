use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::BoxFuture;
use myko_auth::{AuthError, CredentialProvider, Credentials};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{Issuer, http_client, read_json};

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

struct CachedToken {
    token: String,
    renew_at: Instant,
}

/// Obtains API access tokens using Auth0's client credentials grant.
///
/// Clones share the token cache. Reconnects reuse a valid token and renew it
/// before expiry. The client secret is sent only to the configured HTTPS issuer.
#[derive(Clone)]
pub struct Auth0ClientCredentials {
    issuer: Issuer,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    client: reqwest::Client,
    token: Arc<Mutex<Option<CachedToken>>>,
}

impl fmt::Debug for Auth0ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth0ClientCredentials")
            .field("issuer", &self.issuer.url)
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl Auth0ClientCredentials {
    /// Creates a machine credential provider for the configured API.
    ///
    /// # Errors
    /// Returns an error for invalid configuration or empty client credentials.
    pub fn new(
        issuer: &str,
        audience: &str,
        client_id: String,
        client_secret: String,
    ) -> Result<Self, AuthError> {
        if client_id.is_empty() || client_secret.is_empty() {
            return Err(AuthError::Unavailable);
        }
        Ok(Self {
            issuer: Issuer::new(issuer, audience)?,
            client_id,
            client_secret,
            scope: None,
            client: http_client()?,
            token: Arc::new(Mutex::new(None)),
        })
    }

    /// Restricts the requested token to these space-separated API scopes.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    async fn get(&self) -> Result<Credentials, AuthError> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached
            .as_ref()
            .filter(|token| Instant::now() < token.renew_at)
        {
            return Ok(Credentials {
                bearer: Some(token.token.clone()),
            });
        }
        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("audience", self.issuer.audience.as_str()),
        ];
        if let Some(scope) = &self.scope {
            form.push(("scope", scope.as_str()));
        }
        let started = Instant::now();
        let response = self
            .client
            .post(self.issuer.endpoint("oauth/token")?)
            .form(&form)
            .send()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let response: TokenResponse = serde_json::from_value(read_json(response).await?)
            .map_err(|_| AuthError::Unavailable)?;
        if response.access_token.is_empty()
            || response.access_token.len() > crate::MAX_TOKEN_BYTES
            || !response.token_type.eq_ignore_ascii_case("Bearer")
            || response.expires_in == 0
        {
            return Err(AuthError::Unavailable);
        }
        let lifetime = Duration::from_secs(response.expires_in);
        let margin = Duration::from_secs((response.expires_in / 10).clamp(1, 30));
        let renew_at = started
            .checked_add(lifetime.saturating_sub(margin))
            .ok_or(AuthError::Unavailable)?;
        if started.elapsed() >= lifetime {
            return Err(AuthError::Expired);
        }
        let credentials = Credentials {
            bearer: Some(response.access_token.clone()),
        };
        *cached = Some(CachedToken {
            token: response.access_token,
            renew_at,
        });
        drop(cached);
        Ok(credentials)
    }
}

impl CredentialProvider for Auth0ClientCredentials {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>> {
        Box::pin(self.get())
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    async fn fixture(
        expires_in: u64,
        tokens: &[&str],
    ) -> Result<(
        Auth0ClientCredentials,
        tokio::task::JoinHandle<Result<usize>>,
    )> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let tokens: Vec<String> = tokens.iter().map(|token| (*token).to_owned()).collect();
        let handle = tokio::spawn(async move {
            let mut count = 0usize;
            for token in tokens {
                let (mut stream, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = stream.read(&mut chunk).await?;
                    if n == 0 {
                        return Err("incomplete request".into());
                    }
                    bytes.extend_from_slice(chunk.get(..n).ok_or("chunk")?);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(bytes)?;
                let headers_end = request.find("\r\n\r\n").ok_or("headers")?.saturating_add(4);
                let length = request
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .and_then(|n| n.parse::<usize>().ok())
                    })
                    .ok_or("content length")?;
                let mut body = request
                    .get(headers_end..)
                    .ok_or("body")?
                    .as_bytes()
                    .to_vec();
                while body.len() < length {
                    let n = stream.read(&mut chunk).await?;
                    if n == 0 {
                        return Err("incomplete body".into());
                    }
                    body.extend_from_slice(chunk.get(..n).ok_or("chunk")?);
                }
                let body = String::from_utf8(body)?;
                assert!(request.starts_with("POST /oauth/token HTTP/1.1\r\n"));
                assert!(body.contains("grant_type=client_credentials"));
                assert!(body.contains("audience=myko-api"));
                assert!(body.contains("client_id=machine-1"));
                assert!(body.contains("client_secret=fixture-secret"));
                let response = serde_json::json!({"access_token":token,"token_type":"Bearer","expires_in":expires_in}).to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await?;
                count = count.saturating_add(1);
            }
            Ok(count)
        });
        let mut provider = Auth0ClientCredentials::new(
            "https://tenant.example/",
            "myko-api",
            "machine-1".to_owned(),
            "fixture-secret".to_owned(),
        )?;
        provider.issuer.url = reqwest::Url::parse(&format!("http://{address}/"))?;
        Ok((provider, handle))
    }

    #[tokio::test]
    async fn repeated_connections_share_cached_machine_credentials() -> Result {
        let (provider, requests) = fixture(60, &["first-token"]).await?;
        assert_eq!(
            provider.credentials().await?.bearer.as_deref(),
            Some("first-token")
        );
        assert_eq!(requests.await??, 1);
        assert_eq!(
            provider.clone().credentials().await?.bearer.as_deref(),
            Some("first-token")
        );
        Ok(())
    }

    #[tokio::test]
    async fn tokens_inside_renewal_margin_are_replaced() -> Result {
        let (provider, requests) = fixture(1, &["first-token", "second-token"]).await?;
        assert_eq!(
            provider.credentials().await?.bearer.as_deref(),
            Some("first-token")
        );
        assert_eq!(
            provider.credentials().await?.bearer.as_deref(),
            Some("second-token")
        );
        assert_eq!(requests.await??, 2);
        Ok(())
    }

    #[tokio::test]
    async fn zero_lifetime_tokens_are_rejected() -> Result {
        let (provider, requests) = fixture(0, &["invalid-token"]).await?;
        assert!(matches!(
            provider.credentials().await,
            Err(AuthError::Unavailable)
        ));
        assert_eq!(requests.await??, 1);
        Ok(())
    }
}
