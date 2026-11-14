use std::fmt;

use futures::future::BoxFuture;
use myko_auth::{
    AuthError, CredentialProvider, Credentials,
    client::{ClientAuth, ClientCredentials, TargetResource, TokenEndpoint},
};

use crate::Issuer;

/// Obtains API access tokens using Auth0's client credentials grant.
///
/// This is [`ClientCredentials`] configured for Auth0's fixed token endpoint,
/// `client_secret_post`, and `audience` parameter. Clones share the token cache.
/// Reconnects reuse a valid token and renew it before expiry. The client secret
/// is sent only to the configured HTTPS issuer.
#[derive(Clone)]
pub struct Auth0ClientCredentials {
    issuer: String,
    inner: ClientCredentials,
}

impl fmt::Debug for Auth0ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth0ClientCredentials")
            .field("issuer", &self.issuer)
            .field("inner", &self.inner)
            .finish()
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
        Self::for_issuer(&Issuer::new(issuer, audience)?, client_id, client_secret)
    }

    fn for_issuer(
        issuer: &Issuer,
        client_id: String,
        client_secret: String,
    ) -> Result<Self, AuthError> {
        let inner = ClientCredentials::new(
            TokenEndpoint::Fixed(issuer.endpoint("oauth/token")?.to_string()),
            client_id,
            client_secret,
        )?
        .with_client_auth(ClientAuth::Post)
        .with_target(TargetResource::Audience(issuer.audience.clone()));
        Ok(Self {
            issuer: issuer.url.to_string(),
            inner,
        })
    }

    /// Restricts the requested token to these space-separated API scopes.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.inner = self.inner.with_scope(scope);
        self
    }
}

impl CredentialProvider for Auth0ClientCredentials {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>> {
        self.inner.credentials()
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
        let url = reqwest::Url::parse(&format!("http://{address}/"))?;
        let issuer = Issuer {
            identifier: url.to_string(),
            url,
            audience: "myko-api".to_owned(),
        };
        let provider = Auth0ClientCredentials::for_issuer(
            &issuer,
            "machine-1".to_owned(),
            "fixture-secret".to_owned(),
        )?;
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
