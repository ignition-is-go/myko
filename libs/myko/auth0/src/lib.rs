//! Auth0 verifies identity. The consuming app supplies local authorization.
//!
//! Access tokens must target the configured API audience. This crate never
//! contacts Auth0 during authorization or treats an ID token as an API credential.
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::future::BoxFuture;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, Credentials, Principal, PrincipalKind,
};
use serde_json::Value;
use tokio::sync::Mutex;

#[cfg(feature = "client-credentials")]
mod machine;
#[cfg(feature = "client-credentials")]
pub use machine::Auth0ClientCredentials;

pub mod login;

const KEY_TTL: Duration = Duration::from_mins(10);
const REFRESH_COOLDOWN: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 16 * 1024;

#[derive(Clone)]
struct Issuer {
    url: reqwest::Url,
    audience: String,
    identifier: String,
}

impl Issuer {
    fn new(issuer: &str, audience: &str) -> Result<Self, AuthError> {
        let mut url = reqwest::Url::parse(issuer).map_err(|_| AuthError::Unavailable)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || audience.trim().is_empty()
        {
            return Err(AuthError::Unavailable);
        }
        url.set_path("/");
        Ok(Self {
            identifier: url.to_string(),
            url,
            audience: audience.to_owned(),
        })
    }

    fn endpoint(&self, path: &str) -> Result<reqwest::Url, AuthError> {
        self.url.join(path).map_err(|_| AuthError::Unavailable)
    }
}

fn http_client() -> Result<reqwest::Client, AuthError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| AuthError::Unavailable)
}

async fn read_json(mut response: reqwest::Response) -> Result<Value, AuthError> {
    if !response.status().is_success() || response.content_length().is_some_and(|n| n > 1_048_576) {
        return Err(AuthError::Unavailable);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| AuthError::Unavailable)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(AuthError::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| AuthError::Unavailable)
}

struct KeyCache {
    keys: JwkSet,
    fetched_at: Option<Instant>,
    last_attempt: Option<Instant>,
}

/// Verifies RS256 API access tokens using cached Auth0 signing keys.
///
/// Keys refresh after ten minutes. Unknown key IDs trigger at most one refresh
/// per thirty seconds. A stale cache fails closed when refreshing fails.
#[derive(Clone)]
pub struct Auth0Authenticator {
    issuer: Issuer,
    client: reqwest::Client,
    keys: Arc<Mutex<KeyCache>>,
    jwks_uri: reqwest::Url,
}

/// General RS256/OIDC verification using an explicitly trusted JWKS endpoint.
pub type JwtVerifier = Auth0Authenticator;

impl Auth0Authenticator {
    /// `issuer` is an HTTPS tenant origin. `audience` is the API identifier.
    ///
    /// # Errors
    /// Returns an error for invalid configuration or HTTP client setup.
    pub fn new(issuer: &str, audience: &str) -> Result<Self, AuthError> {
        Self::build(issuer, audience, JwkSet { keys: Vec::new() }, false)
    }

    /// Seeds keys from a trusted source, such as an app's startup key fetch.
    /// The same expiry and refresh rules apply to seeded and downloaded keys.
    ///
    /// # Errors
    /// Returns an error for invalid configuration or more than 128 keys.
    pub fn with_jwks(issuer: &str, audience: &str, keys: JwkSet) -> Result<Self, AuthError> {
        Self::build(issuer, audience, keys, true)
    }

    /// Configure a trusted OIDC signing-key endpoint. Loopback HTTP is permitted
    /// for local providers; production endpoints require HTTPS.
    /// # Errors
    /// Rejects unsafe endpoint configuration or an empty audience.
    pub fn with_jwks_uri(issuer: &str, audience: &str, jwks_uri: &str) -> Result<Self, AuthError> {
        Self::with_jwks_uri_and_client(issuer, audience, jwks_uri, http_client()?)
    }

    /// Configure a trusted key endpoint using the caller's HTTP trust roots and policy.
    /// # Errors
    /// Rejects unsafe endpoint configuration or an empty audience.
    pub fn with_jwks_uri_and_client(
        issuer: &str,
        audience: &str,
        jwks_uri: &str,
        client: reqwest::Client,
    ) -> Result<Self, AuthError> {
        let url = login::endpoint(issuer)?;
        let jwks_uri = login::endpoint(jwks_uri)?;
        if audience.trim().is_empty() {
            return Err(AuthError::Unavailable);
        }
        Ok(Self {
            issuer: Issuer {
                url,
                audience: audience.into(),
                identifier: issuer.into(),
            },
            jwks_uri,
            client,
            keys: Arc::new(Mutex::new(KeyCache {
                keys: JwkSet { keys: Vec::new() },
                fetched_at: None,
                last_attempt: None,
            })),
        })
    }

    /// Bind another expected audience while sharing the signing-key cache.
    /// # Errors
    /// Rejects an empty audience.
    pub fn for_audience(&self, audience: &str) -> Result<Self, AuthError> {
        if audience.trim().is_empty() {
            return Err(AuthError::Unavailable);
        }
        let mut verifier = self.clone();
        verifier.issuer.audience = audience.into();
        Ok(verifier)
    }

    fn build(issuer: &str, audience: &str, keys: JwkSet, seeded: bool) -> Result<Self, AuthError> {
        if keys.keys.len() > 128 {
            return Err(AuthError::Unavailable);
        }
        let now = seeded.then(Instant::now);
        Ok(Self {
            jwks_uri: Issuer::new(issuer, audience)?.endpoint(".well-known/jwks.json")?,
            issuer: Issuer::new(issuer, audience)?,
            client: http_client()?,
            keys: Arc::new(Mutex::new(KeyCache {
                keys,
                fetched_at: now,
                last_attempt: now,
            })),
        })
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        let mut cache = self.keys.lock().await;
        let fresh = cache.fetched_at.is_some_and(|at| at.elapsed() < KEY_TTL);
        if fresh && let Some(key) = cache.keys.find(kid) {
            return decoding_key(key);
        }
        if cache
            .last_attempt
            .is_some_and(|at| at.elapsed() < REFRESH_COOLDOWN)
        {
            return Err(if fresh {
                AuthError::Unauthenticated
            } else {
                AuthError::Unavailable
            });
        }
        cache.last_attempt = Some(Instant::now());
        let keys = self.fetch_keys().await?;
        cache.keys = keys;
        cache.fetched_at = Some(Instant::now());
        cache
            .keys
            .find(kid)
            .ok_or(AuthError::Unauthenticated)
            .and_then(decoding_key)
    }

    async fn fetch_keys(&self) -> Result<JwkSet, AuthError> {
        let response = self
            .client
            .get(self.jwks_uri.clone())
            .send()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let keys: JwkSet = serde_json::from_value(read_json(response).await?)
            .map_err(|_| AuthError::Unavailable)?;
        if keys.keys.is_empty()
            || keys.keys.len() > 128
            || !keys.keys.iter().any(|key| decoding_key(key).is_ok())
        {
            return Err(AuthError::Unavailable);
        }
        Ok(keys)
    }

    /// Load trusted signing keys before accepting traffic.
    /// # Errors
    /// Rejects unavailable or invalid key documents.
    pub async fn preload(&self) -> Result<(), AuthError> {
        let mut cache = self.keys.lock().await;
        cache.last_attempt = Some(Instant::now());
        cache.keys = self.fetch_keys().await?;
        cache.fetched_at = Some(Instant::now());
        drop(cache);
        Ok(())
    }

    /// Verify signature, signing-key restrictions, issuer, audience, expiry and not-before.
    /// # Errors
    /// Rejects invalid credentials or unavailable signing-key authority.
    pub async fn verify_claims(&self, token: &str) -> Result<Value, AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::Unauthenticated);
        }
        let header = decode_header(token).map_err(|_| AuthError::Unauthenticated)?;
        if header.alg != Algorithm::RS256 {
            return Err(AuthError::Unauthenticated);
        }
        let kid = header
            .kid
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::Unauthenticated)?;
        let key = self.key(&kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.leeway = 0;
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.set_issuer(&[self.issuer.identifier.as_str()]);
        validation.set_audience(&[self.issuer.audience.as_str()]);
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|error| {
                if matches!(
                    error.kind(),
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature
                ) {
                    AuthError::Expired
                } else {
                    AuthError::Unauthenticated
                }
            })?
            .claims;
        if claims
            .get("sub")
            .and_then(Value::as_str)
            .is_none_or(|subject| subject.trim().is_empty())
        {
            return Err(AuthError::Unauthenticated);
        }
        Ok(claims)
    }

    /// Enforce OIDC authorized-party and nonce binding in addition to JWT validation.
    /// # Errors
    /// Rejects tokens for another client or login transaction.
    pub async fn verify_id_token(
        &self,
        token: &str,
        nonce: Option<&str>,
    ) -> Result<Value, AuthError> {
        let claims = self.verify_claims(token).await?;
        let azp = claims.get("azp").and_then(Value::as_str);
        if azp.is_some_and(|azp| azp != self.issuer.audience)
            || (claims
                .get("aud")
                .and_then(Value::as_array)
                .is_some_and(|aud| aud.len() > 1)
                && azp != Some(self.issuer.audience.as_str()))
            || nonce.is_some_and(|nonce| claims.get("nonce").and_then(Value::as_str) != Some(nonce))
        {
            return Err(AuthError::Unauthenticated);
        }
        Ok(claims)
    }

    async fn verify(&self, credentials: Credentials) -> Result<AuthenticatedIdentity, AuthError> {
        let token = credentials.bearer.ok_or(AuthError::Unauthenticated)?;
        let claims = self.verify_claims(&token).await?;
        let subject = claims
            .get("sub")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::Unauthenticated)?
            .to_owned();
        let exp = claims
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or(AuthError::Unauthenticated)?;
        let expires_at = UNIX_EPOCH
            .checked_add(Duration::from_secs(exp))
            .ok_or(AuthError::Unauthenticated)?;
        if expires_at <= SystemTime::now() {
            return Err(AuthError::Expired);
        }
        let principal_kind = if claims.get("gty").and_then(Value::as_str)
            == Some("client-credentials")
            || subject.ends_with("@clients")
        {
            PrincipalKind::Service
        } else {
            PrincipalKind::User
        };
        let client_id = claims
            .get("azp")
            .or_else(|| claims.get("client_id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let scopes = claims
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        Ok(AuthenticatedIdentity {
            user_id: None,
            principal: Principal {
                issuer: self.issuer.identifier.clone(),
                subject,
                kind: principal_kind,
            },
            client_id,
            scopes,
            delegation: Vec::new(),
            claims,
            expires_at,
        })
    }
}

fn decoding_key(jwk: &jsonwebtoken::jwk::Jwk) -> Result<DecodingKey, AuthError> {
    let value = serde_json::to_value(jwk).map_err(|_| AuthError::Unauthenticated)?;
    if value.get("kty").and_then(Value::as_str) != Some("RSA")
        || value.get("alg").is_some_and(|v| v != "RS256")
        || value.get("use").is_some_and(|v| v != "sig")
        || value.get("key_ops").is_some_and(|v| {
            !v.as_array()
                .is_some_and(|ops| ops.iter().any(|op| op == "verify"))
        })
    {
        return Err(AuthError::Unauthenticated);
    }
    DecodingKey::from_jwk(jwk).map_err(|_| AuthError::Unauthenticated)
}

impl Authenticator for Auth0Authenticator {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(self.verify(credentials))
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[tokio::test]
    async fn login_discovery_and_signing_keys_share_the_supplied_http_client() -> Result {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let issuer = format!("http://{}/", listener.local_addr()?);
        let source = issuer.clone();
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await?;
                let mut bytes = [0u8; 4096];
                let n = stream.read(&mut bytes).await?;
                let request = std::str::from_utf8(bytes.get(..n).ok_or("request")?)?;
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains("x-provider-policy: fixture\r\n")
                );
                let body = if index == 0 {
                    assert!(request.starts_with("GET /.well-known/openid-configuration "));
                    serde_json::json!({"issuer":source,"jwks_uri":format!("{source}keys")})
                        .to_string()
                } else {
                    assert!(request.starts_with("GET /keys "));
                    include_str!("../tests/fixtures/jwks.json").to_owned()
                };
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-provider-policy",
            reqwest::header::HeaderValue::from_static("fixture"),
        );
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(5))
            .build()?;
        login::LoginProvider::discover(
            login::LoginConfig {
                issuer,
                client_id: "client".into(),
                client_secret: "secret".into(),
            },
            http,
        )
        .await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn downloaded_keys_are_cached_and_unknown_keys_cannot_force_repeated_fetches() -> Result {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = [0u8; 4096];
            let n = stream.read(&mut bytes).await?;
            assert!(
                std::str::from_utf8(bytes.get(..n).ok_or("request")?)?
                    .starts_with("GET /.well-known/jwks.json HTTP/1.1\r\n")
            );
            let body = include_str!("../tests/fixtures/jwks.json");
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let provider = Auth0Authenticator::with_jwks_uri(
            &format!("http://{address}/"),
            "myko-api",
            &format!("http://{address}/.well-known/jwks.json"),
        )?;
        let claims = serde_json::json!({"iss":provider.issuer.url.as_str(),"aud":"myko-api","sub":"user-1","exp":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs().saturating_add(300)});
        let key = EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/test-key.pem"))?;
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("fixture-key".to_owned());
        let token = encode(&header, &claims, &key)?;
        assert_eq!(
            provider
                .authenticate(Credentials {
                    bearer: Some(token.clone())
                })
                .await?
                .principal
                .subject,
            "user-1"
        );
        server.await??;
        assert_eq!(
            provider
                .authenticate(Credentials {
                    bearer: Some(token)
                })
                .await?
                .principal
                .subject,
            "user-1"
        );
        for kid in ["unknown-a", "unknown-b"] {
            header.kid = Some(kid.to_owned());
            let token = encode(&header, &claims, &key)?;
            assert!(matches!(
                provider
                    .authenticate(Credentials {
                        bearer: Some(token)
                    })
                    .await,
                Err(AuthError::Unauthenticated)
            ));
        }
        Ok(())
    }
}
