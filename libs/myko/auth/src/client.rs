//! Token acquisition from an external OAuth 2.0 authorization server.
//!
//! Protocol requests are delegated to the `oauth2` and `openidconnect` crates.
//! Myko only selects the grant, applies provider request conventions, and
//! caches the resulting access token for reconnects.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, lock::Mutex};
use oauth2::{
    AuthType, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl, basic::BasicClient,
    basic::BasicTokenType,
};
use openidconnect::{IssuerUrl, core::CoreProviderMetadata};

use crate::{AuthError, CredentialProvider, Credentials};

const MAX_TOKEN_BYTES: usize = 16 * 1024;

/// Where the authorization server's token endpoint comes from.
#[derive(Clone, Debug)]
pub enum TokenEndpoint {
    /// Resolve `token_endpoint` from the issuer's OIDC discovery document
    /// on first use.
    Discover(String),
    /// Use a token endpoint the provider documents at a fixed location.
    Fixed(String),
}

/// How the requested token is bound to the protected API.
#[derive(Clone, Debug, Default)]
pub enum TargetResource {
    /// The authorization server's default target.
    #[default]
    Default,
    /// RFC 8707 `resource` indicator.
    Resource(String),
    /// Auth0-style `audience` parameter.
    Audience(String),
}

/// How the client authenticates to the token endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClientAuth {
    /// `client_secret_basic`, the RFC 6749 default.
    #[default]
    Basic,
    /// `client_secret_post`.
    Post,
}

/// Obtains access tokens with the OAuth 2.0 client credentials grant (RFC 6749 §4.4).
///
/// The resulting identity is the client itself, with no user. Clones share one
/// token cache, so reconnects reuse a valid token and renew it shortly before expiry.
#[derive(Clone)]
pub struct ClientCredentials {
    client_id: ClientId,
    client_secret: ClientSecret,
    client_auth: ClientAuth,
    target: TargetResource,
    scopes: Vec<String>,
    http: reqwest::Client,
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("client_id", &self.client_id.as_str())
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

struct State {
    endpoint: Endpoint,
    token: Option<CachedToken>,
}

enum Endpoint {
    Unresolved(IssuerUrl),
    Resolved(TokenUrl),
}

struct CachedToken {
    token: String,
    renew_at: Instant,
}

impl ClientCredentials {
    /// # Errors
    /// Returns [`AuthError::Unavailable`] for empty client credentials or an endpoint
    /// that is not HTTPS. Plain HTTP is accepted only for loopback development hosts.
    pub fn new(
        endpoint: TokenEndpoint,
        client_id: String,
        client_secret: String,
    ) -> Result<Self, AuthError> {
        if client_id.is_empty() || client_secret.is_empty() {
            return Err(AuthError::Unavailable);
        }
        let endpoint = match endpoint {
            TokenEndpoint::Discover(issuer) => Endpoint::Unresolved(
                IssuerUrl::new(require_secure(&issuer)?).map_err(|_| AuthError::Unavailable)?,
            ),
            TokenEndpoint::Fixed(url) => Endpoint::Resolved(
                TokenUrl::new(require_secure(&url)?).map_err(|_| AuthError::Unavailable)?,
            ),
        };
        Ok(Self {
            client_id: ClientId::new(client_id),
            client_secret: ClientSecret::new(client_secret),
            client_auth: ClientAuth::default(),
            target: TargetResource::default(),
            scopes: Vec::new(),
            http: http_client()?,
            state: Arc::new(Mutex::new(State {
                endpoint,
                token: None,
            })),
        })
    }

    #[must_use]
    pub const fn with_client_auth(mut self, client_auth: ClientAuth) -> Self {
        self.client_auth = client_auth;
        self
    }

    #[must_use]
    pub fn with_target(mut self, target: TargetResource) -> Self {
        self.target = target;
        self
    }

    /// Restricts the requested token to these space-separated scopes.
    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes = scope.into().split_whitespace().map(str::to_owned).collect();
        self
    }

    async fn get(&self) -> Result<Credentials, AuthError> {
        let mut state = self.state.lock().await;
        if let Some(token) = state
            .token
            .as_ref()
            .filter(|token| Instant::now() < token.renew_at)
        {
            return Ok(bearer(token.token.clone()));
        }
        let token_url = self.token_url(&mut state).await?;
        let client = BasicClient::new(self.client_id.clone())
            .set_client_secret(self.client_secret.clone())
            .set_auth_type(match self.client_auth {
                ClientAuth::Basic => AuthType::BasicAuth,
                ClientAuth::Post => AuthType::RequestBody,
            })
            .set_token_uri(token_url);
        let mut request = client
            .exchange_client_credentials()
            .add_scopes(self.scopes.iter().cloned().map(Scope::new));
        match &self.target {
            TargetResource::Default => {}
            TargetResource::Resource(resource) => {
                request = request.add_extra_param("resource", resource.clone());
            }
            TargetResource::Audience(audience) => {
                request = request.add_extra_param("audience", audience.clone());
            }
        }
        let started = Instant::now();
        let response = request
            .request_async(&self.http)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let cached = cache_entry(&response, started)?;
        let credentials = bearer(cached.token.clone());
        state.token = Some(cached);
        drop(state);
        Ok(credentials)
    }

    async fn token_url(&self, state: &mut State) -> Result<TokenUrl, AuthError> {
        let issuer = match &state.endpoint {
            Endpoint::Resolved(url) => return Ok(url.clone()),
            Endpoint::Unresolved(issuer) => issuer.clone(),
        };
        let metadata = CoreProviderMetadata::discover_async(issuer, &self.http)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let url = metadata
            .token_endpoint()
            .ok_or(AuthError::Unavailable)?
            .clone();
        require_secure(url.as_str())?;
        state.endpoint = Endpoint::Resolved(url.clone());
        Ok(url)
    }
}

impl CredentialProvider for ClientCredentials {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>> {
        Box::pin(self.get())
    }
}

fn cache_entry(
    response: &impl TokenResponse<TokenType = BasicTokenType>,
    started: Instant,
) -> Result<CachedToken, AuthError> {
    let token = response.access_token().secret();
    let lifetime = response
        .expires_in()
        .filter(|lifetime| !lifetime.is_zero())
        .ok_or(AuthError::Unavailable)?;
    if token.is_empty()
        || token.len() > MAX_TOKEN_BYTES
        || *response.token_type() != BasicTokenType::Bearer
    {
        return Err(AuthError::Unavailable);
    }
    if started.elapsed() >= lifetime {
        return Err(AuthError::Expired);
    }
    // Renew before expiry so a reconnect never presents a token that lapses in flight.
    let margin = Duration::from_secs((lifetime.as_secs() / 10).clamp(1, 30));
    let renew_at = started
        .checked_add(lifetime.saturating_sub(margin))
        .ok_or(AuthError::Unavailable)?;
    Ok(CachedToken {
        token: token.clone(),
        renew_at,
    })
}

const fn bearer(token: String) -> Credentials {
    Credentials {
        bearer: Some(token),
    }
}

fn require_secure(url: &str) -> Result<String, AuthError> {
    let parsed = url::Url::parse(url).map_err(|_| AuthError::Unavailable)?;
    let loopback = match parsed.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host == "localhost",
        None => false,
    };
    let secure = parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback);
    if !secure || !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AuthError::Unavailable);
    }
    Ok(url.to_owned())
}

fn http_client() -> Result<reqwest::Client, AuthError> {
    // Following redirects would let a token endpoint forward the client secret elsewhere.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| AuthError::Unavailable)
}

#[cfg(test)]
mod tests;
