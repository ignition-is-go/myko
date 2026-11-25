use std::{sync::Arc, time::Instant};

use futures::{future::BoxFuture, lock::Mutex};
use oauth2::{
    AuthType, AuthUrl, AuthorizationCode, ClientId, CsrfToken, DeviceAuthorizationUrl,
    PkceCodeChallenge, RedirectUrl, RefreshToken, RequestTokenError, Scope,
    StandardDeviceAuthorizationResponse, TokenResponse, TokenUrl,
    basic::{BasicClient, BasicErrorResponseType, BasicTokenResponse},
};

use super::{CachedToken, TargetResource, bearer, cache_entry, http_client, require_secure};
use crate::{AuthError, CredentialProvider, Credentials};

/// Endpoints and scopes for a public native OAuth client.
pub struct DeviceCodeConfig {
    pub client_id: String,
    pub device_authorization_url: String,
    pub token_url: String,
    pub target: TargetResource,
    pub scopes: Vec<String>,
}

/// User-facing information that can be displayed on a device without a browser.
pub struct DevicePrompt {
    pub verification_uri: String,
    pub user_code: String,
}

/// Stores a refresh token in app-owned secure storage scoped to the issuer,
/// client, target, and account. Implementations must not log the token.
pub trait DeviceTokenStore: Send + Sync {
    /// # Errors
    /// Returns an error if secure storage cannot be read.
    fn load(&self) -> Result<Option<String>, AuthError>;
    /// # Errors
    /// Returns an error if secure storage cannot be updated.
    fn save(&self, token: Option<&str>) -> Result<(), AuthError>;
}

#[derive(Default)]
struct Session {
    access: Option<CachedToken>,
    refresh: Option<RefreshToken>,
}

/// A user credential provider for the OAuth device authorization grant.
#[derive(Clone)]
pub struct DeviceCode {
    client_id: ClientId,
    device_url: DeviceAuthorizationUrl,
    token_url: TokenUrl,
    target: TargetResource,
    scopes: Vec<Scope>,
    http: reqwest::Client,
    session: Arc<Mutex<Session>>,
    store: Option<Arc<dyn DeviceTokenStore>>,
}

impl DeviceCode {
    /// Construct a provider and restore its saved refresh token, if any.
    ///
    /// # Errors
    /// Returns an error for invalid endpoints or unavailable secure storage.
    pub fn new(
        config: DeviceCodeConfig,
        store: Option<Arc<dyn DeviceTokenStore>>,
    ) -> Result<Self, AuthError> {
        if config.client_id.trim().is_empty() {
            return Err(AuthError::Unavailable);
        }
        let refresh = store
            .as_ref()
            .map(|store| store.load())
            .transpose()?
            .flatten();
        Ok(Self {
            client_id: ClientId::new(config.client_id),
            device_url: DeviceAuthorizationUrl::new(require_secure(
                &config.device_authorization_url,
            )?)
            .map_err(|_| AuthError::Unavailable)?,
            token_url: TokenUrl::new(require_secure(&config.token_url)?)
                .map_err(|_| AuthError::Unavailable)?,
            target: config.target,
            scopes: config.scopes.into_iter().map(Scope::new).collect(),
            http: http_client()?,
            session: Arc::new(Mutex::new(Session {
                access: None,
                refresh: refresh.map(RefreshToken::new),
            })),
            store,
        })
    }

    /// Prompt once and wait for the user to approve on a separate device.
    ///
    /// # Errors
    /// Returns an error if authorization is declined, expires, or cannot be completed.
    pub async fn sign_in(&self, prompt: impl FnOnce(DevicePrompt) + Send) -> Result<(), AuthError> {
        let mut session = self.session.lock().await;
        let client = BasicClient::new(self.client_id.clone())
            .set_auth_type(AuthType::RequestBody)
            .set_token_uri(self.token_url.clone())
            .set_device_authorization_url(self.device_url.clone());
        let mut request = client
            .exchange_device_code()
            .add_scopes(self.scopes.clone());
        match &self.target {
            TargetResource::Default => {}
            TargetResource::Resource(resource) => {
                request = request.add_extra_param("resource", resource.clone());
            }
            TargetResource::Audience(audience) => {
                request = request.add_extra_param("audience", audience.clone());
            }
        }
        let details: StandardDeviceAuthorizationResponse = request
            .request_async(&self.http)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        prompt(DevicePrompt {
            verification_uri: require_secure(details.verification_uri().as_str())?,
            user_code: details.user_code().secret().clone(),
        });
        let response = client
            .exchange_device_access_token(&details)
            .request_async(&self.http, tokio::time::sleep, None)
            .await
            .map_err(|error| match error {
                RequestTokenError::ServerResponse(response) => match response.error() {
                    oauth2::DeviceCodeErrorResponseType::AccessDenied => AuthError::Forbidden,
                    oauth2::DeviceCodeErrorResponseType::ExpiredToken => AuthError::Expired,
                    _ => AuthError::Unavailable,
                },
                _ => AuthError::Unavailable,
            })?;
        *session = Session::default();
        let result = self.accept(&mut session, &response, Instant::now());
        drop(session);
        result
    }

    /// Sign in using the system browser and a loopback callback.
    ///
    /// # Errors
    /// Returns an error if the callback cannot be bound or authorization fails.
    pub async fn sign_in_browser(
        &self,
        authorization_url: &str,
        callback_port: u16,
        open: impl FnOnce(String) -> Result<(), AuthError> + Send,
    ) -> Result<(), AuthError> {
        let mut session = self.session.lock().await;
        let listener =
            tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, callback_port))
                .await
                .map_err(|_| AuthError::Unavailable)?;
        let callback_port = listener
            .local_addr()
            .map_err(|_| AuthError::Unavailable)?
            .port();
        let redirect = RedirectUrl::new(format!("http://127.0.0.1:{callback_port}/oauth/callback"))
            .map_err(|_| AuthError::Unavailable)?;
        let client = BasicClient::new(self.client_id.clone())
            .set_auth_type(AuthType::RequestBody)
            .set_auth_uri(
                AuthUrl::new(require_secure(authorization_url)?)
                    .map_err(|_| AuthError::Unavailable)?,
            )
            .set_redirect_uri(redirect)
            .set_token_uri(self.token_url.clone());
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let mut request = client
            .authorize_url(CsrfToken::new_random)
            .set_pkce_challenge(challenge)
            .add_scopes(self.scopes.clone());
        match &self.target {
            TargetResource::Default => {}
            TargetResource::Resource(resource) => {
                request = request.add_extra_param("resource", resource);
            }
            TargetResource::Audience(audience) => {
                request = request.add_extra_param("audience", audience);
            }
        }
        let (url, state) = request.url();
        open(url.to_string())?;
        let code = tokio::time::timeout(
            std::time::Duration::from_mins(3),
            callback_code(&listener, &state),
        )
        .await
        .map_err(|_| AuthError::Expired)??;
        let started = Instant::now();
        let response = client
            .exchange_code(code)
            .set_pkce_verifier(verifier)
            .request_async(&self.http)
            .await
            .map_err(|_| AuthError::Unavailable)?;
        *session = Session::default();
        let result = self.accept(&mut session, &response, started);
        drop(session);
        result
    }

    /// Clear locally cached credentials and the saved refresh token.
    ///
    /// # Errors
    /// Returns an error if the secure store cannot remove the saved credential.
    pub async fn sign_out(&self) -> Result<(), AuthError> {
        let mut session = self.session.lock().await;
        if let Some(store) = &self.store {
            store.save(None)?;
        }
        *session = Session::default();
        drop(session);
        Ok(())
    }

    fn accept(
        &self,
        session: &mut Session,
        response: &BasicTokenResponse,
        started: Instant,
    ) -> Result<(), AuthError> {
        let token = cache_entry(response, started)?;
        if let Some(refresh) = response.refresh_token() {
            session.refresh = Some(refresh.clone());
        }
        if let Some(store) = &self.store {
            store.save(
                session
                    .refresh
                    .as_ref()
                    .map(|token| token.secret().as_str()),
            )?;
        }
        session.access = Some(token);
        Ok(())
    }

    async fn get(&self) -> Result<Credentials, AuthError> {
        let mut session = self.session.lock().await;
        if let Some(access) = session
            .access
            .as_ref()
            .filter(|token| Instant::now() < token.renew_at)
        {
            return Ok(bearer(access.token.clone()));
        }
        let refresh = session.refresh.as_ref().ok_or(AuthError::Unauthenticated)?;
        let client = BasicClient::new(self.client_id.clone())
            .set_auth_type(AuthType::RequestBody)
            .set_token_uri(self.token_url.clone());
        let started = Instant::now();
        let response = match client
            .exchange_refresh_token(refresh)
            .request_async(&self.http)
            .await
        {
            Ok(response) => response,
            Err(RequestTokenError::ServerResponse(response))
                if *response.error() == BasicErrorResponseType::InvalidGrant =>
            {
                *session = Session::default();
                if let Some(store) = &self.store {
                    store.save(None)?;
                }
                return Err(AuthError::Revoked);
            }
            Err(_) => return Err(AuthError::Unavailable),
        };
        self.accept(&mut session, &response, started)?;
        session
            .access
            .as_ref()
            .map(|token| bearer(token.token.clone()))
            .ok_or(AuthError::Unavailable)
    }
}

async fn callback_code(
    listener: &tokio::net::TcpListener,
    state: &CsrfToken,
) -> Result<AuthorizationCode, AuthError> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let mut line = String::new();
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::io::BufReader::new((&mut stream).take(8192)).read_line(&mut line),
        )
        .await;
        let parsed = matches!(read, Ok(Ok(1..)))
            .then(|| line.strip_prefix("GET /oauth/callback?"))
            .flatten()
            .and_then(|path| path.split_once(' ').map(|(query, _)| query))
            .map(|query| {
                url::form_urlencoded::parse(query.as_bytes())
                    .collect::<std::collections::HashMap<_, _>>()
            });
        if let Some(params) = parsed
            && params
                .get("state")
                .is_some_and(|value| value.as_ref() == state.secret())
        {
            let response = if params.contains_key("code") {
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nCache-Control: no-store\r\nConnection: close\r\n\r\nReturn to the application to check sign-in status."
            } else {
                "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\nSign-in declined."
            };
            let _ = stream.write_all(response.as_bytes()).await;
            return params
                .get("code")
                .map(|code| AuthorizationCode::new(code.to_string()))
                .ok_or(AuthError::Forbidden);
        }
        let _ = stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\nInvalid sign-in callback.",
            )
            .await;
    }
}

impl CredentialProvider for DeviceCode {
    fn credentials(&self) -> BoxFuture<'_, Result<Credentials, AuthError>> {
        Box::pin(self.get())
    }
}
