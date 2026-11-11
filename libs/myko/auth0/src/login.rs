//! Auth0 login methods using shared OIDC verification and PKCE binding.
use crate::{JwtVerifier, read_json};
use myko_auth::AuthError;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

/// Host-owned provider configuration. Login admission policy belongs to the host.
#[derive(Clone)]
pub struct LoginConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginError {
    InvalidRequest,
    ProviderClientRejected,
    ProviderCodeRejected,
    TokenExchangeRejected,
    IdentityValidationRejected,
    Unavailable,
}
impl std::fmt::Display for LoginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LoginError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderEmailError {
    Configuration(&'static str),
    InvalidRequest,
    InvalidCode,
    Denied,
    RateLimited(Option<Duration>),
    Unavailable,
}

pub struct LoginProvider {
    config: LoginConfig,
    http: reqwest::Client,
    verifier: JwtVerifier,
}
impl LoginProvider {
    /// Configure an explicitly trusted Auth0 key endpoint.
    /// # Errors
    /// Rejects invalid configuration.
    pub fn new(
        mut config: LoginConfig,
        http: reqwest::Client,
        jwks_uri: &str,
    ) -> Result<Self, AuthError> {
        config.issuer = normalize_issuer(&config.issuer)?;
        if config.client_id.trim().is_empty() || config.client_secret.trim().is_empty() {
            return Err(AuthError::Unavailable);
        }
        let verifier = JwtVerifier::with_jwks_uri_and_client(
            &config.issuer,
            &config.client_id,
            jwks_uri,
            http.clone(),
        )?;
        Ok(Self {
            config,
            http,
            verifier,
        })
    }
    /// Validate discovery's issuer and preload trusted keys before admission.
    /// # Errors
    /// Rejects unavailable or inconsistent discovery/key documents.
    pub async fn discover(
        mut config: LoginConfig,
        http: reqwest::Client,
    ) -> Result<Self, AuthError> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        config.issuer = normalize_issuer(&config.issuer)?;
        let response = http
            .get(format!("{}.well-known/openid-configuration", config.issuer))
            .send()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        let discovery: Discovery = serde_json::from_value(read_json(response).await?)
            .map_err(|_| AuthError::Unavailable)?;
        if discovery.issuer != config.issuer {
            return Err(AuthError::Unauthenticated);
        }
        let provider = Self::new(config, http, &discovery.jwks_uri)?;
        provider.verifier.preload().await?;
        Ok(provider)
    }
    /// Redeem a code with PKCE and enforce the ID token's login nonce and client.
    /// # Errors
    /// Rejects malformed requests, provider denials, or invalid ID tokens.
    pub async fn exchange_code(
        &self,
        code: &str,
        verifier: &str,
        redirect_uri: &str,
        nonce: &str,
    ) -> Result<Value, LoginError> {
        if code.is_empty()
            || code.len() > 4096
            || !(43..=128).contains(&verifier.len())
            || !verifier
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
        {
            return Err(LoginError::InvalidRequest);
        }
        let response = self
            .http
            .post(format!("{}oauth/token", self.config.issuer))
            .timeout(Duration::from_secs(15))
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", &self.config.client_id),
                ("client_secret", &self.config.client_secret),
                ("code", code),
                ("code_verifier", verifier),
                ("redirect_uri", redirect_uri),
            ])
            .send()
            .await
            .map_err(|_| LoginError::Unavailable)?;
        if !response.status().is_success() {
            let body = response.json::<Value>().await.unwrap_or_default();
            return Err(match body.get("error").and_then(Value::as_str) {
                Some("invalid_client" | "unauthorized_client") => {
                    LoginError::ProviderClientRejected
                }
                Some("invalid_grant") => LoginError::ProviderCodeRejected,
                _ => LoginError::TokenExchangeRejected,
            });
        }
        let body = read_json(response)
            .await
            .map_err(|_| LoginError::Unavailable)?;
        let token = body
            .get("id_token")
            .and_then(Value::as_str)
            .ok_or(LoginError::IdentityValidationRejected)?;
        self.verify_id_token(token, Some(nonce))
            .await
            .map_err(|_| LoginError::IdentityValidationRejected)
    }
    /// Verify ID-token signature, issuer, audience, nonce and authorized party.
    /// # Errors
    /// Rejects invalid or unavailable identity assertions.
    pub async fn verify_id_token(
        &self,
        token: &str,
        nonce: Option<&str>,
    ) -> Result<Value, AuthError> {
        self.verifier.verify_id_token(token, nonce).await
    }
    /// Verify an API token for a host-selected resource audience.
    /// # Errors
    /// Rejects invalid tokens or signing-key failures.
    pub async fn verify_access_token(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<Value, AuthError> {
        self.verifier
            .for_audience(audience)?
            .verify_claims(token)
            .await
    }
    /// Request Auth0's passwordless email code. Host policy must authorize the address first.
    /// # Errors
    /// Rejects malformed addresses or provider failures.
    pub async fn send_email(&self, email: &str) -> Result<(), ProviderEmailError> {
        normalize_email(email).ok_or(ProviderEmailError::InvalidRequest)?;
        let response=self.http.post(format!("{}passwordless/start",self.config.issuer)).timeout(Duration::from_secs(15))
            .json(&serde_json::json!({"client_id":self.config.client_id,"client_secret":self.config.client_secret,"connection":"email","send":"code","email":email}))
            .send().await.map_err(|_|ProviderEmailError::Unavailable)?;
        provider_ok(response).await.map_err(|e| match e {
            ProviderEmailError::InvalidCode=>ProviderEmailError::Configuration("Auth0 rejected the email code request. Check the passwordless email connection and application configuration."),other=>other,
        })?;
        Ok(())
    }
    /// Exchange an email code and bind the verified token to the requested address.
    /// # Errors
    /// Rejects invalid codes, email mismatches, or provider failures.
    pub async fn verify_email(&self, email: &str, code: &str) -> Result<Value, ProviderEmailError> {
        if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ProviderEmailError::InvalidRequest);
        }
        let email = normalize_email(email).ok_or(ProviderEmailError::InvalidRequest)?;
        let response=self.http.post(format!("{}oauth/token",self.config.issuer)).timeout(Duration::from_secs(15))
            .json(&serde_json::json!({"grant_type":"http://auth0.com/oauth/grant-type/passwordless/otp","client_id":self.config.client_id,"client_secret":self.config.client_secret,
                "realm":"email","username":email,"otp":code,"scope":"openid profile email"}))
            .send().await.map_err(|_|ProviderEmailError::Unavailable)?;
        let response = provider_ok(response).await?;
        let body = read_json(response)
            .await
            .map_err(|_| ProviderEmailError::Unavailable)?;
        let token = body
            .get("id_token")
            .and_then(Value::as_str)
            .ok_or(ProviderEmailError::Unavailable)?;
        let claims = self
            .verify_id_token(token, None)
            .await
            .map_err(|_| ProviderEmailError::Unavailable)?;
        if claims.get("email_verified") != Some(&Value::Bool(true))
            || claims
                .get("email")
                .and_then(Value::as_str)
                .and_then(normalize_email)
                .as_deref()
                != Some(&email)
        {
            return Err(ProviderEmailError::Denied);
        }
        Ok(claims)
    }
}

pub(crate) fn endpoint(value: &str) -> Result<reqwest::Url, AuthError> {
    let url = reqwest::Url::parse(value.trim()).map_err(|_| AuthError::Unavailable)?;
    let local = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "https" || (url.scheme() == "http" && local))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AuthError::Unavailable);
    }
    Ok(url)
}
/// Normalize a trusted provider/redirect endpoint.
/// # Errors
/// Rejects unsafe URL schemes or embedded credentials.
pub fn normalize_base_url(value: &str) -> Result<String, AuthError> {
    Ok(endpoint(value)?.as_str().trim_end_matches('/').into())
}
/// Normalize an Auth0 issuer's trailing slash.
/// # Errors
/// Rejects unsafe issuer URLs.
pub fn normalize_issuer(value: &str) -> Result<String, AuthError> {
    Ok(format!("{}/", normalize_base_url(value)?))
}
#[must_use]
pub fn normalize_email(value: &str) -> Option<String> {
    let email = value.trim().to_lowercase();
    let (local, domain) = email.split_once('@')?;
    (!local.is_empty()
        && !domain.is_empty()
        && domain.contains('.')
        && !domain.contains('@')
        && email.len() <= 254
        && !email.chars().any(char::is_whitespace))
    .then_some(email)
}

#[derive(Deserialize)]
struct ProviderErrorBody {
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Check Auth0 passwordless response semantics.
/// # Errors
/// Maps provider failures to stable method errors.
pub async fn provider_ok(
    response: reqwest::Response,
) -> std::result::Result<reqwest::Response, ProviderEmailError> {
    if response.status().is_success() {
        return Ok(response);
    }
    if response.status().as_u16() == 429 {
        return Err(ProviderEmailError::RateLimited(None));
    }
    if response.status().is_server_error() {
        return Err(ProviderEmailError::Unavailable);
    }
    let e = response
        .json::<ProviderErrorBody>()
        .await
        .map_err(|_| ProviderEmailError::Unavailable)?;
    let description = e.error_description.to_ascii_lowercase();
    let configuration = if description.contains("public signup is disabled") {
        Some(
            "Auth0 passwordless signup is disabled. This account needs an existing passwordless email identity.",
        )
    } else if e.error == "bad.connection"
        || [
            "connection is disabled",
            "connection not found",
            "connection is not enabled",
            "no connections enabled",
        ]
        .iter()
        .any(|message| description.contains(message))
    {
        Some(
            "Auth0's passwordless email connection is missing or disabled for this application. Enable the email connection for this client.",
        )
    } else if matches!(e.error.as_str(), "invalid_client" | "unauthorized_client") {
        Some("Auth0 rejected the application's passwordless credentials or grant configuration.")
    } else if e.error == "unsupported_grant_type" {
        Some("Auth0's passwordless OTP grant is not enabled for this application.")
    } else {
        None
    };
    if let Some(message) = configuration {
        return Err(ProviderEmailError::Configuration(message));
    }
    Err(match e.error.as_str() {
        "access_denied" | "unauthorized" => ProviderEmailError::Denied,
        "invalid_grant" | "invalid_request" => ProviderEmailError::InvalidCode,
        _ => ProviderEmailError::Unavailable,
    })
}
