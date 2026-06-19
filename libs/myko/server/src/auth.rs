//! Optional OIDC / JWT authentication for **commands** (not the connection).
//!
//! Auth is enforced per-command at dispatch, not at the WebSocket handshake:
//! reads (queries / views / reports) stay open and the connection's `client_id`
//! stays the per-connection UUID. Each command carries a bearer token on its
//! wire envelope (`WrappedCommand.user_token`, e.g. an Auth0 access token);
//! [`CommandVerifier`] (a [`myko::server::CommandAuthorizer`]) verifies it at
//! the `MykoMessage::Command` branch — signature against the issuer's published
//! JWKS, plus `iss` / `aud` / `exp`. Commands marked `#[myko_command(.., public)]`
//! skip the check.
//!
//! Auth is **off unless configured**: [`AuthConfig::from_env`] returns `None`
//! when `MYKO_AUTH_ISSUER` / `MYKO_AUTH_AUDIENCE` are unset, and the dispatch
//! path skips verification entirely. The whole module is also behind the `auth`
//! cargo feature, so the `jsonwebtoken` dependency and TLS are zero-cost when
//! unused.
//!
//! Verification is sync ([`Verifier::verify_sync`]) against keys cached by
//! [`Verifier::warm`] (one async JWKS fetch at startup; a background re-warm is
//! scheduled on an unknown `kid` so signing-key rotation recovers without a
//! restart).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

/// OIDC verification config. Construct via [`AuthConfig::from_env`].
#[derive(Clone, Debug)]
pub struct AuthConfig {
    /// Token issuer, e.g. `https://pulse-platform.us.auth0.com/` (trailing
    /// slash normalized in — Auth0's `iss` claim includes it).
    pub issuer: String,
    /// Expected `aud` — the API identifier myko tokens are minted for.
    pub audience: String,
}

impl AuthConfig {
    /// Enabled only when both env vars are present, mirroring
    /// `PostgresConfig::from_env`: presence is the on-switch.
    pub fn from_env() -> Option<Self> {
        let issuer = std::env::var("MYKO_AUTH_ISSUER").ok()?;
        let audience = std::env::var("MYKO_AUTH_AUDIENCE").ok()?;
        let issuer = if issuer.ends_with('/') {
            issuer
        } else {
            format!("{issuer}/")
        };
        Some(Self { issuer, audience })
    }

    fn jwks_uri(&self) -> String {
        format!("{}.well-known/jwks.json", self.issuer)
    }
}

/// The authenticated identity extracted from a valid token. Today `authorize`
/// only needs validity + `expires_at` (for cache capping); `subject` / `scope`
/// are carried for a future principal-binding / authorization pass.
#[derive(Clone, Debug)]
pub struct Principal {
    /// `sub` — the stable subject (user id, or `<client_id>@clients` for M2M).
    pub subject: String,
    /// True for client-credentials (machine-to-machine) tokens.
    pub is_machine: bool,
    /// Space-delimited `scope`, if present.
    pub scope: Option<String>,
    /// `exp` — token expiry, seconds since the Unix epoch.
    pub expires_at: u64,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
    #[serde(default)]
    gty: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
}

/// Verifies bearer JWTs against the issuer's JWKS, caching keys by `kid` and
/// refetching (rate-limited) on an unknown key — so signing-key rotation is
/// picked up without a restart.
pub struct Verifier {
    config: AuthConfig,
    http: reqwest::Client,
    keys: RwLock<KeyCache>,
}

#[derive(Default)]
struct KeyCache {
    by_kid: HashMap<String, DecodingKey>,
    last_fetch: Option<Instant>,
}

/// Don't hammer the JWKS endpoint when a bogus `kid` arrives.
const JWKS_MIN_REFETCH: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum AuthError {
    /// Token is structurally invalid / missing a `kid`.
    Malformed,
    /// No signing key matches the token's `kid`, even after a JWKS refetch.
    UnknownKey,
    /// Signature / `iss` / `aud` / `exp` check failed.
    Invalid(String),
    /// Could not reach / parse the JWKS endpoint.
    Jwks(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::Malformed => write!(f, "malformed token"),
            AuthError::UnknownKey => write!(f, "no signing key for token kid"),
            AuthError::Invalid(e) => write!(f, "token rejected: {e}"),
            AuthError::Jwks(e) => write!(f, "jwks fetch failed: {e}"),
        }
    }
}

impl Verifier {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
            keys: RwLock::new(KeyCache::default()),
        }
    }

    /// Fetch + cache the issuer's JWKS. The only async step in the flow — call
    /// it once at server startup (and on a background refresh when an unknown
    /// `kid` appears); afterwards verification is sync against the cached keys.
    /// Rate-limited so a flood of bad `kid`s can't hammer the IdP.
    pub async fn warm(&self) -> Result<(), AuthError> {
        if let Some(last) = self.keys.read().unwrap().last_fetch
            && last.elapsed() < JWKS_MIN_REFETCH
        {
            return Ok(());
        }

        let jwks: Jwks = self
            .http
            .get(self.config.jwks_uri())
            .send()
            .await
            .map_err(|e| AuthError::Jwks(e.to_string()))?
            .error_for_status()
            .map_err(|e| AuthError::Jwks(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::Jwks(e.to_string()))?;

        let mut by_kid = HashMap::new();
        for jwk in jwks.keys {
            if let Ok(key) = DecodingKey::from_rsa_components(&jwk.n, &jwk.e) {
                by_kid.insert(jwk.kid, key);
            }
        }

        let mut cache = self.keys.write().unwrap();
        cache.by_kid = by_kid;
        cache.last_fetch = Some(Instant::now());
        Ok(())
    }

    /// Verify a raw JWT against the cached keys — fully synchronous, no I/O.
    /// `UnknownKey` means the `kid` isn't cached (rotation / cold start);
    /// [`CommandVerifier::authorize`] schedules a background `warm()` on it so
    /// the next attempt picks up the new key.
    pub fn verify_sync(&self, token: &str) -> Result<Principal, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
        let kid = header.kid.ok_or(AuthError::Malformed)?;

        let key = {
            let cache = self.keys.read().unwrap();
            cache
                .by_kid
                .get(&kid)
                .cloned()
                .ok_or(AuthError::UnknownKey)?
        };

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);

        let data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;

        let is_machine = data
            .claims
            .gty
            .as_deref()
            .is_some_and(|g| g == "client-credentials");

        Ok(Principal {
            subject: data.claims.sub,
            is_machine,
            scope: data.claims.scope,
            expires_at: data.claims.exp,
        })
    }
}

/// Cache successful per-token verifications for this long, so a burst of
/// commands on one session doesn't re-verify each time. A short window bounds
/// how long a since-revoked token keeps working.
const TOKEN_CACHE_TTL: Duration = Duration::from_secs(60);

/// Adapts a [`Verifier`] into a [`myko::server::CommandAuthorizer`]: `public`
/// commands pass unauthenticated; all others require a token that verifies.
pub struct CommandVerifier {
    verifier: Arc<Verifier>,
    public: HashSet<String>,
    cache: RwLock<HashMap<String, Instant>>,
}

impl CommandVerifier {
    /// `public` = command ids flagged `#[myko_command(.., public)]`.
    pub fn new(config: AuthConfig, public: HashSet<String>) -> Self {
        Self {
            verifier: Arc::new(Verifier::new(config)),
            public,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Warm the JWKS before serving so `authorize` is pure-sync.
    pub async fn warm(&self) -> Result<(), AuthError> {
        self.verifier.warm().await
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl myko::server::CommandAuthorizer for CommandVerifier {
    fn authorize(&self, command_id: &str, user_token: Option<&str>) -> Result<(), String> {
        if self.public.contains(command_id) {
            return Ok(());
        }
        let token = user_token.ok_or_else(|| "authentication required".to_string())?;

        let now = Instant::now();
        if let Some(exp) = self.cache.read().unwrap().get(token).copied()
            && exp > now
        {
            return Ok(());
        }

        match self.verifier.verify_sync(token) {
            Ok(principal) => {
                // Cache the verification, but never beyond the token's own `exp`
                // (so a near-expiry token isn't honored up to a full TTL past it).
                let secs_to_exp = principal.expires_at.saturating_sub(now_unix());
                let ttl = TOKEN_CACHE_TTL.min(Duration::from_secs(secs_to_exp));
                let mut cache = self.cache.write().unwrap();
                // Sweep expired entries on write so the cache can't grow without
                // bound across token rotation over a long uptime.
                cache.retain(|_, exp| *exp > now);
                cache.insert(token.to_string(), now + ttl);
                Ok(())
            }
            Err(AuthError::UnknownKey) => {
                // Signing-key rotation, or a failed startup warm: the token's
                // `kid` isn't in the cache. Schedule a rate-limited background
                // re-warm (so the next attempt recovers without a restart) and
                // deny this one. authorize() is sync, so we can't await inline.
                let verifier = self.verifier.clone();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        if let Err(e) = verifier.warm().await {
                            log::warn!("auth: background JWKS re-warm failed: {e}");
                        }
                    });
                }
                Err(AuthError::UnknownKey.to_string())
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Command ids flagged `#[myko_command(.., public)]`, collected from the
/// registration inventory. These skip per-command verification.
pub fn public_command_ids() -> HashSet<String> {
    let mut set = HashSet::new();
    for reg in inventory::iter::<myko::command::CommandRegistration> {
        if reg.public {
            set.insert(reg.command_id.to_string());
        }
    }
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_normalizes_issuer_trailing_slash() {
        // (env-independent shape check)
        let c = AuthConfig {
            issuer: "https://t.example.com/".into(),
            audience: "aud".into(),
        };
        assert_eq!(c.jwks_uri(), "https://t.example.com/.well-known/jwks.json");
    }
}
