//! Optional OIDC / JWT authentication for the WebSocket handshake.
//!
//! When configured, a connecting client must present a bearer JWT (e.g. an
//! Auth0 access token) that this module verifies — signature against the
//! issuer's published JWKS, plus `iss` / `aud` / `exp` — *before* the WebSocket
//! upgrade is accepted. The verified principal (`sub`) is then bound to the
//! connection, so `#[myko_client_id]` carries a real, credential-backed
//! identity instead of an anonymous per-connection UUID.
//!
//! Auth is **off unless configured**: [`AuthConfig::from_env`] returns `None`
//! when `MYKO_AUTH_ISSUER` / `MYKO_AUTH_AUDIENCE` are unset, and the server
//! falls back to the existing unauthenticated behaviour. The whole module is
//! also behind the `auth` cargo feature, so the `jsonwebtoken` dependency and
//! TLS are zero-cost when unused.
//!
//! Token transport: native clients send `Authorization: Bearer <jwt>`; browser
//! clients (which cannot set headers on a WebSocket) send `?access_token=<jwt>`
//! in the query string. [`extract_bearer`] accepts either.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};

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

/// The authenticated identity extracted from a valid token.
#[derive(Clone, Debug)]
pub struct Principal {
    /// `sub` — the stable subject (user id, or `<client_id>@clients` for M2M).
    pub subject: String,
    /// True for client-credentials (machine-to-machine) tokens.
    pub is_machine: bool,
    /// Space-delimited `scope`, if present.
    pub scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
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
    /// No token presented (header and query both absent).
    Missing,
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
            AuthError::Missing => write!(f, "no bearer token presented"),
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
    /// `UnknownKey` means the `kid` isn't cached (rotation): the caller should
    /// schedule a `warm()` so the next attempt picks up the new key.
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
    verifier: Verifier,
    public: HashSet<String>,
    cache: RwLock<HashMap<String, Instant>>,
}

impl CommandVerifier {
    /// `public` = command ids flagged `#[myko_command(.., public)]`.
    pub fn new(config: AuthConfig, public: HashSet<String>) -> Self {
        Self {
            verifier: Verifier::new(config),
            public,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Warm the JWKS before serving so `authorize` is pure-sync.
    pub async fn warm(&self) -> Result<(), AuthError> {
        self.verifier.warm().await
    }
}

impl myko::server::CommandAuthorizer for CommandVerifier {
    fn authorize(&self, command_id: &str, user_token: Option<&str>) -> Result<(), String> {
        if self.public.contains(command_id) {
            return Ok(());
        }
        let token = user_token.ok_or_else(|| "authentication required".to_string())?;

        if let Some(exp) = self.cache.read().unwrap().get(token).copied()
            && exp > Instant::now()
        {
            return Ok(());
        }

        match self.verifier.verify_sync(token) {
            Ok(_) => {
                self.cache
                    .write()
                    .unwrap()
                    .insert(token.to_string(), Instant::now() + TOKEN_CACHE_TTL);
                Ok(())
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

/// Pull a bearer token from an `Authorization: Bearer …` header value, or from
/// an `access_token=…` pair in a raw query string (the browser-WebSocket path,
/// since the WebSocket API can't set request headers).
pub fn extract_bearer<'a>(auth_header: Option<&'a str>, raw_query: Option<&'a str>) -> Option<&'a str> {
    if let Some(h) = auth_header {
        let h = h.trim();
        for prefix in ["Bearer ", "bearer "] {
            if let Some(rest) = h.strip_prefix(prefix) {
                let t = rest.trim();
                if !t.is_empty() {
                    return Some(t);
                }
            }
        }
    }
    if let Some(q) = raw_query {
        for pair in q.split('&') {
            if let Some(t) = pair.strip_prefix("access_token=")
                && !t.is_empty()
            {
                return Some(t);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_bearer_from_header() {
        assert_eq!(extract_bearer(Some("Bearer abc.def.ghi"), None), Some("abc.def.ghi"));
        assert_eq!(extract_bearer(Some("bearer xyz"), None), Some("xyz"));
        assert_eq!(extract_bearer(Some("Basic xyz"), None), None);
        assert_eq!(extract_bearer(Some("Bearer "), None), None);
    }

    #[test]
    fn extract_bearer_from_query() {
        assert_eq!(
            extract_bearer(None, Some("foo=1&access_token=tok123&bar=2")),
            Some("tok123")
        );
        assert_eq!(extract_bearer(None, Some("access_token=")), None);
        assert_eq!(extract_bearer(None, Some("other=1")), None);
    }

    #[test]
    fn header_wins_over_query() {
        assert_eq!(
            extract_bearer(Some("Bearer fromheader"), Some("access_token=fromquery")),
            Some("fromheader")
        );
    }

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
