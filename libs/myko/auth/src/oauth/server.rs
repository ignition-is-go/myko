//! OAuth authorization-code/PKCE handoff to a host-rendered login experience.
use super::{OAuthResource, endpoint, pkce_challenge};
use crate::{AuthError, AuthSession};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime},
};
use url::Url;
const LIMIT: usize = 4096;
const PENDING_LIFETIME: Duration = Duration::from_mins(10);
const CODE_LIFETIME: Duration = Duration::from_mins(1);
const ACCESS_LIFETIME: Duration = Duration::from_mins(5);

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "codegen-ts", derive(ts_rs::TS))]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum OAuthFlowOperation {
    Inspect {
        request: String,
    },
    Complete {
        request: String,
        credential: Option<String>,
        approve: bool,
    },
}
impl std::fmt::Debug for OAuthFlowOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Inspect { .. } => "Inspect",
            Self::Complete { .. } => "Complete",
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "codegen-ts", derive(ts_rs::TS))]
pub struct AuthorizationInfo {
    pub client_id: String,
    pub client_name: String,
    pub redirect_uri: String,
    pub resource: String,
    pub scopes: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "codegen-ts", derive(ts_rs::TS))]
pub struct OAuthFlowReply {
    pub info: Option<AuthorizationInfo>,
    pub redirect: Option<String>,
}
#[derive(Clone, Debug)]
pub struct OAuthError {
    pub code: &'static str,
    pub message: &'static str,
    pub status: u16,
}
impl std::fmt::Display for OAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}
impl std::error::Error for OAuthError {}
impl OAuthError {
    const fn invalid(message: &'static str) -> Self {
        Self {
            code: "invalid_request",
            message,
            status: 400,
        }
    }
    const fn grant() -> Self {
        Self {
            code: "invalid_grant",
            message: "The authorization grant is invalid or expired",
            status: 400,
        }
    }
    const fn busy() -> Self {
        Self {
            code: "temporarily_unavailable",
            message: "Authorization capacity exhausted; retry later",
            status: 503,
        }
    }
}
#[derive(Clone)]
struct Client {
    name: String,
    redirects: Vec<String>,
    dynamic: bool,
    last_used: Instant,
}
struct Pending {
    info: AuthorizationInfo,
    challenge: String,
    state: Option<String>,
    expires: Instant,
}
struct Code {
    pending: Pending,
    authority: AuthSession,
    expires: Instant,
}
struct Access {
    session: AuthSession,
    client: String,
    family: String,
    expires: SystemTime,
}
struct Refresh {
    authority: AuthSession,
    client: String,
    scopes: Vec<String>,
    family: String,
    used: bool,
}
#[derive(Default)]
struct State {
    clients: HashMap<String, Client>,
    pending: HashMap<String, Pending>,
    codes: HashMap<String, Code>,
    access: HashMap<String, Access>,
    refresh: HashMap<String, Refresh>,
}
impl State {
    fn touch_client(&mut self, id: &str) {
        if let Some(client) = self.clients.get_mut(id) {
            client.last_used = Instant::now();
        }
    }
    fn cleanup(&mut self) {
        let now = Instant::now();
        let wall = SystemTime::now();
        self.pending.retain(|_, p| p.expires > now);
        self.codes.retain(|_, c| c.expires > now);
        self.access.retain(|_, a| a.expires > wall);
        self.refresh
            .retain(|_, r| r.authority.identity().is_some_and(|i| i.expires_at > wall));
    }
}
/// One resource's OAuth authority. Login methods and UI stay host-owned.
///
/// Pending grants and issued credentials are process-local: a restart requires a
/// new OAuth login, while underlying identity/session storage remains host-owned.
pub struct OAuthServer {
    id: String,
    issuer: String,
    issuer_path: String,
    login: Url,
    resource: OAuthResource,
    dynamic: bool,
    state: Mutex<State>,
}
fn secret(prefix: &str) -> String {
    format!(
        "{prefix}{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn redirect(value: &str) -> Result<Url, OAuthError> {
    let url = Url::parse(value).map_err(|_| OAuthError::invalid("Invalid redirect URI"))?;
    let local = url.host_str().is_some_and(|h| {
        h == "localhost"
            || h.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "https" || (url.scheme() == "http" && local))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || value.len() > 2048
    {
        return Err(OAuthError::invalid(
            "Redirects require HTTPS or loopback HTTP without fragments or credentials",
        ));
    }
    Ok(url)
}
fn required<'a>(args: &'a HashMap<String, String>, key: &str) -> Result<&'a str, OAuthError> {
    args.get(key)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| OAuthError::invalid("Required authorization parameter is missing"))
}
impl OAuthServer {
    /// Configure a canonical issuer/resource and the host's custom login page.
    /// # Errors
    /// Rejects unsafe URLs or a login page containing its own query/fragment.
    pub fn new(issuer: &str, resource: &str, login: &str) -> Result<Self, AuthError> {
        let issuer_url = endpoint(issuer)?;
        let issuer_path = issuer_url.path().trim_end_matches('/').to_owned();
        let issuer = issuer_url.as_str().trim_end_matches('/').to_owned();
        let login = endpoint(login)?;
        let resource = OAuthResource::new(resource, vec![issuer.clone()])?
            .with_scopes(vec!["myko:access".into()])?;
        Ok(Self {
            id: secret("authority_"),
            issuer,
            issuer_path,
            login,
            resource,
            dynamic: false,
            state: Mutex::new(State::default()),
        })
    }
    /// Enable bounded public-client registration. Every grant still requires
    /// explicit authenticated consent in the host's login experience.
    #[must_use]
    pub const fn with_dynamic_registration(mut self) -> Self {
        self.dynamic = true;
        self
    }
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
    #[must_use]
    pub const fn resource(&self) -> &OAuthResource {
        &self.resource
    }
    #[must_use]
    pub fn endpoint_path(&self, name: &str) -> String {
        format!("{}/{name}", self.issuer_path)
    }
    #[must_use]
    pub fn metadata_path(&self) -> String {
        format!(
            "/.well-known/oauth-authorization-server{}",
            self.issuer_path
        )
    }
    #[must_use]
    pub fn metadata(&self) -> Value {
        let mut value = json!({"issuer":self.issuer,"authorization_endpoint":format!("{}/authorize",self.issuer),
            "token_endpoint":format!("{}/token",self.issuer),"revocation_endpoint":format!("{}/revoke",self.issuer),
            "response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],
            "authorization_response_iss_parameter_supported":true,"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["none"],"scopes_supported":["myko:access"]});
        if self.dynamic
            && let Some(object) = value.as_object_mut()
        {
            object.insert(
                "registration_endpoint".into(),
                json!(format!("{}/register", self.issuer)),
            );
        }
        value
    }
    /// Register one configured public client, protected from LRU eviction.
    /// # Errors
    /// Rejects duplicate IDs and unsafe redirect metadata.
    pub fn add_client(
        &self,
        id: &str,
        name: &str,
        redirects: Vec<String>,
    ) -> Result<(), OAuthError> {
        self.insert_client(id, name, redirects, false)
    }
    fn insert_client(
        &self,
        id: &str,
        name: &str,
        redirects: Vec<String>,
        dynamic: bool,
    ) -> Result<(), OAuthError> {
        if id.is_empty()
            || id.len() > 256
            || name.trim().is_empty()
            || name.len() > 128
            || redirects.is_empty()
            || redirects.len() > 8
        {
            return Err(OAuthError::invalid("Invalid public client metadata"));
        }
        for uri in &redirects {
            redirect(uri)?;
        }
        let mut state = self.state.lock().map_err(|_| OAuthError::busy())?;
        if state.clients.contains_key(id) {
            return Err(OAuthError::invalid("Client ID is already registered"));
        }
        if state.clients.len() >= LIMIT {
            let oldest = state
                .clients
                .iter()
                .filter(|(_, client)| client.dynamic)
                .min_by_key(|(_, client)| client.last_used)
                .map(|(id, _)| id.clone())
                .ok_or_else(OAuthError::busy)?;
            // Registration eviction must not revoke independently authorized grants.
            state.clients.remove(&oldest);
        }
        state.clients.insert(
            id.into(),
            Client {
                name: name.into(),
                redirects,
                dynamic,
                last_used: Instant::now(),
            },
        );
        drop(state);
        Ok(())
    }
    /// Register an untrusted public client; this never grants access by itself.
    /// At capacity, the least recently used dynamic registration is evicted.
    /// Existing grants survive eviction; starting a new login needs registration again.
    /// # Errors
    /// Rejects unsupported authentication/grant types or unsafe callbacks.
    pub fn register(&self, value: &Value) -> Result<Value, OAuthError> {
        if !self.dynamic {
            return Err(OAuthError::invalid("Dynamic registration is disabled"));
        }
        for (field, allowed) in [
            ("token_endpoint_auth_method", vec!["none"]),
            ("application_type", vec!["native", "web"]),
        ] {
            if value
                .get(field)
                .is_some_and(|v| v.as_str().is_none_or(|v| !allowed.contains(&v)))
            {
                return Err(OAuthError::invalid("Unsupported public client metadata"));
            }
        }
        for (field, allowed) in [
            ("grant_types", vec!["authorization_code", "refresh_token"]),
            ("response_types", vec!["code"]),
        ] {
            if let Some(values) = value.get(field)
                && values.as_array().is_none_or(|values| {
                    values
                        .iter()
                        .any(|v| v.as_str().is_none_or(|v| !allowed.contains(&v)))
                })
            {
                return Err(OAuthError::invalid("Unsupported grant or response type"));
            }
        }
        let name = match value.get("client_name") {
            None => "Unnamed client",
            Some(value) => value
                .as_str()
                .ok_or_else(|| OAuthError::invalid("Invalid client name"))?,
        };
        let redirects = value
            .get("redirect_uris")
            .and_then(Value::as_array)
            .ok_or_else(|| OAuthError::invalid("Redirect URIs are required"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| OAuthError::invalid("Invalid redirect URI"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let id = secret("client_");
        self.insert_client(&id, name, redirects.clone(), true)?;
        Ok(
            json!({"client_id":id,"client_name":name,"redirect_uris":redirects,"token_endpoint_auth_method":"none",
            "grant_types":["authorization_code","refresh_token"],"response_types":["code"]}),
        )
    }
    /// Create a short-lived handoff after validating the client and callback.
    /// # Errors
    /// Rejects unregistered clients, missing S256 PKCE, or another resource.
    pub fn begin(&self, args: &HashMap<String, String>) -> Result<String, OAuthError> {
        let client = required(args, "client_id")?;
        let uri = required(args, "redirect_uri")?;
        if required(args, "response_type")? != "code"
            || required(args, "code_challenge_method")? != "S256"
        {
            return Err(OAuthError::invalid(
                "Authorization code with S256 PKCE is required",
            ));
        }
        if required(args, "resource")? != self.resource.resource() {
            return Err(OAuthError {
                code: "invalid_target",
                message: "Unknown resource",
                status: 400,
            });
        }
        let challenge = required(args, "code_challenge")?;
        if challenge.len() != 43
            || !challenge
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            return Err(OAuthError::invalid("Invalid S256 challenge"));
        }
        let scopes = args
            .get("scope")
            .map_or("myko:access", String::as_str)
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if scopes.is_empty() || scopes.iter().any(|scope| scope != "myko:access") {
            return Err(OAuthError {
                code: "invalid_scope",
                message: "Unsupported scope",
                status: 400,
            });
        }
        let client_state = args.get("state").cloned();
        if client_state.as_ref().is_some_and(|s| s.len() > 2048) {
            return Err(OAuthError::invalid("State is too long"));
        }
        let mut state = self.state.lock().map_err(|_| OAuthError::busy())?;
        state.cleanup();
        let registered = state
            .clients
            .get_mut(client)
            .ok_or_else(|| OAuthError::invalid("Unknown client"))?;
        if !registered
            .redirects
            .iter()
            .any(|registered| registered == uri)
        {
            return Err(OAuthError::invalid("Redirect URI is not registered"));
        }
        registered.last_used = Instant::now();
        let info = AuthorizationInfo {
            client_id: client.into(),
            client_name: registered.name.clone(),
            redirect_uri: uri.into(),
            resource: self.resource.resource().into(),
            scopes,
        };
        if state.pending.len() >= LIMIT {
            return Err(OAuthError::busy());
        }
        let id = secret("request_");
        state.pending.insert(
            digest(&id),
            Pending {
                info,
                challenge: challenge.into(),
                state: client_state,
                expires: Instant::now()
                    .checked_add(PENDING_LIFETIME)
                    .ok_or_else(OAuthError::busy)?,
            },
        );
        drop(state);
        let mut login = self.login.clone();
        login.query_pairs_mut().append_pair("myko_oauth", &id);
        Ok(login.into())
    }
    /// Read only the public details needed for explicit user consent.
    /// # Errors
    /// Rejects expired or consumed requests.
    pub fn inspect(&self, request: &str) -> Result<AuthorizationInfo, OAuthError> {
        let mut state = self.state.lock().map_err(|_| OAuthError::busy())?;
        state.cleanup();
        state
            .pending
            .get(&digest(request))
            .map(|p| p.info.clone())
            .ok_or_else(OAuthError::grant)
    }
    /// Complete a request with a verified authority, or deny it without granting.
    /// # Errors
    /// Rejects expired requests or approval without a live authenticated authority.
    pub fn complete(
        &self,
        request: &str,
        authority: Option<AuthSession>,
    ) -> Result<String, OAuthError> {
        if let Some(authority) = &authority {
            authority.check().map_err(|_| OAuthError::grant())?;
            if authority.identity().is_none() {
                return Err(OAuthError::grant());
            }
        }
        let mut state = self.state.lock().map_err(|_| OAuthError::busy())?;
        state.cleanup();
        if authority.is_some() && state.codes.len() >= LIMIT {
            return Err(OAuthError::busy());
        }
        let pending = state
            .pending
            .remove(&digest(request))
            .ok_or_else(OAuthError::grant)?;
        let mut uri = redirect(&pending.info.redirect_uri)?;
        uri.query_pairs_mut().append_pair("iss", &self.issuer);
        if let Some(client_state) = &pending.state {
            uri.query_pairs_mut().append_pair("state", client_state);
        }
        if let Some(authority) = authority {
            let code = secret("code_");
            uri.query_pairs_mut().append_pair("code", &code);
            state.codes.insert(
                digest(&code),
                Code {
                    pending,
                    authority,
                    expires: Instant::now()
                        .checked_add(CODE_LIFETIME)
                        .ok_or_else(OAuthError::busy)?,
                },
            );
        } else {
            uri.query_pairs_mut().append_pair("error", "access_denied");
        }
        drop(state);
        Ok(uri.into())
    }
    /// Exchange a bound code or rotating refresh token for a short-lived credential.
    /// # Errors
    /// Rejects replay, PKCE/client/redirect/resource mismatch, or lost authority.
    pub fn token(&self, args: &HashMap<String, String>) -> Result<Value, OAuthError> {
        let client = required(args, "client_id")?;
        if required(args, "resource")? != self.resource.resource() {
            return Err(OAuthError::grant());
        }
        let mut state = self.state.lock().map_err(|_| OAuthError::busy())?;
        state.cleanup();
        if state.access.len() >= LIMIT || state.refresh.len() >= LIMIT {
            return Err(OAuthError::busy());
        }
        let (authority, scopes, family) = match required(args, "grant_type")? {
            "authorization_code" => {
                let key = digest(required(args, "code")?);
                let code = state.codes.get(&key).ok_or_else(OAuthError::grant)?;
                let verifier = required(args, "code_verifier")?;
                if code.pending.info.client_id != client
                    || required(args, "redirect_uri")? != code.pending.info.redirect_uri
                    || pkce_challenge(verifier).map_err(|_| OAuthError::grant())?
                        != code.pending.challenge
                {
                    return Err(OAuthError::grant());
                }
                let code = state.codes.remove(&key).ok_or_else(OAuthError::grant)?;
                (code.authority, code.pending.info.scopes, secret("family_"))
            }
            "refresh_token" => {
                let key = digest(required(args, "refresh_token")?);
                let refresh = state.refresh.get_mut(&key).ok_or_else(OAuthError::grant)?;
                if refresh.client != client {
                    return Err(OAuthError::grant());
                }
                if refresh.used {
                    refresh.authority.revoke();
                    return Err(OAuthError::grant());
                }
                refresh.authority.check().map_err(|_| OAuthError::grant())?;
                if args.get("scope").is_some_and(|scope| {
                    scope
                        .split_whitespace()
                        .any(|s| !refresh.scopes.iter().any(|granted| granted == s))
                }) {
                    return Err(OAuthError::grant());
                }
                refresh.used = true;
                (
                    refresh.authority.clone(),
                    refresh.scopes.clone(),
                    refresh.family.clone(),
                )
            }
            _ => {
                return Err(OAuthError {
                    code: "unsupported_grant_type",
                    message: "Unsupported grant type",
                    status: 400,
                });
            }
        };
        let result = Self::issue(&mut state, client, authority, &scopes, family);
        if result.is_ok() {
            state.touch_client(client);
        }
        drop(state);
        result
    }
    fn issue(
        state: &mut State,
        client: &str,
        authority: AuthSession,
        scopes: &[String],
        family: String,
    ) -> Result<Value, OAuthError> {
        authority.check().map_err(|_| OAuthError::grant())?;
        let expires = authority
            .identity()
            .ok_or_else(OAuthError::grant)?
            .expires_at
            .min(
                SystemTime::now()
                    .checked_add(ACCESS_LIFETIME)
                    .ok_or_else(OAuthError::busy)?,
            );
        let seconds = expires
            .duration_since(SystemTime::now())
            .map_err(|_| OAuthError::grant())?
            .as_secs();
        if seconds == 0 {
            return Err(OAuthError::grant());
        }
        let access = secret("myko_access_");
        let refresh = secret("myko_refresh_");
        let session = authority
            .with_deadline(expires)
            .map_err(|_| OAuthError::grant())?;
        state.access.insert(
            digest(&access),
            Access {
                session,
                client: client.into(),
                family: family.clone(),
                expires,
            },
        );
        state.refresh.insert(
            digest(&refresh),
            Refresh {
                authority,
                client: client.into(),
                scopes: scopes.to_owned(),
                family,
                used: false,
            },
        );
        Ok(
            json!({"access_token":access,"token_type":"Bearer","expires_in":seconds,"refresh_token":refresh,"scope":scopes.join(" ")}),
        )
    }
    /// Authenticate an issued credential while retaining original session liveness.
    /// # Errors
    /// Rejects unknown, expired or revoked credentials.
    pub fn authenticate(&self, token: &str) -> Result<AuthSession, AuthError> {
        let state = self.state.lock().map_err(|_| AuthError::Unavailable)?;
        let access = state
            .access
            .get(&digest(token))
            .ok_or(AuthError::Unauthenticated)?;
        let session = access.session.clone();
        let client = access.client.clone();
        drop(state);
        session.check()?;
        self.state
            .lock()
            .map_err(|_| AuthError::Unavailable)?
            .touch_client(&client);
        Ok(session)
    }
    /// Revoke a known grant family without disclosing whether a token exists.
    /// # Errors
    /// Rejects malformed requests or unavailable authority state.
    pub fn revoke(&self, args: &HashMap<String, String>) -> Result<(), OAuthError> {
        let state = self.state.lock().map_err(|_| OAuthError::busy())?;
        let token = required(args, "token")?;
        let key = digest(token);
        let client = Some(required(args, "client_id")?);
        let family = state
            .access
            .get(&key)
            .filter(|a| client.is_some_and(|c| c == a.client))
            .map(|a| a.family.clone())
            .or_else(|| {
                state
                    .refresh
                    .get(&key)
                    .filter(|r| client.is_some_and(|c| c == r.client))
                    .map(|r| r.family.clone())
            });
        if let Some(family) = family {
            for refresh in state.refresh.values().filter(|r| r.family == family) {
                refresh.authority.revoke();
            }
            for access in state.access.values().filter(|a| a.family == family) {
                access.session.revoke();
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for OAuthServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthServer")
            .field("issuer", &self.issuer)
            .field("resource", &self.resource)
            .field("dynamic_registration", &self.dynamic)
            .finish_non_exhaustive()
    }
}
