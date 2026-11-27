#![allow(clippy::panic_in_result_fn)]
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures::{executor::block_on, future::BoxFuture};
use myko_auth::{
    oauth::{
        OAuthChange, OAuthFlowOperation, OAuthRecord, OAuthRecordKind, OAuthRestore, OAuthServer,
        OAuthStore, OAuthStoreError, pkce_challenge,
    },
    *,
};
use serde_json::{Value, json};
use url::Url;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const RESOURCE: &str = "http://localhost:5155/myko/mcp";
const CALLBACK: &str = "http://127.0.0.1:9000/callback";
const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";

struct Policy;
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}

#[derive(Default)]
struct MemoryStore(Mutex<HashMap<(OAuthRecordKind, String), OAuthRecord>>);
impl OAuthStore for MemoryStore {
    fn load(&self) -> BoxFuture<'_, Result<Vec<OAuthRecord>, OAuthStoreError>> {
        Box::pin(async {
            let records = self.0.lock().map_err(|e| OAuthStoreError(e.to_string()))?;
            Ok(records.values().cloned().collect())
        })
    }
    fn record(&self, change: OAuthChange) {
        let Ok(mut records) = self.0.lock() else {
            return;
        };
        match change {
            OAuthChange::Put(record) => {
                records.insert((record.kind(), record.id().to_owned()), *record);
            }
            OAuthChange::Remove { kind, id } => {
                records.remove(&(kind, id));
            }
            OAuthChange::RevokeGrant { family } => {
                records.retain(|_, record| record.family() != Some(family.as_str()));
            }
        }
    }
}

/// The host's browser sessions as they exist outside any one process.
#[derive(Default)]
struct DurableSessions {
    sessions: Mutex<HashSet<String>>,
    offline: AtomicBool,
}
impl DurableSessions {
    fn login(&self, session: &str) -> TestResult {
        self.sessions
            .lock()
            .map_err(|e| e.to_string())?
            .insert(session.into());
        Ok(())
    }
}

/// One process's view: it validates against sessions it has loaded into memory.
struct SessionAuthenticator {
    durable: Arc<DurableSessions>,
    loaded: Mutex<HashSet<String>>,
}
impl SessionAuthenticator {
    fn load(&self, session: &str) -> Result<(), AuthError> {
        let known = self
            .durable
            .sessions
            .lock()
            .map_err(|_| AuthError::Unavailable)?
            .contains(session);
        if !known {
            return Err(AuthError::Revoked);
        }
        self.loaded
            .lock()
            .map_err(|_| AuthError::Unavailable)?
            .insert(session.into());
        Ok(())
    }
    fn logout(&self, session: &str) -> TestResult {
        self.durable
            .sessions
            .lock()
            .map_err(|e| e.to_string())?
            .remove(session);
        self.loaded
            .lock()
            .map_err(|e| e.to_string())?
            .remove(session);
        Ok(())
    }
}
fn session_of(identity: &AuthenticatedIdentity) -> Result<&str, AuthError> {
    identity
        .claims
        .get("session")
        .and_then(Value::as_str)
        .ok_or(AuthError::Unauthenticated)
}
impl Authenticator for SessionAuthenticator {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async move {
            let session = credentials.bearer.ok_or(AuthError::Unauthenticated)?;
            self.load(&session)
                .map_err(|_| AuthError::Unauthenticated)?;
            Ok(AuthenticatedIdentity {
                user_id: None,
                principal: Principal {
                    issuer: "https://login.example".into(),
                    subject: format!("user-of-{session}"),
                    kind: PrincipalKind::User,
                },
                client_id: None,
                scopes: vec!["read".into()],
                delegation: vec![],
                claims: json!({ "session": session }),
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_hours(1))
                    .ok_or(AuthError::Unavailable)?,
            })
        })
    }
    fn validate_identity(&self, identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        let loaded = self.loaded.lock().map_err(|_| AuthError::Unavailable)?;
        if loaded.contains(session_of(identity)?) {
            Ok(())
        } else {
            Err(AuthError::Revoked)
        }
    }
    fn restore_identity<'a>(
        &'a self,
        identity: &'a AuthenticatedIdentity,
    ) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(async move {
            if self.durable.offline.load(Ordering::Acquire) {
                return Err(AuthError::Unavailable);
            }
            self.load(session_of(identity)?)
        })
    }
}

/// Validates like `SessionAuthenticator` but keeps the default `restore_identity`.
struct MemoryOnlySessions(SessionAuthenticator);
impl Authenticator for MemoryOnlySessions {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        self.0.authenticate(credentials)
    }
    fn validate_identity(&self, identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        self.0.validate_identity(identity)
    }
}

struct Process {
    provider: MykoAuthProvider,
    server: Arc<OAuthServer>,
    sessions: Arc<SessionAuthenticator>,
    restore: OAuthRestore,
}
fn sessions(durable: &Arc<DurableSessions>) -> SessionAuthenticator {
    SessionAuthenticator {
        durable: durable.clone(),
        loaded: Mutex::default(),
    }
}
async fn boot_with(
    authenticator: Arc<dyn Authenticator>,
    sessions: Arc<SessionAuthenticator>,
    store: &Arc<MemoryStore>,
) -> TestResult<Process> {
    let server = OAuthServer::new(
        "http://localhost:5155/oauth",
        RESOURCE,
        "http://localhost:8081",
    )?
    .with_dynamic_registration();
    server.add_client("client", "Test client", vec![CALLBACK.into()])?;
    let server = Arc::new(server);
    let provider =
        MykoAuthProvider::new(authenticator, Arc::new(Policy)).with_oauth_server(server.clone());
    let restore = provider.restore_oauth(store.clone()).await?;
    Ok(Process {
        provider,
        server,
        sessions,
        restore,
    })
}
async fn boot(durable: &Arc<DurableSessions>, store: &Arc<MemoryStore>) -> TestResult<Process> {
    let sessions = Arc::new(sessions(durable));
    boot_with(sessions.clone(), sessions, store).await
}

fn params(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}
fn query(url: &str, key: &str) -> TestResult<String> {
    Url::parse(url)?
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| "query field missing".into())
}
fn field<'a>(value: &'a Value, key: &str) -> TestResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| "token field missing".into())
}
fn authorization(client: &str) -> TestResult<HashMap<String, String>> {
    Ok(params(&[
        ("client_id", client),
        ("redirect_uri", CALLBACK),
        ("response_type", "code"),
        ("resource", RESOURCE),
        ("code_challenge_method", "S256"),
        ("code_challenge", &pkce_challenge(VERIFIER)?),
    ]))
}
impl Process {
    /// A signed-in browser session approves `client` and the client redeems the code.
    async fn grant(&self, client: &str, session: &str) -> TestResult<Value> {
        let request = query(&self.server.begin(&authorization(client)?)?, "myko_oauth")?;
        let reply = self
            .provider
            .oauth_flow(OAuthFlowOperation::Complete {
                request,
                credential: Some(session.into()),
                approve: true,
            })
            .await?;
        let callback = reply.redirect.ok_or("redirect missing")?;
        Ok(self.server.token(&params(&[
            ("grant_type", "authorization_code"),
            ("client_id", client),
            ("resource", RESOURCE),
            ("redirect_uri", CALLBACK),
            ("code_verifier", VERIFIER),
            ("code", &query(&callback, "code")?),
        ]))?)
    }
    fn refresh(&self, tokens: &Value) -> TestResult<Value> {
        Ok(self.server.token(&params(&[
            ("grant_type", "refresh_token"),
            ("client_id", "client"),
            ("resource", RESOURCE),
            ("refresh_token", field(tokens, "refresh_token")?),
        ]))?)
    }
    async fn admits(&self, tokens: &Value) -> TestResult<bool> {
        let credentials = Credentials {
            bearer: Some(field(tokens, "access_token")?.into()),
        };
        let admitted = self.provider.authenticate(credentials).await;
        Ok(admitted.is_ok_and(|session| session.check().is_ok()))
    }
}

#[test]
fn grants_and_registrations_survive_a_restart_and_keep_rotating() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let first = boot(&durable, &store).await?;
        durable.login("browser")?;
        let registered = first
            .server
            .register(&json!({"client_name":"Dynamic","redirect_uris":[CALLBACK]}))?;
        let dynamic = field(&registered, "client_id")?;
        let tokens = first.grant("client", "browser").await?;
        drop(first);

        let second = boot(&durable, &store).await?;
        assert_eq!(
            second.restore,
            OAuthRestore {
                clients: 1,
                restored: 1,
                dropped: 0,
                deferred: 0
            }
        );
        assert!(second.admits(&tokens).await?);
        let identity_session = second
            .provider
            .authenticate(Credentials {
                bearer: Some(field(&tokens, "access_token")?.into()),
            })
            .await?;
        assert_eq!(
            identity_session
                .identity()
                .ok_or("identity missing")?
                .claims,
            json!({"session":"browser"})
        );
        assert!(second.server.begin(&authorization(dynamic)?).is_ok());
        let rotated = second.refresh(&tokens)?;
        assert_ne!(
            field(&rotated, "refresh_token")?,
            field(&tokens, "refresh_token")?
        );
        assert!(second.admits(&rotated).await?);
        drop(second);

        let third = boot(&durable, &store).await?;
        assert!(third.admits(&rotated).await?);
        assert!(third.admits(&third.refresh(&rotated)?).await?);
        Ok(())
    })
}

#[test]
fn refresh_replay_after_a_restart_revokes_the_family_for_good() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let first = boot(&durable, &store).await?;
        durable.login("browser")?;
        let tokens = first.grant("client", "browser").await?;
        let rotated = first.refresh(&tokens)?;
        drop(first);

        let second = boot(&durable, &store).await?;
        assert!(second.admits(&rotated).await?);
        assert!(second.refresh(&tokens).is_err());
        assert!(!second.admits(&rotated).await?);
        assert!(second.refresh(&rotated).is_err());
        drop(second);

        let third = boot(&durable, &store).await?;
        assert_eq!(third.restore, OAuthRestore::default());
        assert!(!third.admits(&rotated).await?);
        assert!(third.refresh(&rotated).is_err());
        Ok(())
    })
}

#[test]
fn explicit_revocation_survives_a_restart() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let first = boot(&durable, &store).await?;
        durable.login("browser")?;
        let tokens = first.grant("client", "browser").await?;
        first.server.revoke(&params(&[
            ("client_id", "client"),
            ("token", field(&tokens, "refresh_token")?),
        ]))?;
        drop(first);

        let second = boot(&durable, &store).await?;
        assert!(!second.admits(&tokens).await?);
        assert!(second.refresh(&tokens).is_err());
        Ok(())
    })
}

#[test]
fn a_grant_dies_with_the_session_that_approved_it() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let first = boot(&durable, &store).await?;
        durable.login("ended")?;
        durable.login("kept")?;
        let ended = first.grant("client", "ended").await?;
        let kept = first.grant("client", "kept").await?;
        first.sessions.logout("ended")?;
        assert!(!first.admits(&ended).await?);
        drop(first);

        let second = boot(&durable, &store).await?;
        assert_eq!(
            second.restore,
            OAuthRestore {
                clients: 0,
                restored: 1,
                dropped: 1,
                deferred: 0
            }
        );
        assert!(!second.admits(&ended).await?);
        assert!(second.refresh(&ended).is_err());
        assert!(second.admits(&kept).await?);
        second.sessions.logout("kept")?;
        assert!(!second.admits(&kept).await?);
        assert!(second.refresh(&kept).is_err());
        drop(second);

        // A dropped grant is deleted, so the same session ID reappearing cannot revive it.
        durable.login("ended")?;
        let third = boot(&durable, &store).await?;
        assert!(!third.admits(&ended).await?);
        assert!(third.refresh(&ended).is_err());
        assert!(!third.admits(&kept).await?);
        Ok(())
    })
}

#[test]
fn an_unreachable_session_store_defers_the_grant_to_the_next_start() -> TestResult {
    block_on(async {
        let (durable, store): (Arc<DurableSessions>, _) = (Arc::default(), Arc::default());
        let first = boot(&durable, &store).await?;
        durable.login("browser")?;
        let tokens = first.grant("client", "browser").await?;
        drop(first);

        durable.offline.store(true, Ordering::Release);
        let second = boot(&durable, &store).await?;
        assert_eq!(second.restore.deferred, 1);
        assert!(!second.admits(&tokens).await?);
        assert!(second.refresh(&tokens).is_err());
        drop(second);

        durable.offline.store(false, Ordering::Release);
        let third = boot(&durable, &store).await?;
        assert!(third.admits(&tokens).await?);
        Ok(())
    })
}

#[test]
fn sessions_held_only_in_memory_do_not_restore_by_default() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let boot = || async {
            let inner = Arc::new(sessions(&durable));
            let authenticator = Arc::new(MemoryOnlySessions(sessions(&durable)));
            boot_with(authenticator, inner, &store).await
        };
        let first = boot().await?;
        durable.login("browser")?;
        let tokens = first.grant("client", "browser").await?;
        assert!(first.admits(&tokens).await?);
        drop(first);

        let second = boot().await?;
        assert_eq!(second.restore.dropped, 1);
        assert!(!second.admits(&tokens).await?);
        Ok(())
    })
}

#[test]
fn a_second_store_is_refused() -> TestResult {
    block_on(async {
        let (durable, store) = (Arc::default(), Arc::default());
        let process = boot(&durable, &store).await?;
        assert!(process.provider.restore_oauth(store.clone()).await.is_err());
        Ok(())
    })
}
