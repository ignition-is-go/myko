//! Durable records for the built-in OAuth authority, beside the events table.
use std::time::Duration;

use ::postgres::Client;
use futures_util::future::BoxFuture;
use myko_auth::oauth::{OAuthChange, OAuthRecord, OAuthStore, OAuthStoreError};
use tracing::warn;

use super::{PostgresConfig, connect_pg_client, format_pg_error, validate_ident};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);

enum Request {
    Change(OAuthChange),
    Load(flume::Sender<Result<Vec<OAuthRecord>, String>>),
}

/// Stores one issuer's records in `{events table}_oauth_records`.
///
/// A single thread owns the connection and serves every request in order, so
/// a load reflects each change recorded before it. Dropping the store waits
/// for queued changes to be written.
pub struct PostgresOAuthStore {
    sender: Option<flume::Sender<Request>>,
    writer: Option<std::thread::JoinHandle<()>>,
}

impl PostgresOAuthStore {
    /// # Errors
    ///
    /// Returns an error when the configured table name is not a valid identifier.
    pub fn new(config: &PostgresConfig, issuer: &str) -> Result<Self, String> {
        let table = format!("{}_oauth_records", config.table);
        validate_ident(&table)?;
        let (sender, requests) = flume::unbounded();
        let writer = Writer {
            config: config.clone(),
            table,
            issuer: issuer.to_owned(),
            client: None,
        };
        Ok(Self {
            sender: Some(sender),
            writer: Some(std::thread::spawn(move || writer.run(&requests))),
        })
    }

    fn send(&self, request: Request) -> Result<(), OAuthStoreError> {
        self.sender
            .as_ref()
            .and_then(|sender| sender.send(request).ok())
            .ok_or_else(|| OAuthStoreError("OAuth store writer stopped".into()))
    }
}

impl Drop for PostgresOAuthStore {
    fn drop(&mut self) {
        self.sender = None;
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

impl OAuthStore for PostgresOAuthStore {
    fn load(&self) -> BoxFuture<'_, Result<Vec<OAuthRecord>, OAuthStoreError>> {
        Box::pin(async move {
            let (reply, loaded) = flume::bounded(1);
            self.send(Request::Load(reply))?;
            loaded
                .recv_async()
                .await
                .map_err(|_| OAuthStoreError("OAuth store writer stopped".into()))?
                .map_err(OAuthStoreError)
        })
    }

    fn record(&self, change: OAuthChange) {
        if self.send(Request::Change(change)).is_err() {
            warn!("OAuth store writer stopped; a change was not persisted");
        }
    }
}

struct Writer {
    config: PostgresConfig,
    table: String,
    issuer: String,
    client: Option<Client>,
}

impl Writer {
    fn run(mut self, requests: &flume::Receiver<Request>) {
        while let Ok(request) = requests.recv() {
            match request {
                Request::Load(reply) => {
                    let _ = reply.send(self.attempt(Self::load));
                }
                Request::Change(change) => {
                    // NOTE(ts): a skipped change could leave a revoked grant stored, so
                    // retry until it lands rather than move on to the next one.
                    while let Err(error) =
                        self.attempt(|writer, client| writer.apply(client, &change))
                    {
                        warn!("OAuth store write failed, retrying: {error}");
                        if requests.is_disconnected() {
                            return;
                        }
                        std::thread::sleep(RETRY_INTERVAL);
                    }
                }
            }
        }
    }

    fn attempt<T>(
        &mut self,
        operation: impl FnOnce(&Self, &mut Client) -> Result<T, ::postgres::Error>,
    ) -> Result<T, String> {
        let mut client = match self.client.take() {
            Some(client) => client,
            None => self.connect()?,
        };
        let result = operation(self, &mut client)
            .map_err(|error| format_pg_error("oauth", Some(&self.config.url), &error))?;
        self.client = Some(client);
        Ok(result)
    }

    fn connect(&self) -> Result<Client, String> {
        let mut client = connect_pg_client(&self.config, "oauth")?;
        let table = &self.table;
        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    issuer TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    id TEXT NOT NULL,
                    family TEXT,
                    record JSONB NOT NULL,
                    PRIMARY KEY (issuer, kind, id)
                );
                CREATE INDEX IF NOT EXISTS {table}_family ON {table} (issuer, family)
                    WHERE family IS NOT NULL;"
            ))
            .map_err(|error| format_pg_error("oauth", Some(&self.config.url), &error))?;
        Ok(client)
    }

    fn load(&self, client: &mut Client) -> Result<Vec<OAuthRecord>, ::postgres::Error> {
        let rows = client.query(
            &format!(
                "SELECT kind, id, record::text FROM {} WHERE issuer = $1",
                self.table
            ),
            &[&self.issuer],
        )?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let text: String = row.get(2);
            match serde_json::from_str(&text) {
                Ok(record) => records.push(record),
                Err(error) => {
                    let (kind, id): (String, String) = (row.get(0), row.get(1));
                    warn!("Ignoring unreadable OAuth record {kind}/{id}: {error}");
                }
            }
        }
        Ok(records)
    }

    fn apply(&self, client: &mut Client, change: &OAuthChange) -> Result<(), ::postgres::Error> {
        let (table, issuer) = (&self.table, &self.issuer);
        match change {
            OAuthChange::Put(record) => {
                let Ok(text) = serde_json::to_string(record) else {
                    warn!("Skipping an OAuth record that does not serialize");
                    return Ok(());
                };
                client.execute(
                    &format!(
                        "INSERT INTO {table} (issuer, kind, id, family, record)
                         VALUES ($1, $2, $3, $4, $5::text::jsonb)
                         ON CONFLICT (issuer, kind, id)
                         DO UPDATE SET family = EXCLUDED.family, record = EXCLUDED.record"
                    ),
                    &[
                        issuer,
                        &record.kind().as_str(),
                        &record.id(),
                        &record.family(),
                        &text,
                    ],
                )?;
            }
            OAuthChange::Remove { kind, id } => {
                client.execute(
                    &format!("DELETE FROM {table} WHERE issuer = $1 AND kind = $2 AND id = $3"),
                    &[issuer, &kind.as_str(), id],
                )?;
            }
            OAuthChange::RevokeGrant { family } => {
                client.execute(
                    &format!("DELETE FROM {table} WHERE issuer = $1 AND family = $2"),
                    &[issuer, family],
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, SystemTime},
    };

    use myko_auth::{
        AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Authorizer,
        Credentials, MykoAuthProvider, Principal, PrincipalKind,
        oauth::{OAuthFlowOperation, OAuthRestore, OAuthServer, pkce_challenge},
    };
    use serde_json::{Value, json};

    use super::*;

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
    struct Stateless;
    impl Authenticator for Stateless {
        fn authenticate(
            &self,
            credentials: Credentials,
        ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
            Box::pin(async move {
                Ok(AuthenticatedIdentity {
                    user_id: None,
                    principal: Principal {
                        issuer: "https://login.example".into(),
                        subject: credentials.bearer.ok_or(AuthError::Unauthenticated)?,
                        kind: PrincipalKind::User,
                    },
                    client_id: None,
                    scopes: vec!["read".into()],
                    delegation: vec![],
                    claims: json!({"team": "blue"}),
                    expires_at: SystemTime::now()
                        .checked_add(Duration::from_hours(1))
                        .ok_or(AuthError::Unavailable)?,
                })
            })
        }
    }

    struct Process {
        provider: MykoAuthProvider,
        server: Arc<OAuthServer>,
        restore: OAuthRestore,
    }
    async fn boot(config: &PostgresConfig, issuer: &str) -> Process {
        let server = OAuthServer::new(issuer, RESOURCE, "http://localhost:8081")
            .expect("authority")
            .with_dynamic_registration();
        server
            .add_client("client", "Test client", vec![CALLBACK.into()])
            .expect("client");
        let server = Arc::new(server);
        let provider = MykoAuthProvider::new(Arc::new(Stateless), Arc::new(Policy))
            .with_oauth_server(server.clone());
        let store = PostgresOAuthStore::new(config, server.issuer()).expect("store");
        let restore = provider
            .restore_oauth(Arc::new(store))
            .await
            .expect("restore");
        Process {
            provider,
            server,
            restore,
        }
    }
    fn params(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }
    fn field<'a>(value: &'a Value, key: &str) -> &'a str {
        value.get(key).and_then(Value::as_str).expect("token field")
    }
    impl Process {
        async fn grant(&self) -> Value {
            let login = self
                .server
                .begin(&params(&[
                    ("client_id", "client"),
                    ("redirect_uri", CALLBACK),
                    ("response_type", "code"),
                    ("resource", RESOURCE),
                    ("code_challenge_method", "S256"),
                    ("code_challenge", &pkce_challenge(VERIFIER).expect("pkce")),
                ]))
                .expect("authorize");
            let (_, request) = login.split_once("myko_oauth=").expect("handoff");
            let callback = self
                .provider
                .oauth_flow(OAuthFlowOperation::Complete {
                    request: request.into(),
                    credential: Some("user-1".into()),
                    approve: true,
                })
                .await
                .expect("approval")
                .redirect
                .expect("redirect");
            let (_, code) = callback.split_once("code=").expect("code");
            self.server
                .token(&params(&[
                    ("grant_type", "authorization_code"),
                    ("client_id", "client"),
                    ("resource", RESOURCE),
                    ("redirect_uri", CALLBACK),
                    ("code_verifier", VERIFIER),
                    ("code", code),
                ]))
                .expect("tokens")
        }
        fn refresh(&self, tokens: &Value) -> Result<Value, String> {
            self.server
                .token(&params(&[
                    ("grant_type", "refresh_token"),
                    ("client_id", "client"),
                    ("resource", RESOURCE),
                    ("refresh_token", field(tokens, "refresh_token")),
                ]))
                .map_err(|error| error.to_string())
        }
        async fn subject(&self, tokens: &Value) -> Option<String> {
            let credentials = Credentials {
                bearer: Some(field(tokens, "access_token").into()),
            };
            let session = self.provider.authenticate(credentials).await.ok()?;
            session.check().ok()?;
            Some(session.identity()?.principal.subject.clone())
        }
    }

    #[tokio::test]
    #[ignore = "requires MYKO_HISTORY_TEST_URL pointing to a dedicated temporary database"]
    async fn grants_survive_a_restart_through_postgres() {
        let config = PostgresConfig {
            url: std::env::var("MYKO_HISTORY_TEST_URL").expect("dedicated test URL"),
            table: format!("oauth_{}", uuid::Uuid::new_v4().simple()),
            channel: "unused".into(),
        };
        let issuer = "http://localhost:5155/oauth";

        let first = boot(&config, issuer).await;
        assert_eq!(first.restore, OAuthRestore::default());
        first
            .server
            .register(&json!({"client_name": "Dynamic", "redirect_uris": [CALLBACK]}))
            .expect("registration");
        let tokens = first.grant().await;
        let rotated = first.refresh(&tokens).expect("rotation");
        let revoked = first.grant().await;
        first
            .server
            .revoke(&params(&[
                ("client_id", "client"),
                ("token", field(&revoked, "access_token")),
            ]))
            .expect("revocation");
        drop(first);

        let second = boot(&config, issuer).await;
        assert_eq!(
            second.restore,
            OAuthRestore {
                clients: 1,
                restored: 1,
                dropped: 0,
                deferred: 0
            }
        );
        assert_eq!(second.subject(&rotated).await.as_deref(), Some("user-1"));
        assert_eq!(second.subject(&revoked).await, None);
        let other = boot(&config, "http://localhost:5155/other").await;
        assert_eq!(other.restore, OAuthRestore::default());
        assert_eq!(other.subject(&rotated).await, None);

        assert!(second.refresh(&tokens).is_err());
        assert_eq!(second.subject(&rotated).await, None);
        drop(second);

        let third = boot(&config, issuer).await;
        assert_eq!(third.restore.restored, 0);
        assert_eq!(third.subject(&rotated).await, None);
        assert!(third.refresh(&rotated).is_err());
    }
}
