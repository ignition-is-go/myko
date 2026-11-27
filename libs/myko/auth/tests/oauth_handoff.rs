#![allow(clippy::panic_in_result_fn)]
use myko_auth::{
    oauth::{OAuthServer, RegistrationKey, pkce_challenge},
    *,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use url::Url;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
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
fn params(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
fn query(url: &str, key: &str) -> TestResult<String> {
    Url::parse(url)?
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| "query field missing".into())
}
const KEY: &str = "q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq80=";
fn authority(issuer: &str, key: &str) -> TestResult<OAuthServer> {
    Ok(OAuthServer::new(
        issuer,
        "http://localhost:5155/myko/mcp",
        "http://localhost:8081",
    )?
    .with_dynamic_registration(&RegistrationKey::from_base64(key)?)?)
}
fn fixture() -> TestResult<(OAuthServer, AuthSession)> {
    let server = authority("http://localhost:5155/oauth", KEY)?;
    server.add_client(
        "client",
        "Test client",
        vec!["http://127.0.0.1:9000/callback".into()],
    )?;
    let identity = AuthenticatedIdentity {
        user_id: None,
        principal: Principal {
            issuer: "https://login.example".into(),
            subject: "user-1".into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec!["read".into()],
        delegation: vec![],
        claims: json!({"user":"shared-user-1"}),
        expires_at: SystemTime::now()
            .checked_add(Duration::from_hours(1))
            .ok_or("expiry overflow")?,
    };
    Ok((server, AuthSession::new(identity, Arc::new(Policy))))
}
const VERIFIER: &str = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";
fn authorization() -> TestResult<HashMap<String, String>> {
    Ok(params(&[
        ("client_id", "client"),
        ("redirect_uri", "http://127.0.0.1:9000/callback"),
        ("response_type", "code"),
        ("resource", "http://localhost:5155/myko/mcp"),
        ("code_challenge_method", "S256"),
        ("code_challenge", &pkce_challenge(VERIFIER)?),
        ("state", "client-state"),
    ]))
}
fn grant(server: &OAuthServer, parent: &AuthSession) -> TestResult<HashMap<String, String>> {
    let request = query(&server.begin(&authorization()?)?, "myko_oauth")?;
    assert_eq!(server.inspect(&request)?.client_name, "Test client");
    let callback = server.complete(&request, Some(parent.clone()))?;
    assert_eq!(query(&callback, "state")?, "client-state");
    assert_eq!(query(&callback, "iss")?, "http://localhost:5155/oauth");
    assert!(server.inspect(&request).is_err());
    let code = query(&callback, "code")?;
    Ok(params(&[
        ("grant_type", "authorization_code"),
        ("client_id", "client"),
        ("resource", "http://localhost:5155/myko/mcp"),
        ("redirect_uri", "http://127.0.0.1:9000/callback"),
        ("code_verifier", VERIFIER),
        ("code", &code),
    ]))
}
fn field<'a>(value: &'a Value, key: &str) -> TestResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| "token field missing".into())
}
#[test]
fn code_binding_and_single_use() -> TestResult {
    let (server, parent) = fixture()?;
    let args = grant(&server, &parent)?;
    for (key, value) in [
        ("client_id", "other"),
        ("resource", "https://other.example"),
        ("redirect_uri", "http://127.0.0.1:9001/callback"),
        ("code_verifier", "wrong"),
    ] {
        let mut invalid = args.clone();
        invalid.insert(key.into(), value.into());
        assert!(server.token(&invalid).is_err(), "{key}");
    }
    let token = server.token(&args)?;
    assert!(server.token(&args).is_err());
    let access = server.authenticate(field(&token, "access_token")?)?;
    assert_eq!(
        access
            .identity()
            .ok_or("identity missing")?
            .claims
            .get("user"),
        Some(&json!("shared-user-1"))
    );
    assert_eq!(
        access.identity().ok_or("identity missing")?.scopes,
        ["read"]
    );
    parent.revoke();
    assert!(access.check().is_err());
    assert!(server.authenticate(field(&token, "access_token")?).is_err());
    Ok(())
}
#[test]
fn refresh_replay_revokes_the_live_family() -> TestResult {
    let (server, parent) = fixture()?;
    let first = server.token(&grant(&server, &parent)?)?;
    let refresh = params(&[
        ("grant_type", "refresh_token"),
        ("client_id", "client"),
        ("resource", "http://localhost:5155/myko/mcp"),
        ("refresh_token", field(&first, "refresh_token")?),
    ]);
    let next = server.token(&refresh)?;
    let active = server.authenticate(field(&next, "access_token")?)?;
    assert!(server.token(&refresh).is_err());
    assert!(active.check().is_err());
    Ok(())
}
#[test]
fn denial_unsafe_registration_and_client_bound_revocation() -> TestResult {
    let (server, parent) = fixture()?;
    let mut args = authorization()?;
    args.insert("redirect_uri".into(), "https://evil.example".into());
    assert!(server.begin(&args).is_err());
    assert!(
        server
            .register(&json!({"client_name":"bad","redirect_uris":["http://evil.example"]}))
            .is_err()
    );
    assert!(server.register(&json!({"client_name":"Native","redirect_uris":["http://127.0.0.1:9001/callback"],"token_endpoint_auth_method":"none"}))?.get("client_id").is_some_and(Value::is_string));
    let request = query(&server.begin(&authorization()?)?, "myko_oauth")?;
    let denied = server.complete(&request, None)?;
    assert_eq!(query(&denied, "error")?, "access_denied");
    assert!(server.inspect(&request).is_err());
    let token = server.token(&grant(&server, &parent)?)?;
    let active = server.authenticate(field(&token, "access_token")?)?;
    server.revoke(&params(&[
        ("client_id", "other"),
        ("token", field(&token, "refresh_token")?),
    ]))?;
    assert!(active.check().is_ok());
    server.revoke(&params(&[
        ("client_id", "client"),
        ("token", field(&token, "refresh_token")?),
    ]))?;
    assert!(active.check().is_err());
    Ok(())
}

struct Reject;
impl Authenticator for Reject {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> futures::future::BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async { Err(AuthError::Unauthenticated) })
    }
}
#[test]
fn approval_requires_verified_credentials_and_does_not_expose_other_operations() -> TestResult {
    futures::executor::block_on(async {
        use myko_auth::oauth::OAuthFlowOperation;
        let (server, _) = fixture()?;
        let server = Arc::new(server);
        let provider = MykoAuthProvider::new(Arc::new(Reject), Arc::new(Policy))
            .with_oauth_server(server.clone());
        let request = query(&server.begin(&authorization()?)?, "myko_oauth")?;
        for credential in [None, Some("invalid-session".into())] {
            assert!(
                provider
                    .oauth_flow(OAuthFlowOperation::Complete {
                        request: request.clone(),
                        credential,
                        approve: true
                    })
                    .await
                    .is_err()
            );
        }
        assert!(server.inspect(&request).is_ok());
        let anonymous = provider.admit(Credentials::default()).await?;
        assert!(
            anonymous
                .authorize(AuthorizationRequest::Discover {
                    kind: OperationKind::Command,
                    operation: "MykoOAuthRequest"
                })
                .is_ok()
        );
        assert!(
            anonymous
                .authorize(AuthorizationRequest::Discover {
                    kind: OperationKind::Query,
                    operation: "PrivateQuery"
                })
                .is_err()
        );
        Ok(())
    })
}

fn register(server: &OAuthServer) -> TestResult<String> {
    let reply = server.register(
        &json!({"client_name":"Dynamic", "redirect_uris":["http://127.0.0.1:9000/callback"]}),
    )?;
    Ok(field(&reply, "client_id")?.to_owned())
}
fn dynamic_authorization(client: &str) -> TestResult<HashMap<String, String>> {
    let mut args = authorization()?;
    args.insert("client_id".into(), client.into());
    Ok(args)
}
/// Authorize and redeem a code for `client` against `server`.
fn sign_in(server: &OAuthServer, parent: &AuthSession, client: &str) -> TestResult<Value> {
    let request = query(
        &server.begin(&dynamic_authorization(client)?)?,
        "myko_oauth",
    )?;
    assert_eq!(server.inspect(&request)?.client_name, "Dynamic");
    let callback = server.complete(&request, Some(parent.clone()))?;
    Ok(server.token(&params(&[
        ("grant_type", "authorization_code"),
        ("client_id", client),
        ("resource", "http://localhost:5155/myko/mcp"),
        ("redirect_uri", "http://127.0.0.1:9000/callback"),
        ("code_verifier", VERIFIER),
        ("code", &query(&callback, "code")?),
    ]))?)
}

#[test]
fn dynamic_registration_survives_restart_and_tokens_require_reauthorization() -> TestResult {
    let (before, parent) = fixture()?;
    let client = register(&before)?;
    let tokens = sign_in(&before, &parent, &client)?;
    before.authenticate(field(&tokens, "access_token")?)?;
    drop(before);

    let (after, _) = fixture()?;
    // Process-local credentials end with the process: a 401 at the resource...
    assert_eq!(
        after.authenticate(field(&tokens, "access_token")?).err(),
        Some(AuthError::Unauthenticated)
    );
    // ...and invalid_grant on refresh, which sends the client back to authorize.
    let refresh = after.token(&params(&[
        ("grant_type", "refresh_token"),
        ("client_id", &client),
        ("resource", "http://localhost:5155/myko/mcp"),
        ("refresh_token", field(&tokens, "refresh_token")?),
    ]));
    assert_eq!(refresh.err().map(|e| e.code), Some("invalid_grant"));
    // The earlier registration is still valid, so re-authorization succeeds.
    let renewed = sign_in(&after, &parent, &client)?;
    after.authenticate(field(&renewed, "access_token")?)?;
    Ok(())
}

#[test]
fn dynamic_registrations_are_distinct_and_redirects_exact() -> TestResult {
    let (server, _) = fixture()?;
    let first = register(&server)?;
    let second = register(&server)?;
    assert_ne!(first, second);
    let mut args = dynamic_authorization(&first)?;
    args.insert(
        "redirect_uri".into(),
        "http://127.0.0.1:9000/callback/".into(),
    );
    assert!(server.begin(&args).is_err());
    assert!(server.begin(&dynamic_authorization(&second)?).is_ok());
    Ok(())
}

#[test]
fn tampered_or_foreign_client_ids_are_unknown() -> TestResult {
    let (server, _) = fixture()?;
    let client = register(&server)?;
    let (body, tag) = client.rsplit_once('.').ok_or("signed client ID")?;
    let other_key = authority(
        "http://localhost:5155/oauth",
        "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
    )?;
    let other_issuer = authority("http://localhost:5155/other", KEY)?;
    // Same redirect, attacker-chosen name, reusing the genuine tag.
    let forged_payload =
        base64_url(br#"{"n":"Trusted","r":["http://127.0.0.1:9000/callback"],"i":"x"}"#);
    let flipped_tag = format!("{}{}", tag.get(1..).ok_or("tag")?, "A");
    for forged in [
        format!("dyn1.{forged_payload}.{tag}"),
        format!("{body}.{flipped_tag}"),
        format!("{body}."),
        body.to_owned(),
        register(&other_key)?,
        register(&other_issuer)?,
        format!("{client}x"),
        "dyn1.".repeat(1000),
    ] {
        let error = server.begin(&dynamic_authorization(&forged)?).err();
        assert_eq!(error.map(|e| e.message), Some("Unknown client"), "{forged}");
    }
    assert!(server.begin(&dynamic_authorization(&client)?).is_ok());
    Ok(())
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[test]
fn registration_requires_a_configured_key() -> TestResult {
    for key in [
        "",
        "c2hvcnQ=",
        "not base64!",
        "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHg==",
    ] {
        assert!(RegistrationKey::from_base64(key).is_err(), "{key}");
    }
    assert!(RegistrationKey::from_base64("q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq80").is_ok());
    assert!(!format!("{:?}", RegistrationKey::from_base64(KEY)?).contains(KEY));
    let without = OAuthServer::new(
        "http://localhost:5155/oauth",
        "http://localhost:5155/myko/mcp",
        "http://localhost:8081",
    )?;
    assert!(
        without
            .register(&json!({"redirect_uris":["http://127.0.0.1:9000/callback"]}))
            .is_err()
    );
    assert!(without.metadata().get("registration_endpoint").is_none());
    let (server, _) = fixture()?;
    let client = register(&server)?;
    assert!(without.begin(&dynamic_authorization(&client)?).is_err());
    assert!(server.metadata().get("registration_endpoint").is_some());
    Ok(())
}

#[test]
fn configured_clients_cannot_claim_the_dynamic_namespace() -> TestResult {
    let (server, _) = fixture()?;
    assert!(
        server
            .add_client(
                "dyn1.configured",
                "Configured",
                vec!["http://127.0.0.1:9000/callback".into()],
            )
            .is_err()
    );
    assert!(server.begin(&authorization()?).is_ok());
    Ok(())
}
