#![allow(clippy::panic_in_result_fn)]
use myko_auth::{
    oauth::{OAuthServer, pkce_challenge},
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
fn fixture() -> TestResult<(OAuthServer, AuthSession)> {
    let server = OAuthServer::new(
        "http://localhost:5155/oauth",
        "http://localhost:5155/myko/mcp",
        "http://localhost:8081",
    )?
    .with_dynamic_registration();
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
