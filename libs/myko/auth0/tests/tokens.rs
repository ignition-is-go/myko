#![allow(clippy::panic_in_result_fn)]

use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, jwk::JwkSet};
use myko_auth::{AuthError, Authenticator, Credentials, PrincipalKind};
use myko_auth0::Auth0Authenticator;
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn provider() -> Result<Auth0Authenticator> {
    let keys: JwkSet = serde_json::from_str(include_str!("fixtures/jwks.json"))?;
    Ok(Auth0Authenticator::with_jwks(
        "https://tenant.example/",
        "myko-api",
        keys,
    )?)
}

fn claims() -> Result<Value> {
    Ok(json!({
        "iss": "https://tenant.example/",
        "aud": "myko-api",
        "sub": "auth0|user-1",
        "azp": "app-1",
        "scope": "read:projects execute:tasks",
        "exp": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs().saturating_add(300)
    }))
}

fn token(claims: &Value, kid: Option<&str>) -> Result<String> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = kid.map(str::to_owned);
    let key = EncodingKey::from_rsa_pem(include_bytes!("fixtures/test-key.pem"))?;
    Ok(encode(&header, claims, &key)?)
}

async fn authenticate(
    provider: &Auth0Authenticator,
    token: String,
) -> std::result::Result<myko_auth::AuthenticatedIdentity, AuthError> {
    provider
        .authenticate(Credentials {
            bearer: Some(token),
        })
        .await
}

#[tokio::test]
async fn user_and_machine_tokens_produce_verified_identities_and_scopes() -> Result {
    let provider = provider()?;
    let mut claims = claims()?;
    let user = authenticate(&provider, token(&claims, Some("fixture-key"))?).await?;
    assert_eq!(user.principal.issuer, "https://tenant.example/");
    assert_eq!(user.principal.subject, "auth0|user-1");
    assert_eq!(user.principal.kind, PrincipalKind::User);
    assert_eq!(user.client_id.as_deref(), Some("app-1"));
    assert_eq!(user.scopes, vec!["read:projects", "execute:tasks"]);
    assert!(user.delegation.is_empty());
    assert!(user.expires_at > SystemTime::now());
    claims
        .as_object_mut()
        .ok_or("claims object")?
        .insert("sub".into(), json!("executor-1@clients"));
    claims
        .as_object_mut()
        .ok_or("claims object")?
        .insert("gty".into(), json!("client-credentials"));
    let machine = authenticate(&provider, token(&claims, Some("fixture-key"))?).await?;
    assert_eq!(machine.principal.subject, "executor-1@clients");
    assert_eq!(machine.principal.kind, PrincipalKind::Service);
    Ok(())
}

#[tokio::test]
async fn wrong_issuer_audience_and_missing_required_claims_are_rejected() -> Result {
    let provider = provider()?;
    for (field, value) in [
        ("iss", json!("https://other.example/")),
        ("aud", json!("another-api")),
        ("sub", json!("")),
    ] {
        let mut changed = claims()?;
        changed
            .as_object_mut()
            .ok_or("claims object")?
            .insert(field.into(), value);
        assert!(matches!(
            authenticate(&provider, token(&changed, Some("fixture-key"))?).await,
            Err(AuthError::Unauthenticated)
        ));
    }
    for field in ["iss", "aud", "sub", "exp"] {
        let mut changed = claims()?;
        changed
            .as_object_mut()
            .ok_or("claims object")?
            .remove(field);
        assert!(matches!(
            authenticate(&provider, token(&changed, Some("fixture-key"))?).await,
            Err(AuthError::Unauthenticated)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn expired_and_future_tokens_are_rejected() -> Result {
    let provider = provider()?;
    let mut expired = claims()?;
    expired
        .as_object_mut()
        .ok_or("claims object")?
        .insert("exp".into(), json!(1));
    assert!(matches!(
        authenticate(&provider, token(&expired, Some("fixture-key"))?).await,
        Err(AuthError::Expired)
    ));
    let mut future = claims()?;
    future.as_object_mut().ok_or("claims object")?.insert(
        "nbf".into(),
        json!(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs()
                .saturating_add(3600)
        ),
    );
    assert!(matches!(
        authenticate(&provider, token(&future, Some("fixture-key"))?).await,
        Err(AuthError::Unauthenticated)
    ));
    Ok(())
}

#[tokio::test]
async fn unknown_or_missing_keys_and_algorithm_confusion_are_rejected() -> Result {
    let provider = provider()?;
    for kid in [None, Some("unknown-key")] {
        assert!(matches!(
            authenticate(&provider, token(&claims()?, kid)?).await,
            Err(AuthError::Unauthenticated)
        ));
    }
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("fixture-key".to_owned());
    let forged = encode(
        &header,
        &claims()?,
        &EncodingKey::from_secret(b"not-the-RSA-key"),
    )?;
    assert!(matches!(
        authenticate(&provider, forged).await,
        Err(AuthError::Unauthenticated)
    ));
    let mut invalid_signature = token(&claims()?, Some("fixture-key"))?;
    let signature_at = invalid_signature
        .rfind('.')
        .ok_or("signature")?
        .saturating_add(1);
    let original = invalid_signature
        .as_bytes()
        .get(signature_at)
        .copied()
        .ok_or("signature")?;
    invalid_signature.replace_range(
        signature_at..signature_at.saturating_add(1),
        if original == b'A' { "B" } else { "A" },
    );
    assert!(matches!(
        authenticate(&provider, invalid_signature).await,
        Err(AuthError::Unauthenticated)
    ));
    Ok(())
}

#[tokio::test]
async fn tokens_for_an_api_can_have_multiple_audiences() -> Result {
    let provider = provider()?;
    let mut claims = claims()?;
    claims.as_object_mut().ok_or("claims object")?.insert(
        "aud".into(),
        json!(["https://tenant.example/userinfo", "myko-api"]),
    );
    assert_eq!(
        authenticate(&provider, token(&claims, Some("fixture-key"))?)
            .await?
            .principal
            .subject,
        "auth0|user-1"
    );
    Ok(())
}

#[test]
fn invalid_issuer_configuration_and_debug_secrets_are_rejected() -> Result {
    for issuer in [
        "http://tenant.example/",
        "https://tenant.example/path",
        "https://secret@tenant.example/",
        "https://tenant.example/?query=secret",
        "https://tenant.example/#fragment",
    ] {
        assert!(matches!(
            Auth0Authenticator::new(issuer, "myko-api"),
            Err(AuthError::Unavailable)
        ));
    }
    assert!(Auth0Authenticator::new("https://tenant.example/", "").is_err());
    let credentials = Credentials {
        bearer: Some("sensitive-token".to_owned()),
    };
    assert!(!format!("{credentials:?}").contains("sensitive-token"));
    #[cfg(feature = "client-credentials")]
    {
        let credentials = myko_auth0::Auth0ClientCredentials::new(
            "https://tenant.example/",
            "myko-api",
            "client-1".to_owned(),
            "sensitive-secret".to_owned(),
        )?;
        assert!(!format!("{credentials:?}").contains("sensitive-secret"));
    }
    Ok(())
}

#[tokio::test]
async fn id_tokens_bind_nonce_and_authorized_party_to_the_browser_client() -> Result {
    let provider = provider()?.for_audience("browser-client")?;
    let mut value = claims()?;
    let object = value.as_object_mut().ok_or("claims")?;
    object.insert("aud".into(), json!(["browser-client", "another-resource"]));
    object.insert("azp".into(), json!("browser-client"));
    object.insert("nonce".into(), json!("expected-nonce"));
    let signed = token(&value, Some("fixture-key"))?;
    assert!(
        provider
            .verify_id_token(&signed, Some("expected-nonce"))
            .await
            .is_ok()
    );
    assert!(
        provider
            .verify_id_token(&signed, Some("other-nonce"))
            .await
            .is_err()
    );
    for azp in [None, Some("another-client")] {
        let mut changed = value.clone();
        let object = changed.as_object_mut().ok_or("claims")?;
        object.remove("azp");
        if let Some(azp) = azp {
            object.insert("azp".into(), json!(azp));
        }
        assert!(
            provider
                .verify_id_token(
                    &token(&changed, Some("fixture-key"))?,
                    Some("expected-nonce")
                )
                .await
                .is_err()
        );
    }
    Ok(())
}
