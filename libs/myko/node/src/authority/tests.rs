use super::*;

fn fixture() -> (
    AuthorityRuntimeConfig,
    SigningKey,
    AuthorityControllerAddress,
) {
    let key = SigningKey::from_bytes(&[1; 32]);
    let peer = AuthorityControllerAddress {
        controller: ControllerId(key.verifying_key().to_bytes()),
        endpoint: EndpointAddr::new(myko_iroh::SecretKey::from_bytes(&[2; 32]).public()),
    };
    (
        AuthorityRuntimeConfig {
            realm: AuthorityRealmId::new("configured"),
            initial_epoch: ControlEpochId([3; 32]),
            genesis: ControlHead([4; 32]),
            initial_controllers: vec![peer.controller],
            controllers: vec![peer.clone()],
        },
        key,
        peer,
    )
}

#[test]
fn routes_do_not_redefine_the_original_electorate() -> Result<(), String> {
    let (mut config, key, peer) = fixture();
    let original = ControllerId(SigningKey::from_bytes(&[5; 32]).verifying_key().to_bytes());
    config.initial_controllers = vec![original];
    config.anchor_for(&peer.endpoint, &key)?;
    config.initial_controllers.push(original);
    if config.anchor_for(&peer.endpoint, &key).is_ok() {
        return Err("replacement route repaired an invalid original anchor".to_owned());
    }
    Ok(())
}

#[test]
fn rejects_ambiguous_routes_and_mismatched_local_identity() {
    let (config, key, peer) = fixture();
    let mut duplicate = config.clone();
    duplicate.controllers.push(AuthorityControllerAddress {
        controller: ControllerId(SigningKey::from_bytes(&[5; 32]).verifying_key().to_bytes()),
        endpoint: peer.endpoint.clone(),
    });
    assert!(duplicate.anchor_for(&peer.endpoint, &key).is_err());
    let other_endpoint = EndpointAddr::new(myko_iroh::SecretKey::from_bytes(&[6; 32]).public());
    duplicate.controllers = vec![
        peer.clone(),
        AuthorityControllerAddress {
            controller: peer.controller,
            endpoint: other_endpoint.clone(),
        },
    ];
    assert!(duplicate.anchor_for(&peer.endpoint, &key).is_err());
    assert!(
        config
            .anchor_for(&peer.endpoint, &SigningKey::from_bytes(&[7; 32]))
            .is_err()
    );
    assert!(config.anchor_for(&other_endpoint, &key).is_err());
}

#[test]
fn serialized_config_rejects_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
    let (config, key, peer) = fixture();
    let mut value = serde_json::to_value(&config)?;
    let decoded: AuthorityRuntimeConfig = serde_json::from_value(value.clone())?;
    decoded.anchor_for(&peer.endpoint, &key)?;
    value
        .as_object_mut()
        .ok_or("configuration was not an object")?
        .insert(
            "signing_secret".to_owned(),
            serde_json::json!("not public configuration"),
        );
    if serde_json::from_value::<AuthorityRuntimeConfig>(value).is_ok() {
        return Err("unknown configuration field accepted".into());
    }
    let mut value = serde_json::to_value(&peer)?;
    value
        .as_object_mut()
        .ok_or("route was not an object")?
        .insert("unknown".to_owned(), serde_json::json!(true));
    if serde_json::from_value::<AuthorityControllerAddress>(value).is_ok() {
        return Err("unknown route field accepted".into());
    }
    Ok(())
}
