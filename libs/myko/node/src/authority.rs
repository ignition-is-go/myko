use std::{collections::BTreeSet, sync::Arc};

use ed25519_dalek::SigningKey;
use myko_authority::certified::{
    AuthorityAnchor, AuthorityControllerPrincipal, AuthorityCoordinatorPeer,
    AuthorityDecisionCoordinator, CertifiedAuthorityControlEndpoint, PreparedAuthorityRuntime,
};
use myko_federation::{
    AccessPolicy, AuthorityRealmId, CommandSnapshot, Principal,
    control_quorum::{ControlEpochId, ControlHead, ControllerId},
};
use myko_iroh::{EndpointAddr, IrohScopedEvidenceEndpoint, endpoint_principal_id};
use serde::{Deserialize, Serialize};

use crate::{Node, NodeError};

#[cfg(test)]
mod tests;

/// Operator-provisioned binding between a controller key and its transport identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityControllerAddress {
    pub controller: ControllerId,
    pub endpoint: EndpointAddr,
}

/// Static trust anchor and authenticated routes, not application access grants.
/// History transfers still require the installed history policy's permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityRuntimeConfig {
    pub realm: AuthorityRealmId,
    pub initial_epoch: ControlEpochId,
    pub genesis: ControlHead,
    /// Original electorate, retained when certified history replaces controllers.
    pub initial_controllers: Vec<ControllerId>,
    /// Current authenticated routes. Membership is verified against certified history.
    pub controllers: Vec<AuthorityControllerAddress>,
}

impl AuthorityRuntimeConfig {
    fn anchor_for(
        &self,
        endpoint: &EndpointAddr,
        key: &SigningKey,
    ) -> Result<AuthorityAnchor, String> {
        let mut endpoints = BTreeSet::new();
        let mut controllers = BTreeSet::new();
        for peer in &self.controllers {
            if !controllers.insert(peer.controller) {
                return Err("authority controller route is duplicated".to_owned());
            }
            if !endpoints.insert(peer.endpoint.id) {
                return Err("authority controller transport identity is duplicated".to_owned());
            }
        }
        let local = ControllerId(key.verifying_key().to_bytes());
        if !self
            .controllers
            .iter()
            .any(|peer| peer.controller == local && peer.endpoint.id == endpoint.id)
        {
            return Err("authority signing key is not bound to this native endpoint".to_owned());
        }
        AuthorityAnchor::new(
            self.realm.clone(),
            self.initial_epoch,
            self.genesis,
            self.initial_controllers.clone(),
        )
    }
}

impl Node {
    /// Install certified authority using this node's existing transport and dispatcher.
    /// Configure history access separately and initialize application resources before
    /// releasing startup. Publication retries as the configured peers become ready.
    /// Keep the signing secret outside serialized public configuration.
    ///
    /// # Errors
    /// Rejects repeated installation, invalid trust bindings, a missing executor,
    /// or failure to install the shared endpoint and policy.
    pub fn install_certified_authority(
        &mut self,
        config: &AuthorityRuntimeConfig,
        key: SigningKey,
        non_effect_policy: Arc<dyn AccessPolicy>,
        report: impl FnMut(Result<CommandSnapshot, String>) + Send + 'static,
    ) -> Result<(), NodeError> {
        if self.certified_authority.is_some() {
            return Err(NodeError::Configuration(
                "certified authority is already installed".to_owned(),
            ));
        }
        tokio::runtime::Handle::try_current()
            .map_err(|error| NodeError::State(error.to_string()))?;
        let anchor = config
            .anchor_for(&self.address(), &key)
            .map_err(NodeError::Configuration)?;
        let principal = Principal::node(endpoint_principal_id(self.address().id));
        let local = AuthorityControllerPrincipal::new(
            principal.clone(),
            ControllerId(key.verifying_key().to_bytes()),
        );
        let callers = config
            .controllers
            .iter()
            .map(|peer| {
                AuthorityControllerPrincipal::new(
                    Principal::node(endpoint_principal_id(peer.endpoint.id)),
                    peer.controller,
                )
            })
            .collect();
        let mut endpoint = CertifiedAuthorityControlEndpoint::new(
            self.federation.clone(),
            anchor.clone(),
            key,
            callers,
        )
        .map_err(NodeError::Configuration)?;
        for peer in &config.controllers {
            if peer.endpoint.id != self.address().id {
                let evidence = Arc::new(IrohScopedEvidenceEndpoint::new(
                    self.replicator.clone(),
                    peer.endpoint.clone(),
                ));
                endpoint = endpoint
                    .with_scoped_evidence_endpoint(
                        endpoint_principal_id(peer.endpoint.id),
                        evidence,
                    )
                    .map_err(NodeError::Configuration)?;
            }
        }
        let endpoint = Arc::new(endpoint);
        let peers = config
            .controllers
            .iter()
            .map(|peer| {
                if peer.endpoint.id == self.address().id {
                    AuthorityCoordinatorPeer::new(
                        endpoint.clone(),
                        principal.clone(),
                        peer.controller,
                        config.realm.clone(),
                    )
                } else {
                    AuthorityCoordinatorPeer::new(
                        Arc::new(self.replicator.command_client(peer.endpoint.clone())),
                        principal.clone(),
                        peer.controller,
                        config.realm.clone(),
                    )
                    .with_observer_evidence_endpoint(Arc::new(
                        IrohScopedEvidenceEndpoint::new(
                            self.replicator.clone(),
                            peer.endpoint.clone(),
                        ),
                    ))
                }
            })
            .collect();
        let coordinator =
            AuthorityDecisionCoordinator::new(anchor, self.federation.clone(), local, peers)
                .map_err(NodeError::Configuration)?;
        let (runtime, policy) = PreparedAuthorityRuntime::new(coordinator, non_effect_policy);
        self.set_access_policy(policy)?;
        self.sessions()
            .set_authority_control(Some(endpoint))
            .map_err(NodeError::State)?;
        self.certified_authority = Some(runtime.start(report).map_err(NodeError::State)?);
        Ok(())
    }

    /// Returns a stopped certified-authority worker's failure, if installed.
    #[must_use]
    pub fn certified_authority_failure(&self) -> Option<String> {
        self.certified_authority
            .as_ref()
            .and_then(myko_authority::certified::PreparedAuthorityGuard::failure)
    }
}
