use myko::server::{
    AuthorityControlEndpoint, AuthorityControlFuture, AuthorityControlProposeRequest,
};
use myko_federation::{
    AccessAttempt, AccessOperation, AccessTarget, AuthorityPresentation, AuthorityUnavailable,
    AuthorizationDecision, AuthorizationFailure, AuthorizationPhase, PrincipalId,
    control_quorum::{ControlBallot, ControlHead, SignedControlProposal, SignedControlVote},
};

use crate::{IrohCommandClient, IrohReplicationError, endpoint_principal_id};

impl IrohCommandClient {
    fn control_sender(
        &self,
        principal: &PrincipalId,
        presentation: &AuthorityPresentation,
    ) -> Result<Self, AuthorizationFailure> {
        let actual = endpoint_principal_id(self.replicator.address().id);
        let request = AccessAttempt {
            admission_id: None,
            principal_id: actual.clone(),
            presentation: presentation.clone(),
            operation: AccessOperation::AdministerAuthority,
            target: AccessTarget::NodeIdentity,
            resource_claims: Vec::new(),
            application_capabilities: Vec::new(),
            arguments_digest: None,
            effect_digest: None,
            lease: None,
            authorization_phase: AuthorizationPhase::Admission,
            topology: None,
        };
        let identity = if principal == &actual
            && presentation == &AuthorityPresentation::direct_node(actual)
        {
            Ok(())
        } else {
            Err("controller caller does not match the authenticated local Iroh endpoint".to_owned())
        };
        AuthorizationDecision::from_rule(&request, identity).into_permit()?;
        Ok(self.clone().with_authority(presentation.clone()))
    }
}

impl AuthorityControlEndpoint for IrohCommandClient {
    fn prepare<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        ballot: ControlBallot,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            self.control_sender(principal, presentation)?
                .prepare_control(head, ballot)
                .await
                .map_err(control_failure)
        })
    }

    fn propose<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        request: AuthorityControlProposeRequest,
    ) -> AuthorityControlFuture<'a, SignedControlProposal> {
        Box::pin(async move {
            self.control_sender(principal, presentation)?
                .propose_control(
                    request.head,
                    request.ballot,
                    request.promises,
                    request.value,
                )
                .await
                .map_err(control_failure)
        })
    }

    fn accept<'a>(
        &'a self,
        principal: &'a PrincipalId,
        presentation: &'a AuthorityPresentation,
        head: ControlHead,
        proposal: SignedControlProposal,
    ) -> AuthorityControlFuture<'a, SignedControlVote> {
        Box::pin(async move {
            self.control_sender(principal, presentation)?
                .accept_control(head, proposal)
                .await
                .map_err(control_failure)
        })
    }
}

fn control_failure(error: IrohReplicationError) -> AuthorizationFailure {
    match error {
        IrohReplicationError::Authorization { decision, .. } => match (*decision).into_permit() {
            Err(failure) => failure,
            Ok(_) => AuthorityUnavailable::CoordinationUnavailable.into(),
        },
        IrohReplicationError::AuthorityUnavailable(reason) => reason.into(),
        IrohReplicationError::Endpoint(_)
        | IrohReplicationError::Stream(_)
        | IrohReplicationError::Encoding(_)
        | IrohReplicationError::Ingest(_)
        | IrohReplicationError::Cursor(_)
        | IrohReplicationError::Supervisor(_)
        | IrohReplicationError::Identity(_) => AuthorityUnavailable::CoordinationUnavailable.into(),
    }
}
