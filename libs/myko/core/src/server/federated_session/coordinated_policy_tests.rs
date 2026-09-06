use myko_federation::{ItemStateRequest, PolicyDecision};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn native_frame_sink_yields_under_backpressure_and_observes_disconnect() -> TestResult {
    use crate::server::SessionSink as _;

    let (send, receive) = flume::bounded(1);
    let sink = NodeFrameSink(send);
    let frame = NodeFrame::AuthorityUnavailable {
        reason: AuthorityUnavailable::PolicyUnavailable,
    };
    sink.send_node_frame(frame.clone()).await?;
    let mut pending = sink.send_node_frame(frame);
    if tokio::time::timeout(Duration::from_millis(10), &mut pending)
        .await
        .is_ok()
    {
        return Err("full native frame queue did not suspend delivery".into());
    }
    drop(receive);
    if tokio::time::timeout(Duration::from_secs(2), pending)
        .await?
        .is_ok()
    {
        return Err("native frame sink accepted a frame after disconnect".into());
    }
    Ok(())
}

#[derive(Debug)]
struct IdentityPolicy(flume::Sender<(Option<CommandId>, AuthorizationPhase)>);

impl AccessPolicy for IdentityPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> PolicyDecision<'a> {
        if self
            .0
            .send((request.admission_id, request.authorization_phase))
            .is_err()
        {
            return Err(AuthorityUnavailable::PolicyUnavailable).into();
        }
        Ok(AuthorizationDecision::from_rule(request, Ok(()))).into()
    }
}

#[tokio::test]
async fn stream_admission_identity_is_stable_and_not_shared_between_opens() -> TestResult {
    let (send, seen) = flume::unbounded();
    let node = Node::in_memory();
    let request = NodeRequestEnvelope::connected(NodeRequest::FollowItems {
        request: myko_federation::ItemFollowRequest {
            serving_node: node.node_id(),
            source_node: node.node_id(),
            service_id: ServiceId::new("identity"),
            scope_id: ScopeId::new("identity:scope"),
            item_type: "Record".to_owned(),
            schema_version: 1,
            after: None,
        },
    });
    let session = FederatedSession::new(node, Arc::new(IdentityPolicy(send)));
    let first = session
        .open(PrincipalId::new("reader"), request.clone())
        .await;
    let admission = tokio::time::timeout(Duration::from_secs(2), seen.recv_async()).await??;
    let continuation = tokio::time::timeout(Duration::from_secs(2), seen.recv_async()).await??;
    if admission.0.is_none()
        || admission.1 != AuthorizationPhase::Admission
        || continuation != (admission.0, AuthorizationPhase::Continuation)
    {
        return Err("stream continuation did not retain its admission identity".into());
    }
    drop(first);
    let _second = session.open(PrincipalId::new("reader"), request).await;
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), seen.recv_async()).await??;
        if next.1 == AuthorizationPhase::Admission {
            if next.0.is_none() || next.0 == admission.0 {
                return Err("separate opens shared an admission identity".into());
            }
            break;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Permit,
    Deny,
    Unavailable,
}

#[derive(Debug)]
struct WaitingPolicy {
    started: flume::Sender<()>,
    resume: flume::Receiver<()>,
    outcome: Outcome,
}

impl AccessPolicy for WaitingPolicy {
    fn decide<'a>(&'a self, request: &'a AccessAttempt) -> PolicyDecision<'a> {
        PolicyDecision::coordinated(async move {
            self.started
                .send_async(())
                .await
                .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?;
            self.resume
                .recv_async()
                .await
                .map_err(|_| AuthorityUnavailable::PolicyUnavailable)?;
            match self.outcome {
                Outcome::Permit => Ok(AuthorizationDecision::from_rule(request, Ok(()))),
                Outcome::Deny => Ok(AuthorizationDecision::from_rule(
                    request,
                    Err("denied".to_owned()),
                )),
                Outcome::Unavailable => Err(AuthorityUnavailable::HistoryUnavailable),
            }
        })
    }
}

fn read_request() -> NodeRequestEnvelope {
    NodeRequestEnvelope::connected(NodeRequest::ItemState {
        request: ItemStateRequest {
            source_node: None,
            service_id: ServiceId::new("async-policy"),
            scope_id: ScopeId::new("scope:async-policy"),
            item_type: "Record".to_owned(),
            schema_version: 1,
            snapshot_through: None,
            after_item_id: None,
            page_size: 1,
        },
    })
}

#[tokio::test]
async fn coordinated_access_waits_without_policy_lock_and_preserves_outcomes() -> TestResult {
    for outcome in [Outcome::Permit, Outcome::Deny, Outcome::Unavailable] {
        let (started, waiting) = flume::bounded(1);
        let (resume, resumed) = flume::bounded(1);
        let session = FederatedSession::new(
            Node::in_memory(),
            Arc::new(WaitingPolicy {
                started,
                resume: resumed,
                outcome,
            }),
        );
        let mut frames = session
            .open(PrincipalId::new("reader"), read_request())
            .await;
        tokio::time::timeout(Duration::from_secs(2), waiting.recv_async()).await??;
        if session.access_policy.try_write().is_err() {
            return Err("policy lock held during coordination".into());
        }
        resume.send(())?;
        let frame = tokio::time::timeout(Duration::from_secs(2), frames.recv()).await?;
        match outcome {
            Outcome::Permit => {
                if !(matches!(frame, Some(NodeFrame::Authorization { decision }) if decision.is_permit()))
                {
                    return Err("coordinated permit was not preserved".into());
                }
                if !matches!(frames.recv().await, Some(NodeFrame::ItemState { .. })) {
                    return Err("permitted item page was not served".into());
                }
            }
            Outcome::Deny => {
                if !matches!(frame, Some(NodeFrame::Authorization { decision })
                    if matches!(*decision, AuthorizationDecision::Deny(_)))
                {
                    return Err("coordinated denial was not preserved".into());
                }
            }
            Outcome::Unavailable => {
                if !matches!(
                    frame,
                    Some(NodeFrame::AuthorityUnavailable {
                        reason: AuthorityUnavailable::HistoryUnavailable,
                    })
                ) {
                    return Err("coordinated unavailability was not preserved".into());
                }
            }
        }
        if frames.recv().await.is_some() {
            return Err("unexpected frame after access response".into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn replacing_policy_invalidates_an_inflight_permit() -> TestResult {
    let (started, waiting) = flume::bounded(1);
    let (resume, resumed) = flume::bounded(1);
    let session = FederatedSession::new(
        Node::in_memory(),
        Arc::new(WaitingPolicy {
            started,
            resume: resumed,
            outcome: Outcome::Permit,
        }),
    );
    let mut frames = session
        .open(PrincipalId::new("reader"), read_request())
        .await;
    tokio::time::timeout(Duration::from_secs(2), waiting.recv_async()).await??;
    session.set_access_policy(Arc::new(DenyAllAccessPolicy))?;
    resume.send(())?;
    if !matches!(
        tokio::time::timeout(Duration::from_secs(2), frames.recv()).await?,
        Some(NodeFrame::AuthorityUnavailable {
            reason: AuthorityUnavailable::StateNotCurrent
        })
    ) {
        return Err("replaced policy released a stale decision".into());
    }
    if frames.recv().await.is_some() {
        return Err("replaced policy served an item page".into());
    }
    Ok(())
}
