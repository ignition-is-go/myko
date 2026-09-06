use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ed25519_dalek::{Signer as _, SigningKey};
use myko_federation::control_quorum::*;
use myko_federation::*;

#[derive(Debug)]
struct TestJournal {
    node_id: NodeId,
    storage_incarnation: StorageIncarnationId,
    events: Mutex<Vec<EventEnvelope>>,
    fail_next_append: AtomicBool,
}

impl EventJournal for TestJournal {
    fn node_id(&self) -> Result<NodeId, NodeError> {
        Ok(self.node_id)
    }

    fn storage_incarnation(&self) -> Result<StorageIncarnationId, NodeError> {
        Ok(self.storage_incarnation)
    }

    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
        self.events
            .lock()
            .map(|events| events.clone())
            .map_err(|_| NodeError::Backend("test journal lock poisoned".to_owned()))
    }

    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError> {
        if self.fail_next_append.swap(false, Ordering::AcqRel) {
            return Err(NodeError::Backend("injected append failure".to_owned()));
        }
        self.events
            .lock()
            .map_err(|_| NodeError::Backend("test journal lock poisoned".to_owned()))?
            .push(event.clone());
        Ok(())
    }
}

fn request(scope: ScopeId) -> CommandRequest {
    let principal = PrincipalId::new("test:control-recording");
    CommandRequest {
        id: CommandId::new(),
        service_id: ServiceId::new("control-recording"),
        scope_id: scope,
        principal_id: principal.clone(),
        authority: AuthorityPresentation::direct_node(principal),
        resource_claims: Vec::new(),
        application_capabilities: Vec::new(),
        arguments_digest: None,
        command_type: "control.fixture".to_owned(),
        payload: Vec::new(),
    }
}

#[test]
fn durable_backend_records_matching_unverified_statement_only_after_manifest_verification()
-> Result<(), Box<dyn Error>> {
    let journal = Arc::new(TestJournal {
        node_id: NodeId::new(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(Vec::new()),
        fail_next_append: AtomicBool::new(false),
    });
    let node = Node::from_journal(journal.clone())?;
    if node.storage_incarnation()? != Some(journal.storage_incarnation) {
        return Err("node did not expose its journal incarnation".into());
    }
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let _policy = policy;
    let scope = ScopeId::new("control:scope");
    let command = node.submit(request(scope.clone()))?;
    let manifest = SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        journal.storage_incarnation,
        command.updated_at,
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [7; 32], [8; 64]);
    let mut events = node.subscribe_from_now()?;
    if events.try_recv().is_some() {
        return Err("new subscription unexpectedly replayed history".into());
    }
    let recorded = node.record_retained_history_statement(signed.clone(), &manifest)?;
    if !matches!(
        &recorded.event,
        NodeEvent::FrameworkControl(FrameworkControlEvent::RetainedHistoryStatement(value))
            if value == &signed
    ) {
        return Err("durable backend did not retain the exact signed statement".into());
    }
    let replay = journal.replay()?;
    if replay.last() != Some(&recorded) {
        return Err("recording returned before the journal retained the control event".into());
    }
    if events.try_recv().as_ref() != Some(&recorded) {
        return Err("successful control recording was not broadcast".into());
    }
    let recorded_cut = node.local_history_cut()?;
    if node.record_retained_history_statement(signed.clone(), &manifest)? != recorded
        || node.local_history_cut()? != recorded_cut
        || events.try_recv().is_some()
    {
        return Err("same-process retry appended or broadcast a duplicate statement".into());
    }
    drop(node);
    let reopened = Node::from_journal(journal)?;
    if reopened.record_retained_history_statement(signed, &manifest)? != recorded
        || reopened.local_history_cut()? != recorded_cut
    {
        return Err("restart retry appended a duplicate statement".into());
    }
    Ok(())
}

#[test]
fn volatile_backend_cannot_record_a_retained_history_statement() -> Result<(), Box<dyn Error>> {
    let durable_journal = Arc::new(TestJournal {
        node_id: NodeId::new(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(Vec::new()),
        fail_next_append: AtomicBool::new(false),
    });
    let durable = Node::from_journal(durable_journal)?;
    let scope = ScopeId::new("control:scope");
    let manifest = SelectedHistorySnapshot::current(&durable)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let statement = RetainedHistoryStatement::new(
        durable.node_id(),
        StorageIncarnationId::new(),
        EventId::new(durable.node_id(), LogPosition::new(1)),
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [7; 32], [8; 64]);
    let volatile = Node::in_memory();
    if volatile.storage_incarnation()?.is_some() {
        return Err("volatile backend advertised a durable storage identity".into());
    }
    if !matches!(
        volatile.record_retained_history_statement(signed, &manifest),
        Err(NodeError::DurableJournalRequired)
    ) {
        return Err("volatile backend recorded a durable control assertion".into());
    }
    Ok(())
}

#[test]
fn failed_control_append_is_invisible_and_retry_reuses_the_position() -> Result<(), Box<dyn Error>>
{
    let journal = Arc::new(TestJournal {
        node_id: NodeId::new(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(Vec::new()),
        fail_next_append: AtomicBool::new(false),
    });
    let node = Node::from_journal(journal.clone())?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let _policy = policy;
    let scope = ScopeId::new("control:failure");
    let command = node.submit(request(scope.clone()))?;
    let manifest = SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        journal.storage_incarnation,
        command.updated_at,
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [7; 32], [8; 64]);
    let before = node
        .local_history_cut()?
        .ok_or("fixture command did not advance history")?;
    let expected_retry_position = LogPosition::new(
        before
            .get()
            .checked_add(1)
            .ok_or("fixture position exhausted")?,
    );
    let mut events = node.subscribe_from_now()?;
    if events.try_recv().is_some() {
        return Err("new subscription unexpectedly replayed history".into());
    }
    journal.fail_next_append.store(true, Ordering::Release);
    if node
        .record_retained_history_statement(signed.clone(), &manifest)
        .is_ok()
    {
        return Err("injected append failure was reported as success".into());
    }
    if node.local_history_cut()? != Some(before)
        || events.try_recv().is_some()
        || journal.replay()?.len() != usize::try_from(before.get())?
    {
        return Err("failed append changed history, cursor, or live delivery".into());
    }

    let recorded = node.record_retained_history_statement(signed, &manifest)?;
    if recorded.position != expected_retry_position || events.try_recv() != Some(recorded) {
        return Err("retry did not reuse and broadcast the failed append position".into());
    }
    Ok(())
}

#[test]
fn recording_rejects_another_storage_incarnation_without_changing_history()
-> Result<(), Box<dyn Error>> {
    let journal = Arc::new(TestJournal {
        node_id: NodeId::new(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(Vec::new()),
        fail_next_append: AtomicBool::new(false),
    });
    let node = Node::from_journal(journal.clone())?;
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let _policy = policy;
    let scope = ScopeId::new("control:incarnation");
    let command = node.submit(request(scope.clone()))?;
    let manifest = SelectedHistorySnapshot::current(&node)?
        .retained_manifest(&ScopeSelection::Exact(scope))?;
    let wrong_incarnation = StorageIncarnationId::new();
    if wrong_incarnation == journal.storage_incarnation {
        return Err("wrong-incarnation fixture matched the actual store".into());
    }
    let statement = RetainedHistoryStatement::new(
        node.node_id(),
        wrong_incarnation,
        command.updated_at,
        &manifest,
    )?;
    let signed = SignedRetainedHistoryStatement::from_signature(statement, [7; 32], [8; 64]);
    let before = journal.replay()?;
    let before_cut = node.local_history_cut()?;
    let mut events = node.subscribe_from_now()?;
    if events.try_recv().is_some() {
        return Err("new subscription unexpectedly replayed history".into());
    }
    if !matches!(
        node.record_retained_history_statement(signed, &manifest),
        Err(NodeError::InvalidRetainedHistoryStatement(_))
    ) {
        return Err("recording accepted a statement for another storage incarnation".into());
    }
    if journal.replay()? != before
        || node.local_history_cut()? != before_cut
        || events.try_recv().is_some()
    {
        return Err("rejected incarnation changed history, cursor, or live delivery".into());
    }
    Ok(())
}

#[test]
fn retained_verification_rejects_conflicting_replay_even_when_last_body_matches()
-> Result<(), Box<dyn Error>> {
    let node = Node::in_memory();
    let policy: Arc<dyn AccessPolicy> = Arc::new(AllowAllAccessPolicy);
    node.set_command_access_policy(policy.clone())?;
    let _policy = policy;
    node.submit(request(ScopeId::new("control:conflicting-replay")))?;
    let event = node
        .events_after(None)?
        .pop()
        .ok_or("missing fixture event")?;
    let mut conflict = event.clone();
    conflict.recorded_at += std::time::Duration::from_secs(1);
    let journal = TestJournal {
        node_id: node.node_id(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(vec![conflict, event.clone()]),
        fail_next_append: AtomicBool::new(false),
    };
    if !matches!(
        journal.verify_retained_history(std::slice::from_ref(&event)),
        Err(NodeError::EventConflict(origin)) if origin == event.origin
    ) {
        return Err(
            "retention verification hid a conflicting replay body behind its duplicate".into(),
        );
    }
    Ok(())
}

fn voting_journal() -> Arc<TestJournal> {
    Arc::new(TestJournal {
        node_id: NodeId::new(),
        storage_incarnation: StorageIncarnationId::new(),
        events: Mutex::new(Vec::new()),
        fail_next_append: AtomicBool::new(false),
    })
}

fn voter_config(key: &SigningKey) -> Result<ControlQuorumVerifier, ControlQuorumError> {
    ControlQuorumVerifier::new(
        ControlSlot {
            realm: ScopeId::new("authority:voter"),
            epoch: ControlEpochId([2; 32]),
            predecessor: ControlHead([3; 32]),
        },
        [ControllerId(key.verifying_key().to_bytes())],
    )
}

#[test]
fn volatile_backend_cannot_release_a_control_vote() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 9,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let node = Node::in_memory();
    if !matches!(
        node.vote_control(&verifier.prepare_request(ballot)?, &key),
        Err(NodeError::DurableJournalRequired)
    ) || !node.events_after(None)?.is_empty()
    {
        return Err("volatile node released or retained a control vote".into());
    }
    Ok(())
}

#[test]
fn control_vote_is_ready_without_fake_obligations_and_stays_in_its_realm()
-> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let vote = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let realm = ScopeSelection::Exact(vote.message.slot.realm);
    let event = journal.replay()?.pop().ok_or("missing vote event")?;
    let NodeEvent::FrameworkControl(control) = &event.event else {
        return Err("vote became application work".into());
    };
    if !control.causal_dependencies().is_empty() || event.event.service_id().is_some() {
        return Err("vote requires a fake obligation or application service".into());
    }
    let snapshot = SelectedHistorySnapshot::current(&node)?;
    if snapshot.retained_manifest(&realm)?.events() != [event.clone()]
        || !snapshot
            .retained_manifest(&ScopeSelection::Exact(ScopeId::new("authority:other")))?
            .events()
            .is_empty()
    {
        return Err("vote was not ready in exactly its selected realm".into());
    }
    if !ReplicationSelection::Scopes(vec![realm]).includes(&event.event)
        || ReplicationSelection::Service(ServiceId::new("authority")).includes(&event.event)
        || ReplicationSelection::Scopes(vec![ScopeSelection::Exact(ScopeId::new(
            "authority:other",
        ))])
        .includes(&event.event)
    {
        return Err("vote crossed replication selection boundaries".into());
    }
    Ok(())
}

#[test]
fn failed_prepare_does_not_release_broadcast_or_remember_a_promise() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let high = ControlBallot {
        counter: 9,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let low = ControlBallot { counter: 1, ..high };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let before_cut = node.local_history_cut()?;
    let mut events = node.subscribe_from_now()?;
    journal.fail_next_append.store(true, Ordering::Release);
    if !matches!(
        node.vote_control(&verifier.prepare_request(high)?, &key),
        Err(NodeError::Backend(_))
    ) {
        return Err("failed append released a successful promise".into());
    }
    if !journal.replay()?.is_empty()
        || node.local_history_cut()? != before_cut
        || events.try_recv().is_some()
    {
        return Err("failed promise changed history or broadcast".into());
    }
    let response = node.vote_control(&verifier.prepare_request(low)?, &key)?;
    if response.message.ballot != low || journal.replay()?.len() != 1 || events.try_recv().is_none()
    {
        return Err("failed higher promise changed the subsequent lower prepare".into());
    }
    let history = journal.replay()?;
    if node.vote_control(&verifier.prepare_request(low)?, &key)? != response
        || journal.replay()? != history
        || events.try_recv().is_some()
    {
        return Err("exact prepare retry appended or broadcast a second vote".into());
    }
    drop(node);
    let reopened = Node::from_journal(journal.clone())?;
    if reopened.vote_control(&verifier.prepare_request(low)?, &key)? != response
        || journal.replay()? != history
    {
        return Err("reopened exact prepare retry was not idempotent".into());
    }
    Ok(())
}

#[test]
fn failed_accept_preserves_only_the_promise_until_retry_commits() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 4,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let prepare_request = verifier.prepare_request(ballot)?;
    let promise = node.vote_control(&prepare_request, &key)?;
    let prepared = verifier.verify_prepare(ballot, std::slice::from_ref(&promise))?;
    let payload = ControlValue(b"complete accepted effect".to_vec());
    let proposal = node.propose_control(&prepared.proposal_request(&payload)?, &key)?;
    let request = verifier.accept_request(&proposal)?;
    let before = journal.replay()?;
    let mut events = node.subscribe_from_now()?;
    journal.fail_next_append.store(true, Ordering::Release);
    if !matches!(
        node.vote_control(&request, &key),
        Err(NodeError::Backend(_))
    ) || journal.replay()? != before
        || events.try_recv().is_some()
    {
        return Err("failed accept released a vote, appended state, or broadcast".into());
    }
    drop(node);
    let reopened = Node::from_journal(journal.clone())?;
    if reopened.vote_control(&prepare_request, &key)? != promise || journal.replay()? != before {
        return Err("failed accept appeared in a reopened prepare response".into());
    }
    let accepted = reopened.vote_control(&request, &key)?;
    prepared.verify_chosen(&payload, std::slice::from_ref(&accepted))?;
    let committed = journal.replay()?;
    if committed.len() != 3
        || reopened.vote_control(&request, &key)? != accepted
        || journal.replay()? != committed
    {
        return Err("accept retry did not commit exactly once".into());
    }
    Ok(())
}

#[test]
fn concurrent_same_ballot_accepts_share_the_journal_lock() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 4,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let promise = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let first_proposal = adversarial_proposal(&key, &promise, b"first")?;
    let second_proposal = adversarial_proposal(&key, &promise, b"second")?;
    let first = verifier.accept_request(&first_proposal)?;
    let second = verifier.accept_request(&second_proposal)?;
    let outcomes = std::thread::scope(|scope| {
        let first = scope.spawn(|| node.vote_control(&first, &key));
        let second = scope.spawn(|| node.vote_control(&second, &key));
        (first.join(), second.join())
    });
    let results = [
        outcomes.0.map_err(|_| "first voting thread panicked")?,
        outcomes.1.map_err(|_| "second voting thread panicked")?,
    ];
    if results.iter().filter(|result| result.is_ok()).count() != 1
        || results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(NodeError::ControlVote(
                        ControlQuorumError::ConflictingAcceptedValues
                    ))
                )
            })
            .count()
            != 1
        || journal.replay()?.len() != 2
    {
        return Err("concurrent conflicting accepts were not serialized".into());
    }
    Ok(())
}

fn adversarial_proposal(
    key: &SigningKey,
    promise: &SignedControlVote,
    value: &[u8],
) -> Result<SignedControlProposal, serde_json::Error> {
    let message = ControlProposal {
        slot: promise.message.slot.clone(),
        ballot: promise.message.ballot,
        value: ControlValue(value.to_vec()),
        prepare_votes: vec![promise.clone()],
    };
    let signature = key.sign(&message.signing_bytes()?).to_bytes();
    Ok(SignedControlProposal { message, signature })
}

struct AmbiguousJournal {
    inner: Arc<TestJournal>,
    fail_after_append: AtomicBool,
}

impl EventJournal for AmbiguousJournal {
    fn node_id(&self) -> Result<NodeId, NodeError> {
        self.inner.node_id()
    }
    fn storage_incarnation(&self) -> Result<StorageIncarnationId, NodeError> {
        self.inner.storage_incarnation()
    }
    fn replay(&self) -> Result<Vec<EventEnvelope>, NodeError> {
        self.inner.replay()
    }
    fn append(&self, event: &EventEnvelope) -> Result<(), NodeError> {
        self.inner.append(event)?;
        if self.fail_after_append.swap(false, Ordering::AcqRel) {
            return Err(NodeError::Backend(
                "injected error after durable append".to_owned(),
            ));
        }
        Ok(())
    }
}

#[test]
fn ambiguous_append_cannot_issue_lower_vote_from_stale_memory() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let high = ControlBallot {
        counter: 9,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let low = ControlBallot { counter: 1, ..high };
    let journal = Arc::new(AmbiguousJournal {
        inner: voting_journal(),
        fail_after_append: AtomicBool::new(true),
    });
    let node = Node::from_journal(journal.clone())?;
    let mut events = node.subscribe_from_now()?;
    if !matches!(
        node.vote_control(&verifier.prepare_request(high)?, &key),
        Err(NodeError::Backend(_))
    ) {
        return Err("ambiguous append returned a successful vote".into());
    }
    if journal.replay()?.len() != 1
        || !node.events_after(None)?.is_empty()
        || events.try_recv().is_some()
    {
        return Err("fixture did not leave durable history ahead of the live cache".into());
    }
    if !matches!(
        node.vote_control(&verifier.prepare_request(low)?, &key),
        Err(NodeError::DurableHistoryChanged)
    ) {
        return Err(
            "voter signed a lower ballot from stale memory after an ambiguous append".into(),
        );
    }
    drop(node);
    let reopened = Node::from_journal(journal.clone())?;
    if reopened
        .vote_control(&verifier.prepare_request(low)?, &key)
        .is_ok()
    {
        return Err("reopen lost the ambiguously committed promise".into());
    }
    let recovered = reopened.vote_control(&verifier.prepare_request(high)?, &key)?;
    if recovered.message.ballot != high || journal.replay()?.len() != 1 {
        return Err("reopen did not recover the original durable vote exactly once".into());
    }
    Ok(())
}

#[test]
fn proposal_requires_durability_and_failed_append_remains_retryable() -> Result<(), Box<dyn Error>>
{
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let promise = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let prepared = verifier.verify_prepare(ballot, &[promise])?;
    let request = prepared.proposal_request(&ControlValue(b"chosen proposal".to_vec()))?;
    if node.propose_control(&request, &SigningKey::from_bytes(&[2; 32]))
        != Err(NodeError::ControlVote(ControlQuorumError::UnknownProposer))
        || Node::in_memory().propose_control(&request, &key)
            != Err(NodeError::DurableJournalRequired)
    {
        return Err("proposal accepted wrong key or volatile storage".into());
    }
    let before = journal.replay()?;
    let mut events = node.subscribe_from_now()?;
    journal.fail_next_append.store(true, Ordering::Release);
    if !matches!(
        node.propose_control(&request, &key),
        Err(NodeError::Backend(_))
    ) || journal.replay()? != before
        || events.try_recv().is_some()
    {
        return Err("failed proposal append released or broadcast a proposal".into());
    }
    let proposal = node.propose_control(&request, &key)?;
    let committed = journal.replay()?;
    if committed.len() != 2
        || events.try_recv().is_none()
        || node.propose_control(&request, &key)? != proposal
        || journal.replay()? != committed
        || events.try_recv().is_some()
    {
        return Err("proposal retry was not durable and idempotent".into());
    }
    verifier.accept_request(&proposal)?;
    Ok(())
}

#[test]
fn ambiguous_proposal_append_recovers_original_binding_after_reopen() -> Result<(), Box<dyn Error>>
{
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = Arc::new(AmbiguousJournal {
        inner: voting_journal(),
        fail_after_append: AtomicBool::new(false),
    });
    let node = Node::from_journal(journal.clone())?;
    let promise = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let prepared = verifier.verify_prepare(ballot, &[promise])?;
    let value = ControlValue(b"durable despite error".to_vec());
    let request = prepared.proposal_request(&value)?;
    let conflict = prepared.proposal_request(&ControlValue(b"replacement".to_vec()))?;
    let mut events = node.subscribe_from_now()?;
    journal.fail_after_append.store(true, Ordering::Release);
    if !matches!(
        node.propose_control(&request, &key),
        Err(NodeError::Backend(_))
    ) || events.try_recv().is_some()
    {
        return Err("ambiguous proposal append released a reply".into());
    }
    if node.propose_control(&conflict, &key) != Err(NodeError::DurableHistoryChanged)
        || journal.replay()?.len() != 2
    {
        return Err("ambiguous proposal allowed stale-cache reuse".into());
    }
    drop(node);
    let reopened = Node::from_journal(journal.clone())?;
    let recovered = reopened.propose_control(&request, &key)?;
    if recovered.message.value != value
        || journal.replay()?.len() != 2
        || reopened.propose_control(&conflict, &key)
            != Err(NodeError::ControlVote(
                ControlQuorumError::ConflictingProposals,
            ))
    {
        return Err("reopen failed to recover the ambiguously committed proposal".into());
    }
    Ok(())
}

#[test]
fn concurrent_proposal_requests_cannot_split_one_ballot() -> Result<(), Box<dyn Error>> {
    let key = SigningKey::from_bytes(&[1; 32]);
    let verifier = voter_config(&key)?;
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId(key.verifying_key().to_bytes()),
    };
    let journal = voting_journal();
    let node = Node::from_journal(journal.clone())?;
    let promise = node.vote_control(&verifier.prepare_request(ballot)?, &key)?;
    let prepared = verifier.verify_prepare(ballot, &[promise])?;
    let first = prepared.proposal_request(&ControlValue(b"first".to_vec()))?;
    let second = prepared.proposal_request(&ControlValue(b"second".to_vec()))?;
    let outcomes = std::thread::scope(|scope| {
        let first = scope.spawn(|| node.propose_control(&first, &key));
        let second = scope.spawn(|| node.propose_control(&second, &key));
        (first.join(), second.join())
    });
    let results = [
        outcomes.0.map_err(|_| "first proposer thread panicked")?,
        outcomes.1.map_err(|_| "second proposer thread panicked")?,
    ];
    if results.iter().filter(|result| result.is_ok()).count() != 1
        || results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(NodeError::ControlVote(
                        ControlQuorumError::ConflictingProposals
                    ))
                )
            })
            .count()
            != 1
        || journal.replay()?.len() != 2
    {
        return Err("concurrent proposer requests split the same ballot".into());
    }
    Ok(())
}
