use std::time::Duration;

use hyphae::{Signal, Watchable as _};
use myko_federation::{LiveSubscriptionHandle as _, LiveSubscriptionState, SubscriptionLiveness};

use super::*;

type NullableState = LiveSubscriptionState<Option<String>>;

#[tokio::test]
async fn nullable_report_retains_a_published_null_over_the_socket() -> TestResult {
    let fixture = Fixture::start().await?;
    let mut report = fixture.client.follow_report(&left::RecordLabel {}).await?;
    if report.current().value != Some(Some(fixture.left.label.clone())) {
        return Err("nullable report did not publish its initial record".into());
    }
    fixture.host.exec_command(left::Remove {
        id: fixture.left.id.clone(),
    })?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = report.recv().await?;
            match state.value {
                Some(None) => return Ok::<(), Box<dyn Error>>(()),
                Some(Some(_)) => {}
                None => return Err("published null became an absent report value".into()),
            }
        }
    })
    .await??;
    fixture.host.exec_command(left::Store {
        record: fixture.left.clone(),
    })?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let state = report.recv().await?;
            if state.value == Some(Some(fixture.left.label.clone())) {
                return Ok::<(), Box<dyn Error>>(());
            }
        }
    })
    .await??;
    fixture.server.shutdown().await?;
    Ok(())
}

async fn next_state(
    updates: &flume::Receiver<Arc<NullableState>>,
    matches: impl Fn(&NullableState) -> bool + Send + Sync,
) -> TestResult<Arc<NullableState>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = updates.recv_async().await?;
            if matches(&state) {
                return Ok(state);
            }
        }
    })
    .await?
}

#[tokio::test]
async fn nullable_report_recovers_after_server_restart_on_the_original_handle() -> TestResult {
    let fixture = Fixture::start().await?;
    fixture.host.exec_command(left::Remove {
        id: fixture.left.id.clone(),
    })?;
    let report = fixture
        .client
        .follow_report_reactive(&left::RecordLabel {})?;
    let (send, updates) = flume::unbounded();
    let guard = report.live_subscription().state().subscribe(move |signal| {
        if let Signal::Value(state) = signal {
            let _closed = send.send(state.clone());
        }
    });
    let current = |state: &NullableState| state.liveness == SubscriptionLiveness::Current;
    let initial = next_state(&updates, current).await?;
    if initial.value != Some(None) {
        return Err("initial null report did not count as a publication".into());
    }

    fixture.server.shutdown().await?;
    let stale = next_state(&updates, |state| {
        matches!(state.liveness, SubscriptionLiveness::Resynchronizing { .. })
    })
    .await?;
    if stale.value != Some(None) {
        return Err("disconnect discarded the published null".into());
    }
    let server = LocalNodeServer::spawn_application(
        fixture.directory.path().join("namespaces.sock"),
        fixture.host.clone(),
        PrincipalId::new("local:namespace-test"),
        Arc::new(AllowAllAccessPolicy),
    )
    .await?;
    let recovered = next_state(&updates, current).await?;
    if recovered.value != Some(None) {
        return Err("reconnection discarded the published null".into());
    }
    fixture.host.exec_command(left::Store {
        record: fixture.left.clone(),
    })?;
    let restored = next_state(&updates, |state| {
        current(state) && state.value.as_ref().is_some_and(Option::is_some)
    })
    .await?;
    if restored.value != Some(Some(fixture.left.label)) {
        return Err("recovered report did not receive the later record".into());
    }
    drop((guard, report));
    server.shutdown().await?;
    Ok(())
}
