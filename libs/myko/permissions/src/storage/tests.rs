#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::model::{
    PermissionAuditActor, PermissionAuthorityId, PermissionChangeMode, PermissionDocument,
};
use std::{sync::Barrier, thread};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn snapshot(revision: u64, change_id: &str) -> PermissionAuthority {
    PermissionAuthority {
        id: PermissionAuthorityId::from(AUTHORITY_ID),
        revision,
        document: PermissionDocument::default(),
        change_id: change_id.into(),
        actor: PermissionAuditActor::Operator {
            identity: "fixture-operator".into(),
        },
        reason: "storage behavior fixture".into(),
        mode: if revision == 1 {
            PermissionChangeMode::Bootstrap
        } else {
            PermissionChangeMode::Administration
        },
    }
}

#[test]
fn durable_snapshots_reopen_with_audit_history_and_identical_retries_do_not_append() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("permissions.jsonl");
    let storage = FilePermissionStorage::new(&path);
    assert!(storage.load()?.is_none());
    let first = snapshot(1, "first");
    storage.compare_and_swap(None, &first)?;
    storage.compare_and_swap(None, &first)?;
    let second = snapshot(2, "second");
    storage.compare_and_swap(Some(1), &second)?;
    let reopened = FilePermissionStorage::new(path);
    assert_eq!(reopened.load()?, Some(second));
    assert_eq!(
        reopened
            .history()?
            .iter()
            .map(|state| state.change_id.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    Ok(())
}

#[test]
fn independent_writers_cannot_both_commit_the_same_revision() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("permissions.jsonl");
    let storage = FilePermissionStorage::new(&path);
    storage.compare_and_swap(None, &snapshot(1, "initial"))?;
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();
    for change in ["left", "right"] {
        let storage = FilePermissionStorage::new(&path);
        let barrier = barrier.clone();
        writers.push(thread::spawn(move || {
            barrier.wait();
            storage.compare_and_swap(Some(1), &snapshot(2, change))
        }));
    }
    barrier.wait();
    let results = writers
        .into_iter()
        .map(|writer| writer.join().map_err(|_| "writer panicked"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(PermissionError::Conflict {
                    expected: Some(1),
                    actual: Some(2)
                })
            ))
            .count(),
        1
    );
    assert_eq!(storage.history()?.len(), 2);
    assert_eq!(storage.load()?.map(|state| state.revision), Some(2));
    Ok(())
}

#[test]
fn stale_revisions_and_invalid_next_snapshots_leave_accepted_state_unchanged() -> TestResult {
    let directory = tempfile::tempdir()?;
    let storage = FilePermissionStorage::new(directory.path().join("policy"));
    let first = snapshot(1, "first");
    storage.compare_and_swap(None, &first)?;
    assert_eq!(
        storage.compare_and_swap(None, &snapshot(2, "stale")),
        Err(PermissionError::Conflict {
            expected: None,
            actual: Some(1)
        })
    );
    assert!(matches!(
        storage.compare_and_swap(Some(1), &snapshot(3, "skipped")),
        Err(PermissionError::Invalid(_))
    ));
    let mut invalid = snapshot(2, "wrong-id");
    invalid.id = PermissionAuthorityId::from("other-authority");
    assert!(matches!(
        storage.compare_and_swap(Some(1), &invalid),
        Err(PermissionError::Invalid(_))
    ));
    assert_eq!(storage.load()?, Some(first));
    assert_eq!(storage.history()?.len(), 1);
    Ok(())
}

#[test]
fn interrupted_tail_is_not_published_and_next_append_replaces_only_that_tail() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("policy");
    let storage = FilePermissionStorage::new(&path);
    let first = snapshot(1, "first");
    storage.compare_and_swap(None, &first)?;
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"{\"revision\":2,")?;
    assert_eq!(storage.load()?, Some(first));
    let second = snapshot(2, "complete");
    storage.compare_and_swap(Some(1), &second)?;
    assert_eq!(storage.load()?, Some(second));
    assert_eq!(storage.history()?.len(), 2);
    Ok(())
}

#[test]
fn corrupt_committed_records_and_revision_gaps_fail_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("corrupt");
    let storage = FilePermissionStorage::new(&path);
    storage.compare_and_swap(None, &snapshot(1, "first"))?;
    OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"not-json\n")?;
    assert!(matches!(storage.load(), Err(PermissionError::Storage(_))));
    assert!(matches!(
        storage.compare_and_swap(Some(1), &snapshot(2, "next")),
        Err(PermissionError::Storage(_))
    ));
    let gap_path = directory.path().join("gap");
    let gap = FilePermissionStorage::new(&gap_path);
    gap.compare_and_swap(None, &snapshot(1, "first"))?;
    let mut encoded = serde_json::to_vec(&snapshot(3, "gap"))?;
    encoded.push(b'\n');
    OpenOptions::new()
        .append(true)
        .open(&gap_path)?
        .write_all(&encoded)?;
    assert!(matches!(gap.load(), Err(PermissionError::Storage(_))));
    Ok(())
}

#[cfg(unix)]
#[test]
fn journal_and_lock_reject_symlinks_and_shared_file_permissions() -> TestResult {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let directory = tempfile::tempdir()?;
    let target = directory.path().join("target");
    FilePermissionStorage::new(&target).compare_and_swap(None, &snapshot(1, "first"))?;
    let link = directory.path().join("link");
    symlink(&target, &link)?;
    assert!(matches!(
        FilePermissionStorage::new(link).load(),
        Err(PermissionError::Storage(_))
    ));
    let lock_target = directory.path().join("lock-target");
    File::create(&lock_target)?;
    let lock_link = directory.path().join("symlinked.lock");
    symlink(&lock_target, lock_link)?;
    assert!(matches!(
        FilePermissionStorage::new(directory.path().join("symlinked")).load(),
        Err(PermissionError::Storage(_))
    ));
    fs::set_permissions(&target, fs::Permissions::from_mode(0o644))?;
    assert!(matches!(
        FilePermissionStorage::new(target).load(),
        Err(PermissionError::Storage(_))
    ));
    Ok(())
}

#[test]
fn memory_store_shares_conditional_state_between_clones() -> TestResult {
    let storage = MemoryPermissionStorage::new();
    let clone = storage.clone();
    storage.compare_and_swap(None, &snapshot(1, "first"))?;
    assert_eq!(clone.load()?.map(|state| state.revision), Some(1));
    assert_eq!(
        clone.compare_and_swap(None, &snapshot(1, "different")),
        Err(PermissionError::Conflict {
            expected: None,
            actual: Some(1)
        })
    );
    Ok(())
}
