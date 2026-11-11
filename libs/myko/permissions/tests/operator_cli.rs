#![allow(clippy::panic_in_result_fn)]

use myko_permissions::{
    FilePermissionStorage, PermissionAuditActor, PermissionAuthority, PermissionChangeMode,
    PermissionChangeResult, PermissionStorage,
};
use std::process::{Command, Output};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn cli(operation: &str, store: &std::path::Path, arguments: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_myko-permissions"))
        .arg(operation)
        .arg("--store")
        .arg(store)
        .args(arguments)
        .output()
}

#[test]
fn real_operator_cli_bootstraps_prevents_implicit_lockout_and_recovers_a_locked_policy()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("permissions.journal");
    let first = bootstrap_and_show(&path)?;
    let mut locked = first.document;
    for principal in &mut locked.principals {
        principal.enabled = false;
    }
    let document_path = directory.path().join("locked.json");
    std::fs::write(&document_path, serde_json::to_vec(&locked)?)?;
    let document_path = document_path.to_str().ok_or("document path")?;
    let arguments = [
        "--document",
        document_path,
        "--expected-revision",
        "1",
        "--operator",
        "host-operator",
        "--reason",
        "intentional lockout",
    ];
    let refused = cli("replace", &path, &arguments)?;
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("LockedOut")
            || String::from_utf8_lossy(&refused.stderr).contains("last effective")
    );
    let mut deliberate = arguments.to_vec();
    deliberate.push("--allow-lockout");
    let deliberate = cli("replace", &path, &deliberate)?;
    assert!(
        deliberate.status.success(),
        "{}",
        String::from_utf8_lossy(&deliberate.stderr)
    );
    let recovered = cli(
        "recover",
        &path,
        &[
            "--issuer",
            "fixture",
            "--subject",
            "replacement-admin",
            "--operator",
            "host-operator",
            "--reason",
            "restore access",
        ],
    )?;
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    let storage = FilePermissionStorage::new(path.clone());
    let current = storage.load()?.ok_or("recovered snapshot missing")?;
    assert_eq!(current.revision, 3);
    assert_eq!(current.mode, PermissionChangeMode::Recovery);
    assert_eq!(
        current.actor,
        PermissionAuditActor::Operator {
            identity: "host-operator".into()
        }
    );
    assert!(
        current
            .document
            .principals
            .iter()
            .any(|principal| principal.subject == "replacement-admin" && principal.enabled)
    );
    assert_eq!(storage.history()?.len(), 3);
    let duplicate = cli(
        "bootstrap",
        &path,
        &[
            "--issuer",
            "fixture",
            "--subject",
            "another-admin",
            "--operator",
            "host-operator",
            "--reason",
            "duplicate bootstrap",
        ],
    )?;
    assert!(!duplicate.status.success());
    assert_eq!(storage.load()?.map(|snapshot| snapshot.revision), Some(3));
    Ok(())
}

fn bootstrap_and_show(path: &std::path::Path) -> TestResult<PermissionAuthority> {
    let bootstrap = cli(
        "bootstrap",
        path,
        &[
            "--issuer",
            "fixture",
            "--subject",
            "first-admin",
            "--operator",
            "host-operator",
            "--reason",
            "initial administrator",
        ],
    )?;
    assert!(
        bootstrap.status.success(),
        "{}",
        String::from_utf8_lossy(&bootstrap.stderr)
    );
    let result: PermissionChangeResult = serde_json::from_slice(&bootstrap.stdout)?;
    assert_eq!(result.revision, 1);
    let show = cli("show", path, &[])?;
    assert!(show.status.success());
    let first: Option<PermissionAuthority> = serde_json::from_slice(&show.stdout)?;
    let first = first.ok_or("missing initial policy")?;
    assert_eq!(first.mode, PermissionChangeMode::Bootstrap);
    assert_eq!(
        first
            .document
            .principals
            .first()
            .map(|principal| principal.subject.as_str()),
        Some("first-admin")
    );
    Ok(first)
}
