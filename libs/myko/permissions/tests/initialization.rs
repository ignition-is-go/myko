#![allow(clippy::panic_in_result_fn)]

use myko_permissions::{CredentialScopeMode, FilePermissionStorage, Permissions};
use myko_server::MykoServer;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn corrupt_permission_storage_aborts_initialization_before_the_listener_opens() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let journal = directory.path().join("permissions.journal");
    std::fs::write(&journal, b"invalid permission snapshot\n")?;
    let storage = Arc::new(FilePermissionStorage::new(journal));
    let permissions = Arc::new(Permissions::new(storage, CredentialScopeMode::LocalGrants));
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let observed_hook = hook_ran.clone();
    let server = MykoServer::builder()
        .with_bind_addr(address)
        .try_after_init(move |server| {
            observed_hook.store(true, Ordering::SeqCst);
            assert!(std::net::TcpStream::connect(address).is_err());
            permissions.attach(Arc::new(server.ctx()))?;
            Ok(())
        })
        .build();
    let error = tokio::time::timeout(Duration::from_secs(3), server.run())
        .await?
        .err()
        .ok_or("startup unexpectedly accepted corrupt permissions")?;
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        error.to_string().contains("permission storage failed"),
        "{error}"
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    let _rebound = tokio::net::TcpListener::bind(address).await?;
    Ok(())
}
