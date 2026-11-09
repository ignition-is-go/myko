//! Conditional permission snapshot storage.
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    error::{PermissionError, PermissionResult},
    model::{AUTHORITY_ID, PermissionAuthority},
};

pub(crate) mod entities;

const MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAX_HISTORY_RECORDS: usize = 4096;

/// Stores each whole policy revision with an atomic conditional commit.
///
/// Implementations must compare the stored revision and publish the next revision
/// as one durable operation. An identical retry may succeed without another write.
pub trait PermissionStorage: Send + Sync + 'static {
    /// Bind a storage adapter to the initialized server after normal event replay.
    /// # Errors
    /// Rejects an incompatible runtime or unavailable reactive queries.
    fn attach(&self, _context: Arc<myko::server::MykoServerContext>) -> PermissionResult<()> {
        Ok(())
    }
    /// A reactive source's current change generation; external stores return None.
    fn reactive_revision(&self) -> Option<u64> {
        None
    }

    /// # Errors
    /// Returns an error when the accepted snapshot cannot be read or validated.
    fn load(&self) -> PermissionResult<Option<PermissionAuthority>>;

    /// Read a shared reactive value without cloning the complete policy on every check.
    /// # Errors
    /// Returns the same storage and validation errors as load.
    fn load_shared(&self) -> PermissionResult<Option<Arc<PermissionAuthority>>> {
        self.load().map(|snapshot| snapshot.map(Arc::new))
    }

    /// # Errors
    /// Returns a conflict when the expected revision is stale, or a storage error.
    fn compare_and_swap(
        &self,
        expected: Option<u64>,
        next: &PermissionAuthority,
    ) -> PermissionResult<()>;
}

/// An append-only local journal protected by a separate operating-system lock.
///
/// Every accepted line contains one complete policy snapshot. A newline is the
/// record terminator; an incomplete tail is discarded before the next append.
/// The lock and journal must reside on a local filesystem with working file locks.
/// Commit success follows file and directory synchronization on Unix systems.
#[derive(Clone, Debug)]
pub struct FilePermissionStorage {
    path: PathBuf,
}

impl FilePermissionStorage {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the recorded administration and operator history.
    ///
    /// # Errors
    /// Returns an error for invalid records or more than 4096 history entries.
    pub fn history(&self) -> PermissionResult<Vec<PermissionAuthority>> {
        self.with_lock(|path| {
            let Some(file) = open_existing(path)? else {
                return Ok(Vec::new());
            };
            let mut history = Vec::new();
            scan(file, |record| {
                if history.len() >= MAX_HISTORY_RECORDS {
                    return Err(PermissionError::Storage(
                        "permission history exceeds the bounded history result".into(),
                    ));
                }
                history.push(record.clone());
                Ok(())
            })?;
            Ok(history)
        })
    }

    fn with_lock<T>(
        &self,
        operation: impl FnOnce(&Path) -> PermissionResult<T>,
    ) -> PermissionResult<T> {
        let path = self.resolved_path()?;
        let mut lock_name = OsString::from(path.as_os_str());
        lock_name.push(".lock");
        let lock_path = PathBuf::from(lock_name);
        reject_unsafe_file(&lock_path)?;
        let lock = private_options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(storage_error)?;
        validate_open_file(&lock)?;
        File::lock(&lock).map_err(storage_error)?;
        let result = operation(&path);
        let unlock = File::unlock(&lock).map_err(storage_error);
        match result {
            Ok(value) => {
                unlock?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    fn resolved_path(&self) -> PermissionResult<PathBuf> {
        let name = self.path.file_name().ok_or_else(|| {
            PermissionError::Storage("permission journal path must name a file".into())
        })?;
        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent).map_err(storage_error)?;
        let parent = parent.canonicalize().map_err(storage_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if fs::metadata(&parent)
                .map_err(storage_error)?
                .permissions()
                .mode()
                & 0o022
                != 0
            {
                return Err(PermissionError::Storage(
                    "permission journal directory must not be group- or world-writable".into(),
                ));
            }
        }
        Ok(parent.join(name))
    }
}

impl PermissionStorage for FilePermissionStorage {
    fn load(&self) -> PermissionResult<Option<PermissionAuthority>> {
        self.with_lock(|path| match open_existing(path)? {
            Some(file) => Ok(scan(file, |_| Ok(()))?.latest),
            None => Ok(None),
        })
    }

    fn compare_and_swap(
        &self,
        expected: Option<u64>,
        next: &PermissionAuthority,
    ) -> PermissionResult<()> {
        self.with_lock(|path| {
            reject_unsafe_file(path)?;
            let journal = match open_existing(path)? {
                Some(file) => scan(file, |_| Ok(()))?,
                None => JournalState::default(),
            };
            if journal.latest.as_ref() == Some(next) {
                return Ok(());
            }
            validate_next(expected, journal.latest.as_ref(), next)?;
            let mut encoded = serde_json::to_vec(next).map_err(storage_error)?;
            let length = u64::try_from(encoded.len()).map_err(storage_error)?;
            if length.saturating_add(1) > MAX_RECORD_BYTES {
                return Err(PermissionError::Invalid(
                    "permission snapshot exceeds 16 MiB".into(),
                ));
            }
            encoded.push(b'\n');
            let mut file = private_options()
                .read(true)
                .append(true)
                .create(true)
                .open(path)
                .map_err(storage_error)?;
            validate_open_file(&file)?;
            file.set_len(journal.committed_bytes)
                .map_err(storage_error)?;
            file.write_all(&encoded).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
            sync_directory_chain(path.parent().unwrap_or_else(|| Path::new(".")))?;
            Ok(())
        })
    }
}

#[derive(Default)]
struct JournalState {
    latest: Option<PermissionAuthority>,
    committed_bytes: u64,
}

fn scan(
    file: File,
    mut record: impl FnMut(&PermissionAuthority) -> PermissionResult<()>,
) -> PermissionResult<JournalState> {
    let mut input = BufReader::new(file);
    let mut result = JournalState::default();
    loop {
        let mut line = Vec::new();
        let count = Read::take(&mut input, MAX_RECORD_BYTES.saturating_add(1))
            .read_until(b'\n', &mut line)
            .map_err(storage_error)?;
        if count == 0 {
            return Ok(result);
        }
        if u64::try_from(count).map_err(storage_error)? > MAX_RECORD_BYTES {
            return Err(PermissionError::Storage(
                "permission journal record exceeds 16 MiB".into(),
            ));
        }
        if line.last() != Some(&b'\n') {
            return Ok(result);
        }
        let snapshot: PermissionAuthority = serde_json::from_slice(&line).map_err(|error| {
            PermissionError::Storage(format!("invalid committed permission record: {error}"))
        })?;
        let expected = result.latest.as_ref().map(|state| state.revision);
        validate_next(expected, result.latest.as_ref(), &snapshot).map_err(|error| {
            PermissionError::Storage(format!("invalid permission journal sequence: {error}"))
        })?;
        record(&snapshot)?;
        result.committed_bytes = result
            .committed_bytes
            .checked_add(u64::try_from(count).map_err(storage_error)?)
            .ok_or_else(|| PermissionError::Storage("permission journal offset overflow".into()))?;
        result.latest = Some(snapshot);
    }
}

fn validate_next(
    expected: Option<u64>,
    current: Option<&PermissionAuthority>,
    next: &PermissionAuthority,
) -> PermissionResult<()> {
    let actual = current.map(|state| state.revision);
    if actual != expected {
        return Err(PermissionError::Conflict { expected, actual });
    }
    let revision = actual
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| PermissionError::Invalid("permission revision exhausted".into()))?;
    if next.revision != revision
        || next.id.to_string() != AUTHORITY_ID
        || next.change_id.trim().is_empty()
    {
        return Err(PermissionError::Invalid(
            "permission snapshot requires the canonical ID, next revision, and a change ID".into(),
        ));
    }
    Ok(())
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn open_existing(path: &Path) -> PermissionResult<Option<File>> {
    reject_unsafe_file(path)?;
    match File::open(path) {
        Ok(file) => {
            validate_open_file(&file)?;
            Ok(Some(file))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_error(error)),
    }
}

fn reject_unsafe_file(path: &Path) -> PermissionResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => Err(
            PermissionError::Storage("permission journal and lock must be regular files".into()),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_error(error)),
    }
}

fn validate_open_file(file: &File) -> PermissionResult<()> {
    let metadata = file.metadata().map_err(storage_error)?;
    if !metadata.is_file() {
        return Err(PermissionError::Storage(
            "permission storage requires regular files".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(PermissionError::Storage(
                "permission files must have private permissions and no additional hard links"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn sync_directory_chain(path: &Path) -> PermissionResult<()> {
    #[cfg(unix)]
    for directory in path.ancestors() {
        File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(storage_error)?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> PermissionError {
    PermissionError::Storage(error.to_string())
}

/// A process-local conditional store for tests and ephemeral applications.
/// This implementation is not durable.
#[derive(Clone, Default)]
pub struct MemoryPermissionStorage {
    state: Arc<Mutex<Option<PermissionAuthority>>>,
}
impl MemoryPermissionStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
impl PermissionStorage for MemoryPermissionStorage {
    fn load(&self) -> PermissionResult<Option<PermissionAuthority>> {
        Ok(self.state.lock().map_err(storage_error)?.clone())
    }
    fn compare_and_swap(
        &self,
        expected: Option<u64>,
        next: &PermissionAuthority,
    ) -> PermissionResult<()> {
        let mut current = self.state.lock().map_err(storage_error)?;
        if current.as_ref() == Some(next) {
            return Ok(());
        }
        validate_next(expected, current.as_ref(), next)?;
        *current = Some(next.clone());
        drop(current);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
