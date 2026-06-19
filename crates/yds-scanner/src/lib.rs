use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ignore::{WalkBuilder, WalkState};
use rusqlite::{
    backup::{Backup, StepResult},
    Connection, ErrorCode, OpenFlags,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;
use yds_core::{
    config::SyncConfig,
    exclusions::{ExclusionDecision, ExclusionMatcher, ExclusionReason},
    path_mapping::{sanitize_remote_path, PathMappingError},
    ComponentStatus,
};
use yds_state::{
    models::{FailedItemRecord, FileStateRecord, SkippedItemRecord},
    StateError, StateRepository,
};

pub const COMPONENT_NAME: &str = "scanner";
pub const SKIP_REASON_FILE_TOO_LARGE: &str = "file_too_large";
pub const SKIP_REASON_EXCLUDED: &str = "excluded";
pub const SKIP_REASON_LOCAL_FILE_UNAVAILABLE: &str = "local_file_unavailable";
pub const FAILED_OPERATION_SCAN: &str = "scan";
const SQLITE_BACKUP_PAGES_PER_STEP: i32 = 1024;
const SQLITE_BACKUP_PAUSE: Duration = Duration::from_millis(5);
const SQLITE_BACKUP_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
const SQLITE_BACKUP_TOTAL_TIMEOUT: Duration = Duration::from_secs(2 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOptions {
    pub global_excludes: Vec<String>,
    pub absolute_excludes: Vec<String>,
    pub max_file_size_bytes: u64,
    pub full_hash_max_bytes: u64,
    pub large_file_name_size_only_min_bytes: u64,
    pub scan_concurrency: usize,
    pub staging_dir: PathBuf,
}

impl ScanOptions {
    #[must_use]
    pub fn from_config(
        sync: &SyncConfig,
        global_excludes: &[String],
        absolute_excludes: &[String],
        staging_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            global_excludes: global_excludes.to_vec(),
            absolute_excludes: absolute_excludes.to_vec(),
            max_file_size_bytes: sync.max_file_size_bytes,
            full_hash_max_bytes: sync.full_hash_max_bytes,
            large_file_name_size_only_min_bytes: sync.large_file_name_size_only_min_bytes,
            scan_concurrency: sync.scan_concurrency,
            staging_dir: staging_dir.into(),
        }
    }

    #[must_use]
    pub fn effective_scan_concurrency(&self) -> usize {
        if self.scan_concurrency > 0 {
            return self.scan_concurrency;
        }

        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRoot {
    pub id: String,
    pub local_path: PathBuf,
    pub canonical_remote_path: String,
    pub excludes: Vec<String>,
}

impl ScanRoot {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        local_path: impl Into<PathBuf>,
        canonical_remote_path: impl Into<String>,
        excludes: Vec<String>,
    ) -> Self {
        Self {
            id: id.into(),
            local_path: local_path.into(),
            canonical_remote_path: canonical_remote_path.into(),
            excludes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanReport {
    pub root_id: String,
    pub files: Vec<LocalFileEntry>,
    pub directories: Vec<LocalDirectoryEntry>,
    pub skipped: Vec<SkippedScanItem>,
    pub failed: Vec<FailedScanItem>,
}

impl ScanReport {
    #[must_use]
    pub fn new(root_id: impl Into<String>) -> Self {
        Self {
            root_id: root_id.into(),
            files: Vec::new(),
            directories: Vec::new(),
            skipped: Vec::new(),
            failed: Vec::new(),
        }
    }

    pub fn cleanup_staging_files(&self) -> Result<(), ScannerError> {
        for file in &self.files {
            if let Some(staging_path) = file.sqlite_backup_staging_path() {
                match fs::remove_file(staging_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(ScannerError::Io {
                            path: staging_path.to_path_buf(),
                            source,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn sort_deterministically(&mut self) {
        self.files
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        self.directories
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        self.skipped
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        self.failed
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFileEntry {
    pub root_id: String,
    pub local_path: PathBuf,
    pub relative_path: String,
    pub remote_path: String,
    pub normalized_remote_path: String,
    pub upload_source: PathBuf,
    pub size_bytes: u64,
    pub mtime_ns: Option<i64>,
    pub ctime_ns: Option<i64>,
    pub fingerprint: Fingerprint,
    sqlite_backup_staging_path: Option<PathBuf>,
}

impl LocalFileEntry {
    #[must_use]
    pub fn sqlite_backup_staging_path(&self) -> Option<&Path> {
        self.sqlite_backup_staging_path.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDirectoryEntry {
    pub root_id: String,
    pub local_path: PathBuf,
    pub relative_path: String,
    pub remote_path: String,
    pub normalized_remote_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    pub kind: FingerprintKind,
    pub value: String,
    pub full_sha256: Option<String>,
    pub local_md5: Option<String>,
    pub reused_from_state: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FingerprintKind {
    Sha256,
    Metadata,
    Deleted,
    Unknown,
}

impl FingerprintKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Metadata => "metadata",
            Self::Deleted => "deleted",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub fn from_state(value: &str) -> Self {
        match value {
            "sha256" => Self::Sha256,
            "metadata" => Self::Metadata,
            "deleted" => Self::Deleted,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedScanItem {
    pub root_id: String,
    pub local_path: PathBuf,
    pub relative_path: String,
    pub remote_path: Option<String>,
    pub reason_code: String,
    pub reason_details: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedScanItem {
    pub root_id: String,
    pub local_path: Option<PathBuf>,
    pub relative_path: String,
    pub remote_path: Option<String>,
    pub error_kind: String,
    pub error_message: String,
}

#[derive(Debug, Error)]
pub enum ScannerError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid exclude rules: {0}")]
    Exclusion(#[from] yds_core::exclusions::ExclusionError),
    #[error("path mapping error: {0}")]
    PathMapping(#[from] PathMappingError),
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("SQLite backup error at {path}: {source}")]
    SqliteBackup {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("SQLite backup timed out at {path} after {elapsed_seconds}s")]
    SqliteBackupTimedOut { path: PathBuf, elapsed_seconds: u64 },
    #[error("fixture output directory is not empty: {0}")]
    FixtureOutputNotEmpty(PathBuf),
    #[error("fixture file count and max depth must be greater than zero")]
    InvalidFixtureArguments,
    #[error("scanner worker mutex was poisoned")]
    MutexPoisoned,
}

#[derive(Debug, Clone)]
pub struct Scanner {
    options: ScanOptions,
}

impl Scanner {
    #[must_use]
    pub fn new(options: ScanOptions) -> Self {
        Self { options }
    }

    pub fn scan_root(
        &self,
        root: &ScanRoot,
        previous_states: &[FileStateRecord],
    ) -> Result<ScanReport, ScannerError> {
        let pattern_matcher = Arc::new(ExclusionMatcher::new(
            &self.options.global_excludes,
            &root.excludes,
            &[],
        )?);
        let mut absolute_excludes = self.options.absolute_excludes.clone();
        absolute_excludes.push(path_to_stable_string(&self.options.staging_dir));
        let absolute_matcher = Arc::new(ExclusionMatcher::new(&[], &[], &absolute_excludes)?);
        let previous_states = Arc::new(previous_state_map(previous_states));
        let partial = Arc::new(Mutex::new(ScanReport::new(root.id.clone())));
        let scan_context = Arc::new(ScanContext {
            options: self.options.clone(),
            root: root.clone(),
            pattern_matcher,
            absolute_matcher,
            previous_states,
        });

        let mut builder = WalkBuilder::new(&root.local_path);
        builder
            .standard_filters(false)
            .hidden(false)
            .ignore(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .parents(false)
            .threads(self.options.effective_scan_concurrency());

        let context = Arc::clone(&scan_context);
        let report = Arc::clone(&partial);
        builder.build_parallel().run(|| {
            let context = Arc::clone(&context);
            let report = Arc::clone(&report);
            Box::new(move |entry| match entry {
                Ok(entry) => handle_walk_entry(&context, &report, &entry),
                Err(error) => {
                    let failure = FailedScanItem {
                        root_id: context.root.id.clone(),
                        local_path: None,
                        relative_path: String::new(),
                        remote_path: None,
                        error_kind: "walk".to_string(),
                        error_message: error.to_string(),
                    };
                    push_failed(&report, failure);
                    WalkState::Continue
                }
            })
        });

        let mut report = partial
            .lock()
            .map_err(|_| ScannerError::MutexPoisoned)?
            .clone();
        report.sort_deterministically();
        Ok(report)
    }

    pub fn scan_root_with_repository(
        &self,
        root: &ScanRoot,
        repository: &StateRepository,
    ) -> Result<ScanReport, ScannerError> {
        let previous_states = repository.list_file_states_for_root(&root.id)?;
        self.scan_root(root, &previous_states)
    }
}

#[derive(Debug)]
struct ScanContext {
    options: ScanOptions,
    root: ScanRoot,
    pattern_matcher: Arc<ExclusionMatcher>,
    absolute_matcher: Arc<ExclusionMatcher>,
    previous_states: Arc<HashMap<String, FileStateRecord>>,
}

pub fn record_scan_findings(
    repository: &StateRepository,
    report: &ScanReport,
) -> Result<(), ScannerError> {
    for skipped in &report.skipped {
        repository.mark_skipped(&SkippedItemRecord {
            root_id: skipped.root_id.clone(),
            local_path: path_to_stable_string(&skipped.local_path),
            relative_path: Some(skipped.relative_path.clone()),
            remote_path: skipped.remote_path.clone(),
            reason_code: skipped.reason_code.clone(),
            reason_details: skipped.reason_details.clone(),
            size_bytes: skipped.size_bytes.and_then(u64_to_i64),
        })?;
    }

    for failed in &report.failed {
        repository.mark_failed(&FailedItemRecord {
            root_id: failed.root_id.clone(),
            local_path: failed.local_path.as_ref().map(path_to_stable_string),
            remote_path: failed.remote_path.clone(),
            operation: FAILED_OPERATION_SCAN.to_string(),
            error_kind: failed.error_kind.clone(),
            error_message: failed.error_message.clone(),
            retry_count: 0,
            next_retry_at_utc: None,
        })?;
    }

    Ok(())
}

pub fn generate_fixture_tree(
    output: impl AsRef<Path>,
    files: usize,
    max_depth: usize,
) -> Result<(), ScannerError> {
    if files == 0 || max_depth == 0 {
        return Err(ScannerError::InvalidFixtureArguments);
    }

    let output = output.as_ref();
    if output.exists()
        && output
            .read_dir()
            .map_err(|source| ScannerError::Io {
                path: output.to_path_buf(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(ScannerError::FixtureOutputNotEmpty(output.to_path_buf()));
    }
    fs::create_dir_all(output).map_err(|source| ScannerError::Io {
        path: output.to_path_buf(),
        source,
    })?;

    for index in 0..files {
        let depth = (index % max_depth) + 1;
        let mut directory = output.to_path_buf();
        for level in 0..depth {
            directory.push(format!("d{level:02}"));
        }
        fs::create_dir_all(&directory).map_err(|source| ScannerError::Io {
            path: directory.clone(),
            source,
        })?;

        let file_path = directory.join(format!("file-{index:08}.txt"));
        let mut file = File::create(&file_path).map_err(|source| ScannerError::Io {
            path: file_path.clone(),
            source,
        })?;
        writeln!(file, "ya-disk-sync fixture file {index}").map_err(|source| ScannerError::Io {
            path: file_path,
            source,
        })?;
    }

    Ok(())
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(
        COMPONENT_NAME,
        "filesystem scanner, fingerprinting and SQLite backup boundary is available",
    )
}

fn handle_walk_entry(
    context: &ScanContext,
    report: &Mutex<ScanReport>,
    entry: &ignore::DirEntry,
) -> WalkState {
    let path = entry.path();
    if path == context.root.local_path {
        return WalkState::Continue;
    }

    let file_type = entry.file_type();
    let is_dir = file_type.is_some_and(|file_type| file_type.is_dir());
    let is_file = file_type.is_some_and(|file_type| file_type.is_file());

    let relative = match relative_path(&context.root.local_path, path) {
        Ok(relative) => relative,
        Err(error) => {
            push_failed(
                report,
                FailedScanItem {
                    root_id: context.root.id.clone(),
                    local_path: Some(path.to_path_buf()),
                    relative_path: String::new(),
                    remote_path: None,
                    error_kind: "relative_path".to_string(),
                    error_message: error.to_string(),
                },
            );
            return WalkState::Continue;
        }
    };

    match exclusion_decision(context, path, &relative, is_dir) {
        Some(decision) if decision.is_excluded() => {
            let remote_path =
                remote_path_for_relative(&context.root.canonical_remote_path, &relative).ok();
            push_skipped(
                report,
                SkippedScanItem {
                    root_id: context.root.id.clone(),
                    local_path: path.to_path_buf(),
                    relative_path: relative,
                    remote_path,
                    reason_code: SKIP_REASON_EXCLUDED.to_string(),
                    reason_details: Some(format!("{:?}", decision.reason())),
                    size_bytes: None,
                },
            );
            return if is_dir {
                WalkState::Skip
            } else {
                WalkState::Continue
            };
        }
        _ => {}
    }

    if is_dir {
        let directory = match directory_entry(context, path, &relative) {
            Ok(directory) => directory,
            Err(error) => {
                push_failed(
                    report,
                    FailedScanItem {
                        root_id: context.root.id.clone(),
                        local_path: Some(path.to_path_buf()),
                        relative_path: relative,
                        remote_path: None,
                        error_kind: "path_mapping".to_string(),
                        error_message: error.to_string(),
                    },
                );
                return WalkState::Continue;
            }
        };
        push_directory(report, directory);
    } else if is_file {
        match file_entry(context, path, &relative) {
            Ok(FileEntryResult::File(file)) => push_file(report, file),
            Ok(FileEntryResult::Skipped(skipped)) => push_skipped(report, skipped),
            Err(error) => {
                let remote_path =
                    remote_path_for_relative(&context.root.canonical_remote_path, &relative).ok();
                if is_skippable_scan_error(&error) {
                    push_skipped(
                        report,
                        SkippedScanItem {
                            root_id: context.root.id.clone(),
                            local_path: path.to_path_buf(),
                            relative_path: relative,
                            remote_path,
                            reason_code: SKIP_REASON_LOCAL_FILE_UNAVAILABLE.to_string(),
                            reason_details: Some(error.to_string()),
                            size_bytes: fs::metadata(path).ok().map(|metadata| metadata.len()),
                        },
                    );
                    return WalkState::Continue;
                }
                push_failed(
                    report,
                    FailedScanItem {
                        root_id: context.root.id.clone(),
                        local_path: Some(path.to_path_buf()),
                        relative_path: relative,
                        remote_path,
                        error_kind: "scan".to_string(),
                        error_message: error.to_string(),
                    },
                );
            }
        }
    }

    WalkState::Continue
}

fn exclusion_decision(
    context: &ScanContext,
    absolute_path: &Path,
    relative: &str,
    is_dir: bool,
) -> Option<ExclusionDecision> {
    let absolute = context
        .absolute_matcher
        .decision_for_path(absolute_path, is_dir);
    if absolute.is_excluded() && absolute.reason() == ExclusionReason::Absolute {
        return Some(absolute);
    }

    let pattern = context
        .pattern_matcher
        .decision_for_path(Path::new(relative), is_dir);
    if pattern.is_excluded() {
        return Some(pattern);
    }

    None
}

fn directory_entry(
    context: &ScanContext,
    path: &Path,
    relative: &str,
) -> Result<LocalDirectoryEntry, ScannerError> {
    let remote_path = remote_path_for_relative(&context.root.canonical_remote_path, relative)?;
    Ok(LocalDirectoryEntry {
        root_id: context.root.id.clone(),
        local_path: path.to_path_buf(),
        relative_path: relative.to_string(),
        normalized_remote_path: remote_path.to_lowercase(),
        remote_path,
    })
}

enum FileEntryResult {
    File(LocalFileEntry),
    Skipped(SkippedScanItem),
}

fn file_entry(
    context: &ScanContext,
    path: &Path,
    relative: &str,
) -> Result<FileEntryResult, ScannerError> {
    let metadata = fs::metadata(path).map_err(|source| ScannerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let source_size = metadata.len();
    let remote_path = remote_path_for_relative(&context.root.canonical_remote_path, relative)?;
    let normalized_remote_path = remote_path.to_lowercase();
    let mtime_ns = metadata.modified().ok().and_then(system_time_to_ns);
    let ctime_ns = metadata.created().ok().and_then(system_time_to_ns);

    if source_size > context.options.max_file_size_bytes {
        return Ok(FileEntryResult::Skipped(SkippedScanItem {
            root_id: context.root.id.clone(),
            local_path: path.to_path_buf(),
            relative_path: relative.to_string(),
            remote_path: Some(remote_path),
            reason_code: SKIP_REASON_FILE_TOO_LARGE.to_string(),
            reason_details: Some(format!(
                "file size {source_size} exceeds configured max {}",
                context.options.max_file_size_bytes
            )),
            size_bytes: Some(source_size),
        }));
    }

    if let Some(previous) = context.previous_states.get(relative) {
        if previous.size_bytes == source_size as i64
            && previous.mtime_ns == mtime_ns
            && previous.normalized_remote_path == normalized_remote_path
        {
            return Ok(FileEntryResult::File(LocalFileEntry {
                root_id: context.root.id.clone(),
                local_path: path.to_path_buf(),
                relative_path: relative.to_string(),
                remote_path,
                normalized_remote_path,
                upload_source: path.to_path_buf(),
                size_bytes: source_size,
                mtime_ns,
                ctime_ns,
                fingerprint: Fingerprint {
                    kind: FingerprintKind::from_state(&previous.fingerprint_kind),
                    value: previous.fingerprint_value.clone().unwrap_or_default(),
                    full_sha256: previous.full_sha256.clone(),
                    local_md5: previous.remote_md5.clone(),
                    reused_from_state: true,
                },
                sqlite_backup_staging_path: None,
            }));
        }
    }

    let mut upload_source = path.to_path_buf();
    let mut sqlite_backup_staging_path = None;
    if is_sqlite_candidate(path) {
        let staging_path =
            backup_sqlite_to_staging(path, &context.options.staging_dir, &context.root.id)?;
        upload_source = staging_path.clone();
        sqlite_backup_staging_path = Some(staging_path);
    }

    let fingerprint = fingerprint_path(&upload_source, source_size, mtime_ns, &context.options)?;

    Ok(FileEntryResult::File(LocalFileEntry {
        root_id: context.root.id.clone(),
        local_path: path.to_path_buf(),
        relative_path: relative.to_string(),
        remote_path,
        normalized_remote_path,
        upload_source,
        size_bytes: source_size,
        mtime_ns,
        ctime_ns,
        fingerprint,
        sqlite_backup_staging_path,
    }))
}

fn fingerprint_path(
    path: &Path,
    source_size: u64,
    source_mtime_ns: Option<i64>,
    options: &ScanOptions,
) -> Result<Fingerprint, ScannerError> {
    if source_size <= options.full_hash_max_bytes {
        let (sha256, md5) = sha256_and_md5_file(path)?;
        return Ok(Fingerprint {
            kind: FingerprintKind::Sha256,
            value: sha256.clone(),
            full_sha256: Some(sha256),
            local_md5: Some(md5),
            reused_from_state: false,
        });
    }

    Ok(Fingerprint {
        kind: FingerprintKind::Metadata,
        value: metadata_fingerprint_value(source_size, source_mtime_ns),
        full_sha256: None,
        local_md5: None,
        reused_from_state: false,
    })
}

fn sha256_and_md5_file(path: &Path) -> Result<(String, String), ScannerError> {
    let mut file = File::open(path).map_err(|source| ScannerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut sha256 = Sha256::new();
    let mut md5 = md5::Md5::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer).map_err(|source| ScannerError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        sha256.update(&buffer[..read]);
        md5.update(&buffer[..read]);
    }

    Ok((hex::encode(sha256.finalize()), hex::encode(md5.finalize())))
}

fn is_skippable_scan_error(error: &ScannerError) -> bool {
    match error {
        ScannerError::Io { source, .. } => is_skippable_local_io_error(source),
        ScannerError::SqliteBackupTimedOut { .. } => true,
        ScannerError::SqliteBackup { source, .. } => is_skippable_sqlite_backup_error(source),
        _ => false,
    }
}

fn is_skippable_local_io_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || matches!(error.raw_os_error(), Some(32 | 33))
}

fn is_skippable_sqlite_backup_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if matches!(inner.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn metadata_fingerprint_value(size_bytes: u64, mtime_ns: Option<i64>) -> String {
    format!(
        "size={size_bytes};mtime_ns={}",
        mtime_ns.unwrap_or_default()
    )
}

fn backup_sqlite_to_staging(
    source_path: &Path,
    staging_dir: &Path,
    root_id: &str,
) -> Result<PathBuf, ScannerError> {
    backup_sqlite_to_staging_with_timeouts(
        source_path,
        staging_dir,
        root_id,
        SQLITE_BACKUP_NO_PROGRESS_TIMEOUT,
        SQLITE_BACKUP_TOTAL_TIMEOUT,
    )
}

fn backup_sqlite_to_staging_with_timeouts(
    source_path: &Path,
    staging_dir: &Path,
    root_id: &str,
    no_progress_timeout: Duration,
    total_timeout: Duration,
) -> Result<PathBuf, ScannerError> {
    fs::create_dir_all(staging_dir).map_err(|source| ScannerError::Io {
        path: staging_dir.to_path_buf(),
        source,
    })?;
    let staging_path = staging_dir.join(format!(
        "yds-sqlite-backup-{}-{}.sqlite",
        sanitize_file_name(root_id),
        Uuid::new_v4()
    ));

    let backup_result = {
        let source = Connection::open_with_flags(source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|source| ScannerError::SqliteBackup {
                path: source_path.to_path_buf(),
                source,
            })?;
        let mut destination =
            Connection::open(&staging_path).map_err(|source| ScannerError::SqliteBackup {
                path: staging_path.clone(),
                source,
            })?;
        let backup = Backup::new(&source, &mut destination).map_err(|source| {
            ScannerError::SqliteBackup {
                path: source_path.to_path_buf(),
                source,
            }
        })?;
        run_sqlite_backup_with_deadline(&backup, source_path, no_progress_timeout, total_timeout)
    };
    if let Err(error) = backup_result {
        cleanup_incomplete_sqlite_backup(&staging_path);
        return Err(error);
    }

    Ok(staging_path)
}

fn run_sqlite_backup_with_deadline(
    backup: &Backup<'_, '_>,
    source_path: &Path,
    no_progress_timeout: Duration,
    total_timeout: Duration,
) -> Result<(), ScannerError> {
    let started_at = Instant::now();
    let mut last_progress_at = started_at;
    let mut last_remaining = None;

    loop {
        let step = backup
            .step(SQLITE_BACKUP_PAGES_PER_STEP)
            .map_err(|source| ScannerError::SqliteBackup {
                path: source_path.to_path_buf(),
                source,
            })?;
        let progress = backup.progress();
        if last_remaining != Some(progress.remaining) {
            last_progress_at = Instant::now();
            last_remaining = Some(progress.remaining);
        }

        if step == StepResult::Done {
            return Ok(());
        }

        let elapsed = started_at.elapsed();
        if elapsed >= total_timeout || last_progress_at.elapsed() >= no_progress_timeout {
            return Err(ScannerError::SqliteBackupTimedOut {
                path: source_path.to_path_buf(),
                elapsed_seconds: elapsed.as_secs(),
            });
        }

        thread::sleep(SQLITE_BACKUP_PAUSE);
    }
}

fn cleanup_incomplete_sqlite_backup(staging_path: &Path) {
    let _ = fs::remove_file(staging_path);
    if let Some(file_name) = staging_path.file_name().and_then(|name| name.to_str()) {
        let journal_path = staging_path.with_file_name(format!("{file_name}-journal"));
        let _ = fs::remove_file(journal_path);
    }
}

fn is_sqlite_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "sqlite" | "sqlite3" | "db"
            )
        })
        .unwrap_or(false)
}

fn remote_path_for_relative(
    canonical_remote_path: &str,
    relative: &str,
) -> Result<String, ScannerError> {
    let remote = format!(
        "{}/{}",
        canonical_remote_path.trim_end_matches('/'),
        relative.replace('\\', "/")
    );
    Ok(sanitize_remote_path(&remote)?)
}

fn relative_path(root: &Path, path: &Path) -> Result<String, io::Error> {
    let relative = path
        .strip_prefix(root)
        .map_err(io::Error::other)?
        .to_string_lossy()
        .replace('\\', "/");
    Ok(relative.trim_matches('/').to_string())
}

fn previous_state_map(previous_states: &[FileStateRecord]) -> HashMap<String, FileStateRecord> {
    previous_states
        .iter()
        .cloned()
        .map(|record| (record.relative_path.clone(), record))
        .collect()
}

fn path_to_stable_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
}

fn system_time_to_ns(value: SystemTime) -> Option<i64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn sanitize_file_name(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        .collect();
    if sanitized.is_empty() {
        "root".to_string()
    } else {
        sanitized
    }
}

fn push_file(report: &Mutex<ScanReport>, file: LocalFileEntry) {
    if let Ok(mut report) = report.lock() {
        report.files.push(file);
    }
}

fn push_directory(report: &Mutex<ScanReport>, directory: LocalDirectoryEntry) {
    if let Ok(mut report) = report.lock() {
        report.directories.push(directory);
    }
}

fn push_skipped(report: &Mutex<ScanReport>, skipped: SkippedScanItem) {
    if let Ok(mut report) = report.lock() {
        report.skipped.push(skipped);
    }
}

fn push_failed(report: &Mutex<ScanReport>, failed: FailedScanItem) {
    if let Ok(mut report) = report.lock() {
        report.failed.push(failed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use filetime::{set_file_mtime, FileTime};
    use yds_core::config::default_config;
    use yds_state::models::FileStateStatus;

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), yds_core::ComponentHealth::Ok);
    }

    #[test]
    fn scanner_applies_exclusions_without_implicit_defaults() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path().join(".env"), "secret");
        write_file(temp.path().join(".git").join("config"), "git");
        write_file(temp.path().join("node_modules").join("pkg.js"), "pkg");
        write_file(temp.path().join("target").join("artifact"), "bin");
        write_file(temp.path().join("drop.tmp"), "tmp");
        write_file(temp.path().join("keep.tmp"), "tmp");
        write_file(temp.path().join("nested").join("abs.txt"), "abs");

        let options = ScanOptions {
            global_excludes: vec!["**/*.tmp".to_string()],
            absolute_excludes: vec![path_to_stable_string(temp.path().join("nested"))],
            max_file_size_bytes: 1024,
            full_hash_max_bytes: 1024,
            large_file_name_size_only_min_bytes: 2048,
            scan_concurrency: 2,
            staging_dir: temp.path().join("staging"),
        };
        let scanner = Scanner::new(options);
        let root = ScanRoot::new(
            "root",
            temp.path(),
            "disk:/root",
            vec!["!keep.tmp".to_string()],
        );

        let report = scanner.scan_root(&root, &[]).unwrap();
        let files: Vec<_> = report
            .files
            .iter()
            .map(|file| file.relative_path.as_str())
            .collect();
        let skipped: Vec<_> = report
            .skipped
            .iter()
            .map(|item| item.relative_path.as_str())
            .collect();

        assert!(files.contains(&".env"));
        assert!(files.contains(&".git/config"));
        assert!(files.contains(&"node_modules/pkg.js"));
        assert!(files.contains(&"target/artifact"));
        assert!(files.contains(&"keep.tmp"));
        assert!(skipped.contains(&"drop.tmp"));
        assert!(skipped.contains(&"nested"));
    }

    #[test]
    fn scanner_ordering_is_deterministic_across_concurrency() {
        let temp = tempfile::tempdir().unwrap();
        for index in (0..50).rev() {
            write_file(temp.path().join(format!("d/file-{index:02}.txt")), "data");
        }

        let report_one = test_scan(temp.path(), 1, 1024, 1024);
        let report_many = test_scan(temp.path(), 4, 1024, 1024);
        let one: Vec<_> = report_one
            .files
            .iter()
            .map(|file| &file.relative_path)
            .collect();
        let many: Vec<_> = report_many
            .files
            .iter()
            .map(|file| &file.relative_path)
            .collect();

        assert_eq!(one, many);
    }

    #[test]
    fn unchanged_by_metadata_reuses_previous_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("same.txt");
        write_file(&file_path, "aaaa");
        let fixed_mtime = FileTime::from_unix_time(1_700_000_000, 123);
        set_file_mtime(&file_path, fixed_mtime).unwrap();

        let first = test_scan(temp.path(), 1, 1024, 1024);
        let mut previous = state_record_from_entry(&first.files[0]);
        previous.fingerprint_value = Some("previous-fingerprint".to_string());
        previous.full_sha256 = Some("previous-sha".to_string());

        write_file(&file_path, "bbbb");
        set_file_mtime(&file_path, fixed_mtime).unwrap();

        let scanner = Scanner::new(test_options(temp.path(), 1, 1024, 1024));
        let root = test_root(temp.path());
        let second = scanner.scan_root(&root, &[previous]).unwrap();

        assert!(second.files[0].fingerprint.reused_from_state);
        assert_eq!(second.files[0].fingerprint.value, "previous-fingerprint");
    }

    #[test]
    fn small_files_use_sha256_and_large_files_use_metadata() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path().join("small.txt"), "abc");
        write_file(temp.path().join("large.bin"), &"x".repeat(128));

        let report = test_scan(temp.path(), 1, 16, 1024);
        let small = report
            .files
            .iter()
            .find(|file| file.relative_path == "small.txt")
            .unwrap();
        let large = report
            .files
            .iter()
            .find(|file| file.relative_path == "large.bin")
            .unwrap();

        assert_eq!(small.fingerprint.kind, FingerprintKind::Sha256);
        assert_eq!(
            small.fingerprint.full_sha256.as_deref(),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(large.fingerprint.kind, FingerprintKind::Metadata);
        assert!(large.fingerprint.full_sha256.is_none());
    }

    #[test]
    fn file_larger_than_threshold_is_skipped() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path().join("big.bin"), "1234567890");

        let report = test_scan(temp.path(), 1, 1024, 5);

        assert!(report.files.is_empty());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].reason_code, SKIP_REASON_FILE_TOO_LARGE);
    }

    #[test]
    fn sqlite_backup_creates_staging_and_cleanup_removes_it() {
        let temp = tempfile::tempdir().unwrap();
        let sqlite_path = temp.path().join("data.sqlite");
        create_sqlite_db(&sqlite_path);

        let report = test_scan(temp.path(), 1, 1024 * 1024, 1024 * 1024);
        let file = report
            .files
            .iter()
            .find(|file| file.relative_path == "data.sqlite")
            .unwrap();
        let staging = file.sqlite_backup_staging_path().unwrap().to_path_buf();

        assert_ne!(file.upload_source, sqlite_path);
        assert!(staging.exists());
        assert_eq!(file.remote_path, "disk:/root/data.sqlite");

        report.cleanup_staging_files().unwrap();
        assert!(!staging.exists());
    }

    #[test]
    fn sqlite_backup_timeout_cleans_incomplete_staging() {
        let temp = tempfile::tempdir().unwrap();
        let sqlite_path = temp.path().join("locked.sqlite");
        create_sqlite_db(&sqlite_path);
        let lock = Connection::open(&sqlite_path).unwrap();
        lock.execute_batch("BEGIN EXCLUSIVE; INSERT INTO items(name) VALUES ('locked');")
            .unwrap();

        let result = backup_sqlite_to_staging_with_timeouts(
            &sqlite_path,
            &temp.path().join("staging"),
            "locked-root",
            Duration::ZERO,
            Duration::from_secs(1),
        );

        assert!(matches!(
            result,
            Err(ScannerError::SqliteBackupTimedOut { .. }) | Err(ScannerError::SqliteBackup { .. })
        ));
        let staging_files: Vec<_> = fs::read_dir(temp.path().join("staging"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(staging_files.is_empty());
        drop(lock);
    }

    #[test]
    fn sqlite_backup_timeout_is_skippable_scan_error() {
        let temp = tempfile::tempdir().unwrap();
        let error = ScannerError::SqliteBackupTimedOut {
            path: temp.path().join("busy.sqlite3"),
            elapsed_seconds: 120,
        };

        assert!(is_skippable_scan_error(&error));
    }

    #[test]
    fn invalid_sqlite_candidate_is_failed_not_scanned_as_regular_file() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path().join("broken.db"), "not sqlite");

        let report = test_scan(temp.path(), 1, 1024, 1024);

        assert!(report.files.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert!(report.failed[0].error_message.contains("SQLite"));
    }

    #[test]
    fn default_wal_shm_excludes_work() {
        let temp = tempfile::tempdir().unwrap();
        write_file(temp.path().join("data.sqlite-wal"), "wal");
        write_file(temp.path().join("data.db-shm"), "shm");
        write_file(temp.path().join("data.sqlite"), "not sqlite");

        let mut options = test_options(temp.path(), 1, 1024, 1024);
        options.global_excludes = default_config().global_excludes;
        let scanner = Scanner::new(options);
        let report = scanner.scan_root(&test_root(temp.path()), &[]).unwrap();

        assert!(report
            .skipped
            .iter()
            .any(|item| item.relative_path == "data.sqlite-wal"));
        assert!(report
            .skipped
            .iter()
            .any(|item| item.relative_path == "data.db-shm"));
    }

    #[test]
    fn record_scan_findings_writes_skipped_and_failed_only() {
        let temp = tempfile::tempdir().unwrap();
        let repository = StateRepository::open_in_memory_for_tests().unwrap();
        let mut report = ScanReport::new("root");
        report.skipped.push(SkippedScanItem {
            root_id: "root".to_string(),
            local_path: temp.path().join("big.bin"),
            relative_path: "big.bin".to_string(),
            remote_path: Some("disk:/root/big.bin".to_string()),
            reason_code: SKIP_REASON_FILE_TOO_LARGE.to_string(),
            reason_details: None,
            size_bytes: Some(42),
        });
        report.failed.push(FailedScanItem {
            root_id: "root".to_string(),
            local_path: Some(temp.path().join("broken.db")),
            relative_path: "broken.db".to_string(),
            remote_path: Some("disk:/root/broken.db".to_string()),
            error_kind: "scan".to_string(),
            error_message: "broken".to_string(),
        });

        record_scan_findings(&repository, &report).unwrap();

        assert!(repository
            .get_file_state("root", "big.bin")
            .unwrap()
            .is_none());
        let paths = repository.list_known_remote_paths("root").unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn fixture_generator_creates_files_and_rejects_non_empty_output() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("fixtures");

        generate_fixture_tree(&output, 7, 3).unwrap();
        let count = WalkBuilder::new(&output)
            .standard_filters(false)
            .build()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_type()
                    .is_some_and(|file_type| file_type.is_file())
            })
            .count();

        assert_eq!(count, 7);
        assert!(matches!(
            generate_fixture_tree(&output, 1, 1),
            Err(ScannerError::FixtureOutputNotEmpty(_))
        ));
    }

    #[test]
    #[ignore = "requires YDS_STRESS_TESTS=1; creates a large synthetic tree"]
    fn stress_fixture_tree_scans_large_inventory_when_gated() {
        if std::env::var("YDS_STRESS_TESTS").ok().as_deref() != Some("1") {
            return;
        }

        let files = std::env::var("YDS_STRESS_FILES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(200_000);
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("stress-tree");
        generate_fixture_tree(&output, files, 30).unwrap();

        let options = ScanOptions {
            scan_concurrency: 0,
            staging_dir: temp.path().join("staging"),
            ..test_options(&output, 0, 1024, u64::MAX)
        };
        let scanner = Scanner::new(options);
        let report = scanner.scan_root(&test_root(&output), &[]).unwrap();

        assert_eq!(report.files.len(), files);
    }

    fn test_scan(
        root_path: &Path,
        concurrency: usize,
        full_hash_max_bytes: u64,
        max_file_size_bytes: u64,
    ) -> ScanReport {
        let scanner = Scanner::new(test_options(
            root_path,
            concurrency,
            full_hash_max_bytes,
            max_file_size_bytes,
        ));
        scanner.scan_root(&test_root(root_path), &[]).unwrap()
    }

    fn test_options(
        root_path: &Path,
        concurrency: usize,
        full_hash_max_bytes: u64,
        max_file_size_bytes: u64,
    ) -> ScanOptions {
        ScanOptions {
            global_excludes: Vec::new(),
            absolute_excludes: Vec::new(),
            max_file_size_bytes,
            full_hash_max_bytes,
            large_file_name_size_only_min_bytes: full_hash_max_bytes * 2,
            scan_concurrency: concurrency,
            staging_dir: root_path.join(".staging"),
        }
    }

    fn test_root(root_path: &Path) -> ScanRoot {
        ScanRoot::new("root", root_path, "disk:/root", Vec::new())
    }

    fn write_file(path: impl AsRef<Path>, content: &str) {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    fn create_sqlite_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO items(name) VALUES ('one')", [])
            .unwrap();
    }

    fn state_record_from_entry(entry: &LocalFileEntry) -> FileStateRecord {
        FileStateRecord {
            root_id: entry.root_id.clone(),
            local_path: path_to_stable_string(&entry.local_path),
            relative_path: entry.relative_path.clone(),
            remote_path: entry.remote_path.clone(),
            normalized_remote_path: entry.normalized_remote_path.clone(),
            size_bytes: entry.size_bytes as i64,
            mtime_ns: entry.mtime_ns,
            ctime_ns: entry.ctime_ns,
            fingerprint_kind: entry.fingerprint.kind.as_str().to_string(),
            fingerprint_value: Some(entry.fingerprint.value.clone()),
            full_sha256: entry.fingerprint.full_sha256.clone(),
            remote_etag: None,
            remote_md5: entry.fingerprint.local_md5.clone(),
            last_run_id: Some(1),
            status: FileStateStatus::Synced,
            last_error: None,
        }
    }
}
