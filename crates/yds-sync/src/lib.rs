pub mod migration;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;
use yds_core::{
    config::{validate_config, AppConfig, SyncRootConfig},
    path_mapping::{canonical_remote_path, PathMappingError},
    ComponentStatus,
};
use yds_scanner::{
    record_scan_findings, LocalDirectoryEntry, LocalFileEntry, ScanOptions, ScanReport, ScanRoot,
    Scanner, ScannerError,
};
use yds_state::{
    models::{
        DirectoryStateRecord, DirectoryStateStatus, FailedItemRecord, FileStateRecord,
        FileStateStatus, OperationRecord, OperationStatus as StateOperationStatus,
        RemoteResourceRecord, RemoteResourceType, RootRemoteInventoryStatus, SkippedItemRecord,
        SyncRunStatus, SyncRunSummary, SyncRunTrigger,
    },
    StateError, StateRepository,
};
use yds_yandex_disk::{
    error::{YandexDiskError, YandexDiskErrorKind},
    models::{ListRecursiveOptions, ResourceMetadata, ResourceType},
    UploadSource, YandexDiskClient,
};

pub const COMPONENT_NAME: &str = "sync";
const DEFAULT_STALE_LOCK_HOURS: i64 = 6;
const ROOT_STATUS_SUCCEEDED: &str = "succeeded";
const ROOT_STATUS_SKIPPED: &str = "skipped";
const ROOT_STATUS_PARTIAL_FAILED: &str = "partial_failed";
const ROOT_STATUS_FAILED: &str = "failed";
const ROOT_STATUS_CANCELLED: &str = "cancelled";

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("config validation failed: {0}")]
    ConfigInvalid(String),
    #[error("sync run is already running")]
    AlreadyRunning,
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("path mapping error for root {root_id}: {source}")]
    PathMapping {
        root_id: String,
        #[source]
        source: PathMappingError,
    },
    #[error("scanner error: {0}")]
    Scanner(#[from] ScannerError),
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("Yandex Disk error: {0}")]
    YandexDisk(#[from] YandexDiskError),
    #[error("migration error: {0}")]
    Migration(#[from] migration::MigrationError),
    #[error("upload task failed to join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct SyncRunOptions {
    pub trigger: SyncRunTrigger,
    pub lock_owner: String,
    pub stale_lock_after: Duration,
    pub force_remote_rescan: bool,
}

impl Default for SyncRunOptions {
    fn default() -> Self {
        Self {
            trigger: SyncRunTrigger::Cli,
            lock_owner: format!("yds-sync-{}", Uuid::new_v4()),
            stale_lock_after: Duration::hours(DEFAULT_STALE_LOCK_HOURS),
            force_remote_rescan: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPlan {
    pub operations: Vec<SyncPlanOperation>,
}

impl SyncPlan {
    #[must_use]
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    #[must_use]
    pub fn create_directory_count(&self) -> usize {
        self.operations
            .iter()
            .filter(|operation| matches!(operation, SyncPlanOperation::CreateDirectory { .. }))
            .count()
    }

    #[must_use]
    pub fn upload_count(&self) -> usize {
        self.operations
            .iter()
            .filter(|operation| matches!(operation, SyncPlanOperation::UploadFile { .. }))
            .count()
    }

    #[must_use]
    pub fn delete_count(&self) -> usize {
        self.operations
            .iter()
            .filter(|operation| matches!(operation, SyncPlanOperation::DeleteRemote { .. }))
            .count()
    }
}

impl Default for SyncPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPlanOperation {
    CreateDirectory {
        directory: LocalDirectoryEntry,
    },
    UploadFile {
        file: LocalFileEntry,
        is_update: bool,
    },
    DeleteRemote {
        root_id: String,
        remote_path: String,
        relative_path: String,
        resource_type: ResourceType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedLocalSubtree {
    pub root_id: String,
    pub relative_prefix: String,
    pub remote_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveScanPlan {
    pub root_id: String,
    pub root_local_path: PathBuf,
    pub files: Vec<LocalFileEntry>,
    pub directories: Vec<LocalDirectoryEntry>,
    pub deleted_subtrees: Vec<DeletedLocalSubtree>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootSyncSummary {
    pub root_id: String,
    pub local_path: String,
    pub canonical_remote_path: String,
    pub status: String,
    pub scanned_files: i64,
    pub uploaded_files: i64,
    pub updated_files: i64,
    pub deleted_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
    pub bytes_uploaded: i64,
    pub error_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRunReport {
    pub run_id: i64,
    pub status: SyncRunStatus,
    pub summary: SyncRunSummary,
    pub roots: Vec<RootSyncSummary>,
    pub plan: SyncPlan,
}

impl SyncRunReport {
    #[must_use]
    pub fn is_successful(&self) -> bool {
        self.status == SyncRunStatus::Succeeded
    }
}

impl LiveScanPlan {
    #[must_use]
    pub fn from_scan_report(root_local_path: impl Into<PathBuf>, report: &ScanReport) -> Self {
        let root_local_path = root_local_path.into();
        let mut plan = Self {
            root_id: report.root_id.clone(),
            root_local_path,
            files: Vec::new(),
            directories: Vec::new(),
            deleted_subtrees: Vec::new(),
        };

        let mut directories = report.directories.clone();
        directories.sort_by_key(|directory| {
            (
                directory.relative_path.matches('/').count(),
                directory.relative_path.clone(),
            )
        });
        for directory in directories {
            if plan.is_deleted_relative_path(&directory.relative_path) {
                continue;
            }
            if !directory.local_path.is_dir() {
                plan.add_deleted_subtree(DeletedLocalSubtree {
                    root_id: directory.root_id.clone(),
                    relative_prefix: directory.relative_path.clone(),
                    remote_prefix: directory.remote_path.clone(),
                });
                continue;
            }
            plan.directories.push(directory);
        }

        for file in &report.files {
            if !plan.is_deleted_relative_path(&file.relative_path) {
                plan.files.push(file.clone());
            }
        }

        plan
    }

    #[must_use]
    pub fn prune_remote_prefixes(&self) -> Vec<String> {
        self.deleted_subtrees
            .iter()
            .map(|subtree| subtree.remote_prefix.clone())
            .collect()
    }

    #[must_use]
    pub fn is_deleted_relative_path(&self, relative_path: &str) -> bool {
        self.deleted_subtrees.iter().any(|subtree| {
            relative_path == subtree.relative_prefix
                || relative_path
                    .strip_prefix(subtree.relative_prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    }

    pub fn register_missing_local_path(
        &mut self,
        local_path: &Path,
        relative_path: &str,
        remote_path: &str,
    ) -> Option<DeletedLocalSubtree> {
        let subtree = detect_deleted_local_subtree(
            &self.root_id,
            &self.root_local_path,
            local_path,
            relative_path,
            remote_path,
        )?;
        if self.add_deleted_subtree(subtree.clone()) {
            Some(subtree)
        } else {
            None
        }
    }

    fn add_deleted_subtree(&mut self, subtree: DeletedLocalSubtree) -> bool {
        if self.is_deleted_relative_path(&subtree.relative_prefix) {
            return false;
        }
        self.deleted_subtrees.retain(|existing| {
            !(existing.relative_prefix == subtree.relative_prefix
                || existing
                    .relative_prefix
                    .strip_prefix(subtree.relative_prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/')))
        });
        self.deleted_subtrees.push(subtree);
        self.deleted_subtrees
            .sort_by(|left, right| left.relative_prefix.cmp(&right.relative_prefix));
        true
    }
}

pub struct SyncEngine<'a> {
    config: &'a AppConfig,
    repository: &'a StateRepository,
    client: Arc<dyn YandexDiskClient>,
}

impl<'a> SyncEngine<'a> {
    #[must_use]
    pub fn new(
        config: &'a AppConfig,
        repository: &'a StateRepository,
        client: Arc<dyn YandexDiskClient>,
    ) -> Self {
        Self {
            config,
            repository,
            client,
        }
    }

    pub async fn run_once(
        &self,
        options: SyncRunOptions,
        cancellation: &CancellationToken,
    ) -> Result<SyncRunReport, SyncError> {
        ensure_valid_config(self.config)?;

        if options.trigger != SyncRunTrigger::Migration && self.should_auto_migrate()? {
            let migration_report = migration::MigrationEngine::new(
                self.config,
                self.repository,
                Arc::clone(&self.client),
            )
            .run_once(
                migration::MigrationRunOptions {
                    lock_owner: options.lock_owner,
                    stale_lock_after: options.stale_lock_after,
                    force_remote_rescan: options.force_remote_rescan,
                },
                cancellation,
            )
            .await?;
            return Ok(sync_report_from_migration(migration_report));
        }

        let guard = self
            .repository
            .try_acquire_run_lock(
                &options.lock_owner,
                OffsetDateTime::now_utc(),
                options.stale_lock_after,
            )?
            .ok_or(SyncError::AlreadyRunning)?;

        self.repository.repair_interrupted_runs()?;
        let run_id = self.repository.begin_run(options.trigger)?;
        let mut report = SyncRunReport {
            run_id,
            status: SyncRunStatus::Running,
            summary: SyncRunSummary::default(),
            roots: Vec::new(),
            plan: SyncPlan::new(),
        };

        let execution = self
            .execute_run(
                run_id,
                cancellation,
                &mut report,
                options.force_remote_rescan || self.config.sync.force_remote_rescan,
            )
            .await;
        if let Err(error) = execution {
            report.summary.failed_files += 1;
            report.summary.error_summary = Some(error.to_string());
            report.status = SyncRunStatus::Failed;
        } else if cancellation.is_cancelled() {
            report.status = SyncRunStatus::Cancelled;
            if report.summary.error_summary.is_none() {
                report.summary.error_summary = Some("sync run cancelled".to_string());
            }
        } else {
            report.status = status_from_summary(&report.summary, &report.roots);
        }

        let finish_result = self
            .repository
            .finish_run(run_id, report.status, &report.summary);
        let release_result = self.repository.release_run_lock(&guard);
        finish_result?;
        release_result?;

        Ok(report)
    }

    async fn execute_run(
        &self,
        run_id: i64,
        cancellation: &CancellationToken,
        report: &mut SyncRunReport,
        force_remote_rescan: bool,
    ) -> Result<(), SyncError> {
        let mut available_remote_bytes = self.available_remote_bytes(report).await?;
        let scanner = Scanner::new(ScanOptions::from_config(
            &self.config.sync,
            &self.config.global_excludes,
            &self.config.absolute_excludes,
            PathBuf::from(&self.config.paths.staging_dir),
        ));

        for root in self.config.roots.iter().filter(|root| root.enabled) {
            if cancellation.is_cancelled() {
                report.summary.error_summary = Some("sync run cancelled".to_string());
                break;
            }

            let mut root_summary = self.initial_root_summary(root)?;
            self.repository.upsert_sync_root(
                &root.id,
                &root.local_path,
                &self.config.instance.remote_root,
                &root_summary.canonical_remote_path,
                root.enabled,
            )?;

            if !Path::new(&root.local_path).is_dir() {
                self.record_missing_root(root, &mut root_summary)?;
                add_root_summary(&mut report.summary, &root_summary);
                report.roots.push(root_summary);
                continue;
            }

            let root_result = self
                .execute_root(
                    run_id,
                    root,
                    &scanner,
                    cancellation,
                    available_remote_bytes,
                    report,
                    &mut root_summary,
                    force_remote_rescan,
                )
                .await?;

            if let Some(consumed) = root_result.bytes_uploaded {
                if let Some(available) = &mut available_remote_bytes {
                    *available = available.saturating_sub(consumed);
                }
            }

            add_root_summary(&mut report.summary, &root_summary);
            let should_stop = root_result.should_stop;
            report.roots.push(root_summary);
            if should_stop || cancellation.is_cancelled() {
                break;
            }
        }

        Ok(())
    }

    fn should_auto_migrate(&self) -> Result<bool, SyncError> {
        for root in self.config.roots.iter().filter(|root| root.enabled) {
            if Path::new(&root.local_path).is_dir()
                && !self.repository.has_snapshot_for_root(&root.id)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn available_remote_bytes(
        &self,
        report: &mut SyncRunReport,
    ) -> Result<Option<u64>, SyncError> {
        match self.client.disk_info().await {
            Ok(info) => Ok(info
                .quota
                .total_space
                .zip(info.quota.used_space)
                .map(|(total, used)| total.saturating_sub(used))),
            Err(error) => {
                report.summary.error_summary = Some(error.to_string());
                Ok(None)
            }
        }
    }

    fn initial_root_summary(&self, root: &SyncRootConfig) -> Result<RootSyncSummary, SyncError> {
        let canonical_remote_path = canonical_remote_path(
            &self.config.instance.remote_root,
            &root.local_path,
            root.remote_path_override.as_deref(),
        )
        .map_err(|source| SyncError::PathMapping {
            root_id: root.id.clone(),
            source,
        })?;

        Ok(RootSyncSummary {
            root_id: root.id.clone(),
            local_path: root.local_path.clone(),
            canonical_remote_path,
            status: ROOT_STATUS_SUCCEEDED.to_string(),
            ..RootSyncSummary::default()
        })
    }

    fn record_missing_root(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut RootSyncSummary,
    ) -> Result<(), SyncError> {
        let message = format!(
            "local root is missing or not a directory: {}",
            root.local_path
        );
        warn!(root_id = root.id, local_path = root.local_path, "{message}");
        self.repository.mark_skipped(&SkippedItemRecord {
            root_id: root.id.clone(),
            local_path: root.local_path.clone(),
            relative_path: None,
            remote_path: Some(root_summary.canonical_remote_path.clone()),
            reason_code: "missing_root".to_string(),
            reason_details: Some(message.clone()),
            size_bytes: None,
        })?;
        self.repository
            .update_sync_root_status(&root.id, ROOT_STATUS_SKIPPED, Some(&message))?;
        root_summary.status = ROOT_STATUS_SKIPPED.to_string();
        root_summary.skipped_files = 1;
        root_summary.error_summary = Some(message);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_root(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        scanner: &Scanner,
        cancellation: &CancellationToken,
        available_remote_bytes: Option<u64>,
        report: &mut SyncRunReport,
        root_summary: &mut RootSyncSummary,
        force_remote_rescan: bool,
    ) -> Result<RootExecutionResult, SyncError> {
        let base_summary = report.summary.clone();
        let scan_root = ScanRoot::new(
            root.id.clone(),
            PathBuf::from(&root.local_path),
            root_summary.canonical_remote_path.clone(),
            root.excludes.clone(),
        );
        let scan_report = match scanner.scan_root_with_repository(&scan_root, self.repository) {
            Ok(scan_report) => scan_report,
            Err(error) => {
                let message = error.to_string();
                self.fail_root(root, root_summary, "scan", "scanner", &message)?;
                return Ok(RootExecutionResult::stop(false));
            }
        };
        record_scan_findings(self.repository, &scan_report)?;
        let mut live_plan =
            LiveScanPlan::from_scan_report(PathBuf::from(&root.local_path), &scan_report);

        root_summary.scanned_files = live_plan.files.len() as i64;
        root_summary.skipped_files = scan_report.skipped.len() as i64;
        root_summary.failed_files = scan_report.failed.len() as i64;
        self.record_deleted_subtrees(run_id, &mut live_plan, root_summary)?;
        self.persist_root_progress(run_id, &base_summary, root_summary)?;

        if let Some(message) = local_remote_collision(&live_plan.directories, &live_plan.files) {
            self.fail_root(root, root_summary, "plan", "path_collision", &message)?;
            return Ok(RootExecutionResult::stop(false));
        }

        let remote_snapshot = match self
            .remote_inventory(
                root,
                &root_summary.canonical_remote_path,
                &live_plan,
                force_remote_rescan,
            )
            .await
        {
            Ok(remote_inventory) => remote_inventory,
            Err(error) if is_stop_error(error.kind()) => {
                self.repository
                    .set_root_remote_inventory_status(&RootRemoteInventoryStatus {
                        root_id: root.id.clone(),
                        is_complete: false,
                        snapshot_completed_at_utc: None,
                        snapshot_expires_at_utc: None,
                        resource_count: 0,
                        last_error: Some(error.message().to_string()),
                    })?;
                self.fail_root(
                    root,
                    root_summary,
                    "remote_inventory",
                    error.kind().as_str(),
                    error.message(),
                )?;
                return Ok(RootExecutionResult::stop(true));
            }
            Err(error) => {
                self.repository
                    .set_root_remote_inventory_status(&RootRemoteInventoryStatus {
                        root_id: root.id.clone(),
                        is_complete: false,
                        snapshot_completed_at_utc: None,
                        snapshot_expires_at_utc: None,
                        resource_count: 0,
                        last_error: Some(error.message().to_string()),
                    })?;
                self.partial_fail_root(
                    root,
                    root_summary,
                    "remote_inventory",
                    error.kind().as_str(),
                    error.message(),
                )?;
                return Ok(RootExecutionResult::stop(false));
            }
        };
        let remote_inventory = remote_snapshot.resources;

        let root_plan = build_root_plan(
            root,
            &root_summary.canonical_remote_path,
            &live_plan,
            &remote_inventory,
        );
        let upload_bytes = root_plan.upload_bytes();
        if let Some(available) = available_remote_bytes {
            if upload_bytes > available {
                let message = format!(
                    "planned upload size {upload_bytes} bytes exceeds available Yandex Disk quota {available} bytes"
                );
                self.fail_root(
                    root,
                    root_summary,
                    "quota_check",
                    YandexDiskErrorKind::QuotaExceeded.as_str(),
                    &message,
                )?;
                report.summary.error_summary = Some(message);
                return Ok(RootExecutionResult::stop(true));
            }
        }

        report
            .plan
            .operations
            .extend(root_plan.operations.iter().cloned());

        let stop_after_create = self
            .apply_create_directories(
                run_id,
                root,
                &root_plan,
                &mut live_plan,
                cancellation,
                root_summary,
                &base_summary,
            )
            .await?;
        if stop_after_create || cancellation.is_cancelled() {
            scan_report.cleanup_staging_files()?;
            self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
            return Ok(RootExecutionResult::stop(true));
        }

        let upload_result = self
            .apply_uploads(
                run_id,
                &root_plan,
                &mut live_plan,
                cancellation,
                root_summary,
                &base_summary,
            )
            .await?;
        scan_report.cleanup_staging_files()?;
        if upload_result.should_stop || cancellation.is_cancelled() {
            self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
            return Ok(RootExecutionResult {
                should_stop: true,
                bytes_uploaded: Some(upload_result.bytes_uploaded),
            });
        }

        let stop_after_delete = self
            .apply_deletes(
                run_id,
                root,
                &live_plan,
                &remote_inventory,
                remote_snapshot.from_cache,
                cancellation,
                root_summary,
                &base_summary,
            )
            .await?;
        self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
        self.persist_root_progress(run_id, &base_summary, root_summary)?;

        Ok(RootExecutionResult {
            should_stop: stop_after_delete,
            bytes_uploaded: Some(upload_result.bytes_uploaded),
        })
    }

    async fn remote_inventory(
        &self,
        root: &SyncRootConfig,
        canonical_remote_path: &str,
        live_plan: &LiveScanPlan,
        force_remote_rescan: bool,
    ) -> Result<RemoteInventorySnapshot, YandexDiskError> {
        if !force_remote_rescan {
            if let Some(status) = self
                .repository
                .get_root_remote_inventory_status(&root.id)
                .map_err(|error| YandexDiskError::critical(error.to_string()))?
            {
                if status.is_complete && !is_remote_inventory_expired(&status) {
                    let cached = self
                        .repository
                        .list_remote_resources_for_root(&root.id)
                        .map_err(|error| YandexDiskError::critical(error.to_string()))?;
                    return Ok(RemoteInventorySnapshot {
                        resources: cached
                            .into_iter()
                            .filter_map(|record| {
                                cached_resource_to_metadata(canonical_remote_path, &record)
                            })
                            .map(|resource| (resource.path.clone(), resource))
                            .collect(),
                        from_cache: true,
                    });
                }
            }
        }

        let mut resources = BTreeMap::new();
        match self.client.metadata(canonical_remote_path).await {
            Ok(root) => {
                resources.insert(root.path.clone(), root);
            }
            Err(error) if error.kind() == YandexDiskErrorKind::NotFound => {
                return Ok(RemoteInventorySnapshot {
                    resources,
                    from_cache: false,
                });
            }
            Err(error) => return Err(error),
        }

        match self
            .client
            .list_recursive_with_options(
                canonical_remote_path,
                ListRecursiveOptions {
                    prune_remote_prefixes: live_plan.prune_remote_prefixes(),
                },
            )
            .await
        {
            Ok(listed) => {
                for resource in listed {
                    resources.insert(resource.path.clone(), resource);
                }
                self.persist_remote_inventory_snapshot(
                    &root.id,
                    resources.values().cloned().collect::<Vec<_>>(),
                )
                .map_err(|error| YandexDiskError::critical(error.to_string()))?;
                Ok(RemoteInventorySnapshot {
                    resources,
                    from_cache: false,
                })
            }
            Err(error) if error.kind() == YandexDiskErrorKind::NotFound => {
                Ok(RemoteInventorySnapshot {
                    resources,
                    from_cache: false,
                })
            }
            Err(error) => Err(error),
        }
    }

    fn fail_root(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut RootSyncSummary,
        operation: &str,
        error_kind: &str,
        error_message: &str,
    ) -> Result<(), SyncError> {
        self.repository.mark_failed(&FailedItemRecord {
            root_id: root.id.clone(),
            local_path: Some(root.local_path.clone()),
            remote_path: Some(root_summary.canonical_remote_path.clone()),
            operation: operation.to_string(),
            error_kind: error_kind.to_string(),
            error_message: error_message.to_string(),
            retry_count: 0,
            next_retry_at_utc: None,
        })?;
        self.repository.update_sync_root_status(
            &root.id,
            ROOT_STATUS_FAILED,
            Some(error_message),
        )?;
        root_summary.status = ROOT_STATUS_FAILED.to_string();
        root_summary.failed_files += 1;
        root_summary.error_summary = Some(error_message.to_string());
        Ok(())
    }

    fn partial_fail_root(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut RootSyncSummary,
        operation: &str,
        error_kind: &str,
        error_message: &str,
    ) -> Result<(), SyncError> {
        self.repository.mark_failed(&FailedItemRecord {
            root_id: root.id.clone(),
            local_path: Some(root.local_path.clone()),
            remote_path: Some(root_summary.canonical_remote_path.clone()),
            operation: operation.to_string(),
            error_kind: error_kind.to_string(),
            error_message: error_message.to_string(),
            retry_count: 0,
            next_retry_at_utc: None,
        })?;
        self.repository.update_sync_root_status(
            &root.id,
            ROOT_STATUS_PARTIAL_FAILED,
            Some(error_message),
        )?;
        root_summary.status = ROOT_STATUS_PARTIAL_FAILED.to_string();
        root_summary.failed_files += 1;
        root_summary.error_summary = Some(error_message.to_string());
        Ok(())
    }

    fn persist_root_progress(
        &self,
        run_id: i64,
        base_summary: &SyncRunSummary,
        root_summary: &RootSyncSummary,
    ) -> Result<(), SyncError> {
        let mut summary = base_summary.clone();
        add_root_summary(&mut summary, root_summary);
        self.repository.update_run_progress(run_id, &summary)?;
        Ok(())
    }

    fn record_deleted_subtrees(
        &self,
        run_id: i64,
        live_plan: &mut LiveScanPlan,
        root_summary: &mut RootSyncSummary,
    ) -> Result<(), SyncError> {
        let subtrees = live_plan.deleted_subtrees.clone();
        for subtree in subtrees {
            self.record_deleted_subtree(run_id, &subtree, root_summary)?;
        }
        Ok(())
    }

    fn record_deleted_subtree(
        &self,
        run_id: i64,
        subtree: &DeletedLocalSubtree,
        root_summary: &mut RootSyncSummary,
    ) -> Result<(), SyncError> {
        self.repository.mark_subtree_deleted(
            &subtree.root_id,
            &subtree.relative_prefix,
            &subtree.remote_prefix,
            run_id,
        )?;
        self.repository.mark_skipped(&SkippedItemRecord {
            root_id: subtree.root_id.clone(),
            local_path: subtree.relative_prefix.clone(),
            relative_path: Some(subtree.relative_prefix.clone()),
            remote_path: Some(subtree.remote_prefix.clone()),
            reason_code: "local_subtree_deleted".to_string(),
            reason_details: Some(
                "local subtree deleted during sync preparation or execution".to_string(),
            ),
            size_bytes: None,
        })?;
        self.repository
            .append_operations_succeeded(&[OperationRecord {
                run_id,
                operation: "skipped_local_subtree_deleted".to_string(),
                local_path: Some(subtree.relative_prefix.clone()),
                remote_path: Some(subtree.remote_prefix.clone()),
            }])?;
        root_summary.skipped_files += 1;
        Ok(())
    }

    fn persist_remote_inventory_snapshot(
        &self,
        root_id: &str,
        resources: Vec<ResourceMetadata>,
    ) -> Result<(), SyncError> {
        let records = resources
            .iter()
            .map(|resource| remote_resource_record(root_id, resource))
            .collect::<Vec<_>>();
        self.repository
            .replace_remote_resources_for_root(root_id, &records)?;
        let now = OffsetDateTime::now_utc();
        let expires_at =
            now + Duration::hours(self.config.sync.remote_inventory_cache_ttl_hours as i64);
        self.repository
            .set_root_remote_inventory_status(&RootRemoteInventoryStatus {
                root_id: root_id.to_string(),
                is_complete: true,
                snapshot_completed_at_utc: Some(format_timestamp(now)),
                snapshot_expires_at_utc: Some(format_timestamp(expires_at)),
                resource_count: i64::try_from(records.len()).unwrap_or(i64::MAX),
                last_error: None,
            })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_create_directories(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        root_plan: &RootPlan,
        live_plan: &mut LiveScanPlan,
        cancellation: &CancellationToken,
        root_summary: &mut RootSyncSummary,
        base_summary: &SyncRunSummary,
    ) -> Result<bool, SyncError> {
        let concurrency = self.config.sync.create_directory_concurrency.max(1);
        let mut by_depth = BTreeMap::<usize, Vec<LocalDirectoryEntry>>::new();
        for directory in root_plan.create_directories().cloned() {
            if live_plan.is_deleted_relative_path(&directory.relative_path) {
                continue;
            }
            by_depth
                .entry(relative_depth(&directory.relative_path))
                .or_default()
                .push(directory);
        }

        for directories in by_depth.into_values() {
            let mut pending: VecDeque<_> = directories.into_iter().collect();
            while !pending.is_empty() {
                if cancellation.is_cancelled() {
                    return Ok(true);
                }

                let mut batch = Vec::new();
                while batch.len() < concurrency {
                    let Some(directory) = pending.pop_front() else {
                        break;
                    };
                    if live_plan.is_deleted_relative_path(&directory.relative_path) {
                        continue;
                    }
                    if let Some(subtree) = live_plan.register_missing_local_path(
                        &directory.local_path,
                        &directory.relative_path,
                        &directory.remote_path,
                    ) {
                        self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                        self.persist_root_progress(run_id, base_summary, root_summary)?;
                        continue;
                    }
                    batch.push(directory);
                }
                if batch.is_empty() {
                    continue;
                }

                let mut tasks = JoinSet::new();
                for directory in batch {
                    let client = Arc::clone(&self.client);
                    let operation_id =
                        self.repository.append_operation_started(&OperationRecord {
                            run_id,
                            operation: "create_directory".to_string(),
                            local_path: Some(path_to_stable_string(&directory.local_path)),
                            remote_path: Some(directory.remote_path.clone()),
                        })?;
                    tasks.spawn(async move {
                        let result = client.create_directory(&directory.remote_path).await;
                        (directory, operation_id, result)
                    });
                }

                while let Some(outcome) = tasks.join_next().await {
                    let (directory, operation_id, result) = outcome?;
                    match result {
                        Ok(()) => {
                            self.repository.finish_operation(
                                operation_id,
                                StateOperationStatus::Succeeded,
                                None,
                                None,
                            )?;
                            self.repository
                                .upsert_directory_state(&DirectoryStateRecord {
                                    root_id: directory.root_id.clone(),
                                    local_path: path_to_stable_string(&directory.local_path),
                                    relative_path: directory.relative_path.clone(),
                                    remote_path: directory.remote_path.clone(),
                                    normalized_remote_path: directory
                                        .normalized_remote_path
                                        .clone(),
                                    last_run_id: Some(run_id),
                                    status: DirectoryStateStatus::Synced,
                                    last_error: None,
                                })?;
                            self.repository
                                .upsert_remote_resource(&RemoteResourceRecord {
                                    root_id: directory.root_id.clone(),
                                    remote_path: directory.remote_path.clone(),
                                    resource_type: RemoteResourceType::Dir,
                                    size_bytes: None,
                                    mtime_remote: None,
                                    remote_md5: None,
                                })?;
                        }
                        Err(error) => {
                            self.repository.finish_operation(
                                operation_id,
                                StateOperationStatus::Failed,
                                Some(error.kind().as_str()),
                                Some(error.message()),
                            )?;
                            self.repository.mark_failed(&FailedItemRecord {
                                root_id: root.id.clone(),
                                local_path: Some(path_to_stable_string(&directory.local_path)),
                                remote_path: Some(directory.remote_path.clone()),
                                operation: "create_directory".to_string(),
                                error_kind: error.kind().as_str().to_string(),
                                error_message: error.message().to_string(),
                                retry_count: 0,
                                next_retry_at_utc: None,
                            })?;
                            root_summary.failed_files += 1;
                            root_summary.error_summary = Some(error.message().to_string());
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            if is_stop_error(error.kind()) {
                                root_summary.status = ROOT_STATUS_FAILED.to_string();
                                root_summary.error_summary = Some(error.to_string());
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    async fn apply_uploads(
        &self,
        run_id: i64,
        root_plan: &RootPlan,
        live_plan: &mut LiveScanPlan,
        cancellation: &CancellationToken,
        root_summary: &mut RootSyncSummary,
        base_summary: &SyncRunSummary,
    ) -> Result<UploadPhaseResult, SyncError> {
        let mut bytes_uploaded = 0_u64;
        let mut pending: VecDeque<_> = root_plan.uploads().into();
        let upload_concurrency = self.config.sync.upload_concurrency.max(1);
        let large_threshold = self.config.sync.large_file_name_size_only_min_bytes;

        while !pending.is_empty() {
            if cancellation.is_cancelled() {
                return Ok(UploadPhaseResult {
                    should_stop: true,
                    bytes_uploaded,
                });
            }

            let batch = take_upload_batch(&mut pending, upload_concurrency, large_threshold);
            if batch.is_empty() {
                break;
            }
            let mut tasks = JoinSet::new();
            for (file, is_update) in batch {
                if live_plan.is_deleted_relative_path(&file.relative_path) {
                    continue;
                }
                if let Some(subtree) = live_plan.register_missing_local_path(
                    &file.local_path,
                    &file.relative_path,
                    &file.remote_path,
                ) {
                    self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                    continue;
                }
                let operation = if is_update {
                    "update_file"
                } else {
                    "upload_file"
                };
                let operation_id = self.repository.append_operation_started(&OperationRecord {
                    run_id,
                    operation: operation.to_string(),
                    local_path: Some(path_to_stable_string(&file.local_path)),
                    remote_path: Some(file.remote_path.clone()),
                })?;
                let client = Arc::clone(&self.client);
                let file = file.clone();
                let cancellation = cancellation.clone();
                tasks.spawn(async move {
                    upload_task(operation_id, client, file, is_update, cancellation).await
                });
            }

            while let Some(outcome) = tasks.join_next().await {
                let outcome = outcome??;
                match outcome.result {
                    Ok(()) => {
                        self.repository.finish_operation(
                            outcome.operation_id,
                            StateOperationStatus::Succeeded,
                            None,
                            None,
                        )?;
                        self.repository.upsert_file_state(&file_state_record(
                            &outcome.file,
                            run_id,
                            file_status_after_upload(&outcome.file),
                            None,
                        ))?;
                        self.repository
                            .upsert_remote_resource(&RemoteResourceRecord {
                                root_id: outcome.file.root_id.clone(),
                                remote_path: outcome.file.remote_path.clone(),
                                resource_type: RemoteResourceType::File,
                                size_bytes: Some(u64_to_i64(outcome.file.size_bytes)),
                                mtime_remote: None,
                                remote_md5: outcome.file.fingerprint.local_md5.clone(),
                            })?;
                        if outcome.is_update {
                            root_summary.updated_files += 1;
                        } else {
                            root_summary.uploaded_files += 1;
                        }
                        root_summary.bytes_uploaded += u64_to_i64(outcome.file.size_bytes);
                        bytes_uploaded = bytes_uploaded.saturating_add(outcome.file.size_bytes);
                        self.persist_root_progress(run_id, base_summary, root_summary)?;
                    }
                    Err(failure) => {
                        if let Some(subtree) = live_plan.register_missing_local_path(
                            &outcome.file.local_path,
                            &outcome.file.relative_path,
                            &outcome.file.remote_path,
                        ) {
                            self.repository.finish_operation(
                                outcome.operation_id,
                                StateOperationStatus::Cancelled,
                                Some("local_subtree_deleted"),
                                Some("local subtree deleted during sync run"),
                            )?;
                            self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            continue;
                        }
                        if let Some(reason_code) = failure.skip_reason_code.as_deref() {
                            self.repository.finish_operation(
                                outcome.operation_id,
                                StateOperationStatus::Cancelled,
                                Some(reason_code),
                                Some(&failure.error_message),
                            )?;
                            self.repository.mark_skipped(&SkippedItemRecord {
                                root_id: outcome.file.root_id.clone(),
                                local_path: path_to_stable_string(&outcome.file.local_path),
                                relative_path: Some(outcome.file.relative_path.clone()),
                                remote_path: Some(outcome.file.remote_path.clone()),
                                reason_code: reason_code.to_string(),
                                reason_details: Some(failure.error_message.clone()),
                                size_bytes: Some(u64_to_i64(outcome.file.size_bytes)),
                            })?;
                            self.repository.upsert_file_state(&file_state_record(
                                &outcome.file,
                                run_id,
                                FileStateStatus::Skipped,
                                Some(failure.error_message.clone()),
                            ))?;
                            root_summary.skipped_files += 1;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            continue;
                        }
                        self.repository.finish_operation(
                            outcome.operation_id,
                            StateOperationStatus::Failed,
                            Some(&failure.error_kind),
                            Some(&failure.error_message),
                        )?;
                        self.repository.mark_failed(&FailedItemRecord {
                            root_id: outcome.file.root_id.clone(),
                            local_path: Some(path_to_stable_string(&outcome.file.local_path)),
                            remote_path: Some(outcome.file.remote_path.clone()),
                            operation: if outcome.is_update {
                                "update_file".to_string()
                            } else {
                                "upload_file".to_string()
                            },
                            error_kind: failure.error_kind.clone(),
                            error_message: failure.error_message.clone(),
                            retry_count: 0,
                            next_retry_at_utc: None,
                        })?;
                        root_summary.failed_files += 1;
                        root_summary.error_summary = Some(failure.error_message.clone());
                        self.persist_root_progress(run_id, base_summary, root_summary)?;
                        if failure.should_stop {
                            root_summary.status = ROOT_STATUS_FAILED.to_string();
                            root_summary.error_summary = Some(failure.error_message);
                            return Ok(UploadPhaseResult {
                                should_stop: true,
                                bytes_uploaded,
                            });
                        }
                    }
                }
            }
        }

        Ok(UploadPhaseResult {
            should_stop: false,
            bytes_uploaded,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_deletes(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        live_plan: &LiveScanPlan,
        remote_inventory: &BTreeMap<String, ResourceMetadata>,
        inventory_from_cache: bool,
        cancellation: &CancellationToken,
        root_summary: &mut RootSyncSummary,
        base_summary: &SyncRunSummary,
    ) -> Result<bool, SyncError> {
        let deletes = collect_coalesced_delete_operations(
            root,
            &root_summary.canonical_remote_path,
            live_plan,
            remote_inventory,
        );
        if !deletes.is_empty() {
            info!(
                root_id = root.id,
                planned_delete_count = deletes.len(),
                "planned remote extra deletions"
            );
        }

        for delete in deletes {
            if cancellation.is_cancelled() {
                return Ok(true);
            }
            let operation_id = self.repository.append_operation_started(&OperationRecord {
                run_id,
                operation: "delete_remote".to_string(),
                local_path: None,
                remote_path: Some(delete.remote_path.clone()),
            })?;
            if inventory_from_cache {
                match self.client.metadata(&delete.remote_path).await {
                    Ok(_) => {}
                    Err(error) if error.kind() == YandexDiskErrorKind::NotFound => {
                        self.repository.finish_operation(
                            operation_id,
                            StateOperationStatus::Succeeded,
                            None,
                            None,
                        )?;
                        self.repository
                            .delete_remote_resource_subtree(&delete.root_id, &delete.remote_path)?;
                        continue;
                    }
                    Err(error) => {
                        self.repository.finish_operation(
                            operation_id,
                            StateOperationStatus::Failed,
                            Some(error.kind().as_str()),
                            Some(error.message()),
                        )?;
                        self.repository.mark_failed(&FailedItemRecord {
                            root_id: root.id.clone(),
                            local_path: None,
                            remote_path: Some(delete.remote_path.clone()),
                            operation: "delete_remote_metadata_check".to_string(),
                            error_kind: error.kind().as_str().to_string(),
                            error_message: error.message().to_string(),
                            retry_count: 0,
                            next_retry_at_utc: None,
                        })?;
                        root_summary.failed_files += 1;
                        root_summary.error_summary = Some(error.message().to_string());
                        self.persist_root_progress(run_id, base_summary, root_summary)?;
                        if is_stop_error(error.kind()) {
                            root_summary.status = ROOT_STATUS_FAILED.to_string();
                            root_summary.error_summary = Some(error.to_string());
                            return Ok(true);
                        }
                        continue;
                    }
                }
            }
            match self.client.delete_permanently(&delete.remote_path).await {
                Ok(()) => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Succeeded,
                        None,
                        None,
                    )?;
                    match delete.resource_type {
                        ResourceType::File => self.repository.mark_deleted(
                            &delete.root_id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                        ResourceType::Directory => self.repository.mark_directory_deleted(
                            &delete.root_id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                    }
                    self.repository
                        .delete_remote_resource_subtree(&delete.root_id, &delete.remote_path)?;
                    root_summary.deleted_files += 1;
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                }
                Err(error) if error.kind() == YandexDiskErrorKind::NotFound => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Succeeded,
                        None,
                        None,
                    )?;
                    match delete.resource_type {
                        ResourceType::File => self.repository.mark_deleted(
                            &delete.root_id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                        ResourceType::Directory => self.repository.mark_directory_deleted(
                            &delete.root_id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                    }
                    self.repository
                        .delete_remote_resource_subtree(&delete.root_id, &delete.remote_path)?;
                }
                Err(error) => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Failed,
                        Some(error.kind().as_str()),
                        Some(error.message()),
                    )?;
                    self.repository.mark_failed(&FailedItemRecord {
                        root_id: root.id.clone(),
                        local_path: None,
                        remote_path: Some(delete.remote_path.clone()),
                        operation: "delete_remote".to_string(),
                        error_kind: error.kind().as_str().to_string(),
                        error_message: error.message().to_string(),
                        retry_count: 0,
                        next_retry_at_utc: None,
                    })?;
                    root_summary.failed_files += 1;
                    root_summary.error_summary = Some(error.message().to_string());
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                    if is_stop_error(error.kind()) {
                        root_summary.status = ROOT_STATUS_FAILED.to_string();
                        root_summary.error_summary = Some(error.to_string());
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn finish_root_cancelled_or_failed(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut RootSyncSummary,
        cancellation: &CancellationToken,
    ) -> Result<(), SyncError> {
        if cancellation.is_cancelled() {
            root_summary.status = ROOT_STATUS_CANCELLED.to_string();
            root_summary.error_summary = Some("sync run cancelled".to_string());
            self.repository.update_sync_root_status(
                &root.id,
                ROOT_STATUS_CANCELLED,
                root_summary.error_summary.as_deref(),
            )?;
        } else if root_summary.status == ROOT_STATUS_FAILED {
            self.repository.update_sync_root_status(
                &root.id,
                ROOT_STATUS_FAILED,
                root_summary.error_summary.as_deref(),
            )?;
        } else if root_summary.failed_files > 0 {
            root_summary.status = ROOT_STATUS_PARTIAL_FAILED.to_string();
            self.repository.update_sync_root_status(
                &root.id,
                ROOT_STATUS_PARTIAL_FAILED,
                root_summary.error_summary.as_deref(),
            )?;
        } else {
            root_summary.status = ROOT_STATUS_SUCCEEDED.to_string();
            self.repository
                .update_sync_root_status(&root.id, ROOT_STATUS_SUCCEEDED, None)?;
        }
        Ok(())
    }
}

fn sync_report_from_migration(report: migration::MigrationRunReport) -> SyncRunReport {
    SyncRunReport {
        run_id: report.run_id,
        status: report.status,
        summary: report.summary,
        roots: report
            .roots
            .into_iter()
            .map(|root| RootSyncSummary {
                root_id: root.root_id,
                local_path: root.local_path,
                canonical_remote_path: root.canonical_remote_path,
                status: root.status,
                scanned_files: root.scanned_files,
                uploaded_files: root.uploaded_files,
                updated_files: root.updated_files,
                deleted_files: root.deleted_files,
                skipped_files: root.skipped_files,
                failed_files: root.failed_files,
                bytes_uploaded: root.bytes_uploaded,
                error_summary: root.error_summary,
            })
            .collect(),
        plan: SyncPlan::new(),
    }
}

#[derive(Debug, Clone)]
struct RootPlan {
    operations: Vec<SyncPlanOperation>,
}

impl RootPlan {
    fn create_directories(&self) -> impl Iterator<Item = &LocalDirectoryEntry> {
        self.operations
            .iter()
            .filter_map(|operation| match operation {
                SyncPlanOperation::CreateDirectory { directory } => Some(directory),
                _ => None,
            })
    }

    fn uploads(&self) -> Vec<(&LocalFileEntry, bool)> {
        self.operations
            .iter()
            .filter_map(|operation| match operation {
                SyncPlanOperation::UploadFile { file, is_update } => Some((file, *is_update)),
                _ => None,
            })
            .collect()
    }

    fn upload_bytes(&self) -> u64 {
        self.operations
            .iter()
            .filter_map(|operation| match operation {
                SyncPlanOperation::UploadFile { file, .. } => Some(file.size_bytes),
                _ => None,
            })
            .sum()
    }
}

#[derive(Debug, Clone)]
struct RemoteInventorySnapshot {
    resources: BTreeMap<String, ResourceMetadata>,
    from_cache: bool,
}

#[derive(Debug, Clone)]
struct RemoteDeleteOperation {
    root_id: String,
    remote_path: String,
    relative_path: String,
    resource_type: ResourceType,
}

#[derive(Debug, Clone, Copy)]
struct RootExecutionResult {
    should_stop: bool,
    bytes_uploaded: Option<u64>,
}

impl RootExecutionResult {
    const fn stop(should_stop: bool) -> Self {
        Self {
            should_stop,
            bytes_uploaded: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UploadPhaseResult {
    should_stop: bool,
    bytes_uploaded: u64,
}

#[derive(Debug)]
struct UploadTaskOutcome {
    operation_id: i64,
    file: LocalFileEntry,
    is_update: bool,
    result: Result<(), UploadTaskFailure>,
}

#[derive(Debug)]
struct UploadTaskFailure {
    error_kind: String,
    error_message: String,
    should_stop: bool,
    skip_reason_code: Option<String>,
}

async fn upload_task(
    operation_id: i64,
    client: Arc<dyn YandexDiskClient>,
    file: LocalFileEntry,
    is_update: bool,
    cancellation: CancellationToken,
) -> Result<UploadTaskOutcome, SyncError> {
    if cancellation.is_cancelled() {
        return Ok(UploadTaskOutcome {
            operation_id,
            file,
            is_update,
            result: Err(UploadTaskFailure {
                error_kind: "cancelled".to_string(),
                error_message: "sync run cancelled".to_string(),
                should_stop: true,
                skip_reason_code: None,
            }),
        });
    }

    let result = client
        .upload_source(
            &file.remote_path,
            UploadSource::File(file.upload_source.clone()),
            true,
        )
        .await
        .map_err(upload_failure);

    Ok(UploadTaskOutcome {
        operation_id,
        file,
        is_update,
        result,
    })
}

fn upload_failure(error: YandexDiskError) -> UploadTaskFailure {
    let skip_reason_code = if error.is_persistent_uploader_conflict() {
        Some("remote_uploader_content_conflict".to_string())
    } else if error.is_local_upload_source_unavailable() {
        Some("local_file_unavailable".to_string())
    } else {
        None
    };
    UploadTaskFailure {
        error_kind: error.kind().as_str().to_string(),
        error_message: error.message().to_string(),
        should_stop: is_stop_error(error.kind()),
        skip_reason_code,
    }
}

fn build_root_plan(
    root: &SyncRootConfig,
    canonical_remote_path: &str,
    live_plan: &LiveScanPlan,
    remote_inventory: &BTreeMap<String, ResourceMetadata>,
) -> RootPlan {
    let mut operations = Vec::new();

    let root_directory = LocalDirectoryEntry {
        root_id: root.id.clone(),
        local_path: PathBuf::from(&root.local_path),
        relative_path: String::new(),
        remote_path: canonical_remote_path.to_string(),
        normalized_remote_path: canonical_remote_path.to_lowercase(),
    };
    if !matches!(
        remote_inventory
            .get(canonical_remote_path)
            .map(|resource| &resource.resource_type),
        Some(ResourceType::Directory)
    ) {
        operations.push(SyncPlanOperation::CreateDirectory {
            directory: root_directory,
        });
    }

    for directory in &live_plan.directories {
        if !matches!(
            remote_inventory
                .get(&directory.remote_path)
                .map(|resource| &resource.resource_type),
            Some(ResourceType::Directory)
        ) {
            operations.push(SyncPlanOperation::CreateDirectory {
                directory: directory.clone(),
            });
        }
    }

    for file in &live_plan.files {
        let remote_file_exists = matches!(
            remote_inventory
                .get(&file.remote_path)
                .map(|resource| &resource.resource_type),
            Some(ResourceType::File)
        );
        let should_upload = !file.fingerprint.reused_from_state || !remote_file_exists;
        if should_upload {
            operations.push(SyncPlanOperation::UploadFile {
                file: file.clone(),
                is_update: remote_file_exists,
            });
        }
    }

    let mut deletes: Vec<_> = collect_coalesced_delete_operations(
        root,
        canonical_remote_path,
        live_plan,
        remote_inventory,
    )
    .into_iter()
    .map(|delete| SyncPlanOperation::DeleteRemote {
        root_id: delete.root_id,
        relative_path: delete.relative_path,
        remote_path: delete.remote_path,
        resource_type: delete.resource_type,
    })
    .collect();
    deletes.sort_by_key(delete_order);
    operations.extend(deletes);

    RootPlan { operations }
}

fn delete_order(operation: &SyncPlanOperation) -> (std::cmp::Reverse<usize>, String) {
    match operation {
        SyncPlanOperation::DeleteRemote { remote_path, .. } => (
            std::cmp::Reverse(remote_path.matches('/').count()),
            remote_path.clone(),
        ),
        _ => (std::cmp::Reverse(0), String::new()),
    }
}

fn local_remote_collision(
    directories: &[LocalDirectoryEntry],
    files: &[LocalFileEntry],
) -> Option<String> {
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for directory in directories {
        if let Some(previous) =
            seen.insert(&directory.normalized_remote_path, &directory.relative_path)
        {
            return Some(format!(
                "canonical remote path collision between {previous} and {}",
                directory.relative_path
            ));
        }
    }
    for file in files {
        if let Some(previous) = seen.insert(&file.normalized_remote_path, &file.relative_path) {
            return Some(format!(
                "canonical remote path collision between {previous} and {}",
                file.relative_path
            ));
        }
    }
    None
}

fn remote_resource_record(root_id: &str, resource: &ResourceMetadata) -> RemoteResourceRecord {
    RemoteResourceRecord {
        root_id: root_id.to_string(),
        remote_path: resource.path.clone(),
        resource_type: match resource.resource_type {
            ResourceType::File => RemoteResourceType::File,
            ResourceType::Directory => RemoteResourceType::Dir,
        },
        size_bytes: resource
            .size_bytes
            .and_then(|size| i64::try_from(size).ok()),
        mtime_remote: resource.modified.clone(),
        remote_md5: resource.md5.clone(),
    }
}

fn file_state_record(
    file: &LocalFileEntry,
    run_id: i64,
    status: FileStateStatus,
    last_error: Option<String>,
) -> FileStateRecord {
    FileStateRecord {
        root_id: file.root_id.clone(),
        local_path: path_to_stable_string(&file.local_path),
        relative_path: file.relative_path.clone(),
        remote_path: file.remote_path.clone(),
        normalized_remote_path: file.normalized_remote_path.clone(),
        size_bytes: u64_to_i64(file.size_bytes),
        mtime_ns: file.mtime_ns,
        ctime_ns: file.ctime_ns,
        fingerprint_kind: file.fingerprint.kind.as_str().to_string(),
        fingerprint_value: Some(file.fingerprint.value.clone()),
        full_sha256: file.fingerprint.full_sha256.clone(),
        remote_etag: None,
        remote_md5: file.fingerprint.local_md5.clone(),
        last_run_id: Some(run_id),
        status,
        last_error,
    }
}

fn file_status_after_upload(file: &LocalFileEntry) -> FileStateStatus {
    let Ok(metadata) = fs::metadata(&file.local_path) else {
        return FileStateStatus::SyncedChangedDuringUpload;
    };
    let current_mtime_ns = metadata.modified().ok().and_then(system_time_to_ns);
    if metadata.len() != file.size_bytes || current_mtime_ns != file.mtime_ns {
        FileStateStatus::SyncedChangedDuringUpload
    } else {
        FileStateStatus::Synced
    }
}

fn known_live_remote_paths(
    canonical_remote_path: &str,
    directories: &[LocalDirectoryEntry],
    files: &[LocalFileEntry],
    deleted_subtrees: &[DeletedLocalSubtree],
) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    paths.insert(canonical_remote_path.to_string());
    for directory in directories {
        if !is_relative_path_deleted(&directory.relative_path, deleted_subtrees) {
            paths.insert(directory.remote_path.clone());
        }
    }
    for file in files {
        if !is_relative_path_deleted(&file.relative_path, deleted_subtrees) {
            paths.insert(file.remote_path.clone());
        }
    }
    paths
}

fn is_relative_path_deleted(relative_path: &str, deleted_subtrees: &[DeletedLocalSubtree]) -> bool {
    deleted_subtrees.iter().any(|subtree| {
        relative_path == subtree.relative_prefix
            || relative_path
                .strip_prefix(subtree.relative_prefix.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn collect_coalesced_delete_operations(
    root: &SyncRootConfig,
    canonical_remote_path: &str,
    live_plan: &LiveScanPlan,
    remote_inventory: &BTreeMap<String, ResourceMetadata>,
) -> Vec<RemoteDeleteOperation> {
    let known_local_remote_paths = known_live_remote_paths(
        canonical_remote_path,
        &live_plan.directories,
        &live_plan.files,
        &live_plan.deleted_subtrees,
    );
    let mut suppressed_prefixes = Vec::<String>::new();
    let mut operations = Vec::new();
    let mut resources = remote_inventory
        .values()
        .filter(|resource| resource.path != canonical_remote_path)
        .cloned()
        .collect::<Vec<_>>();
    resources.sort_by_key(|resource| {
        (
            resource.resource_type != ResourceType::Directory,
            resource.path.matches('/').count(),
            resource.path.clone(),
        )
    });

    for resource in resources {
        if let Err(reason) =
            remote_delete_candidate_relative_path(canonical_remote_path, &resource.path)
        {
            warn!(
                root_id = root.id,
                canonical_remote_path,
                remote_path = resource.path,
                reason,
                "skipped invalid remote delete candidate"
            );
            continue;
        }
        if suppressed_prefixes.iter().any(|prefix| {
            resource.path == *prefix
                || resource
                    .path
                    .strip_prefix(prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }) {
            continue;
        }
        if known_local_remote_paths.contains(&resource.path) {
            continue;
        }

        if let Some(directory_path) = top_missing_remote_directory_prefix(
            canonical_remote_path,
            &resource.path,
            &known_local_remote_paths,
        ) {
            if !suppressed_prefixes.iter().any(|prefix| {
                directory_path == *prefix
                    || directory_path
                        .strip_prefix(prefix.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }) {
                suppressed_prefixes.push(directory_path.clone());
                operations.push(RemoteDeleteOperation {
                    root_id: root.id.clone(),
                    remote_path: directory_path.clone(),
                    relative_path: relative_remote_path(canonical_remote_path, &directory_path),
                    resource_type: ResourceType::Directory,
                });
            }
            continue;
        }

        if resource.resource_type == ResourceType::Directory {
            let child_prefix = format!("{}/", resource.path.trim_end_matches('/'));
            let has_live_descendants = known_local_remote_paths
                .iter()
                .any(|path| path.starts_with(&child_prefix));
            if !has_live_descendants {
                suppressed_prefixes.push(resource.path.clone());
                operations.push(RemoteDeleteOperation {
                    root_id: root.id.clone(),
                    remote_path: resource.path.clone(),
                    relative_path: relative_remote_path(canonical_remote_path, &resource.path),
                    resource_type: ResourceType::Directory,
                });
                continue;
            }
        }

        operations.push(RemoteDeleteOperation {
            root_id: root.id.clone(),
            remote_path: resource.path.clone(),
            relative_path: relative_remote_path(canonical_remote_path, &resource.path),
            resource_type: resource.resource_type.clone(),
        });
    }

    operations.sort_by(|left, right| {
        (
            std::cmp::Reverse(left.remote_path.matches('/').count()),
            left.remote_path.clone(),
        )
            .cmp(&(
                std::cmp::Reverse(right.remote_path.matches('/').count()),
                right.remote_path.clone(),
            ))
    });
    operations
}

fn top_missing_remote_directory_prefix(
    canonical_remote_path: &str,
    remote_path: &str,
    known_local_remote_paths: &BTreeSet<String>,
) -> Option<String> {
    let relative_path = relative_remote_path(canonical_remote_path, remote_path);
    let segments = relative_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() < 2 {
        return None;
    }

    let mut prefix = canonical_remote_path.trim_end_matches('/').to_string();
    for segment in segments.iter().take(segments.len() - 1) {
        prefix.push('/');
        prefix.push_str(segment);
        if known_local_remote_paths.contains(&prefix) {
            continue;
        }
        let child_prefix = format!("{prefix}/");
        let has_live_descendants = known_local_remote_paths
            .iter()
            .any(|path| path.starts_with(&child_prefix));
        if !has_live_descendants {
            return Some(prefix);
        }
    }
    None
}

fn cached_resource_to_metadata(
    canonical_remote_path: &str,
    record: &RemoteResourceRecord,
) -> Option<ResourceMetadata> {
    if record.remote_path != canonical_remote_path
        && record
            .remote_path
            .strip_prefix(format!("{}/", canonical_remote_path.trim_end_matches('/')).as_str())
            .is_none()
    {
        return None;
    }

    Some(ResourceMetadata {
        path: record.remote_path.clone(),
        name: record
            .remote_path
            .rsplit('/')
            .next()
            .filter(|segment| !segment.is_empty())
            .map(ToOwned::to_owned),
        resource_type: match record.resource_type {
            RemoteResourceType::File => ResourceType::File,
            RemoteResourceType::Dir => ResourceType::Directory,
        },
        size_bytes: record.size_bytes.and_then(|size| u64::try_from(size).ok()),
        md5: record.remote_md5.clone(),
        revision: None,
        created: None,
        modified: record.mtime_remote.clone(),
    })
}

fn is_remote_inventory_expired(status: &RootRemoteInventoryStatus) -> bool {
    let Some(expires_at) = &status.snapshot_expires_at_utc else {
        return true;
    };
    let Ok(expires_at) =
        OffsetDateTime::parse(expires_at, &time::format_description::well_known::Rfc3339)
    else {
        return true;
    };
    expires_at <= OffsetDateTime::now_utc()
}

fn format_timestamp(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

fn detect_deleted_local_subtree(
    root_id: &str,
    root_local_path: &Path,
    local_path: &Path,
    relative_path: &str,
    remote_path: &str,
) -> Option<DeletedLocalSubtree> {
    let missing_directory = nearest_missing_directory(root_local_path, local_path)?;
    let relative_prefix = missing_directory
        .strip_prefix(root_local_path)
        .ok()?
        .to_string_lossy()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    if relative_prefix.is_empty() {
        return None;
    }
    Some(DeletedLocalSubtree {
        root_id: root_id.to_string(),
        relative_prefix: relative_prefix.clone(),
        remote_prefix: if relative_prefix == relative_path {
            remote_path.to_string()
        } else {
            let remote_root = remote_path
                .strip_suffix(relative_path)
                .unwrap_or(remote_path)
                .trim_end_matches('/');
            format!("{remote_root}/{relative_prefix}")
        },
    })
}

fn nearest_missing_directory(root_local_path: &Path, local_path: &Path) -> Option<PathBuf> {
    let mut candidate = if local_path.is_dir() {
        local_path.to_path_buf()
    } else {
        local_path.parent()?.to_path_buf()
    };
    let mut nearest = None;
    while candidate != root_local_path {
        if !candidate.exists() {
            nearest = Some(candidate.clone());
            let parent = candidate.parent()?.to_path_buf();
            if parent == root_local_path || parent.exists() {
                break;
            }
            candidate = parent;
            continue;
        }
        break;
    }
    nearest
}

fn relative_depth(relative_path: &str) -> usize {
    if relative_path.is_empty() {
        0
    } else {
        relative_path.matches('/').count() + 1
    }
}

fn take_upload_batch<'a>(
    pending: &mut VecDeque<(&'a LocalFileEntry, bool)>,
    limit: usize,
    large_threshold: u64,
) -> Vec<(&'a LocalFileEntry, bool)> {
    let mut selected = Vec::new();
    let mut deferred = VecDeque::new();
    let mut large_used = false;

    while let Some(candidate) = pending.pop_front() {
        let is_large = candidate.0.size_bytes >= large_threshold;
        if selected.len() < limit && (!is_large || !large_used) {
            if is_large {
                large_used = true;
            }
            selected.push(candidate);
        } else {
            deferred.push_back(candidate);
        }
    }

    *pending = deferred;
    selected
}

fn is_stop_error(kind: YandexDiskErrorKind) -> bool {
    matches!(
        kind,
        YandexDiskErrorKind::AuthUnavailable
            | YandexDiskErrorKind::QuotaExceeded
            | YandexDiskErrorKind::Critical
    )
}

fn status_from_summary(summary: &SyncRunSummary, roots: &[RootSyncSummary]) -> SyncRunStatus {
    if roots.iter().any(|root| root.status == ROOT_STATUS_FAILED) {
        return SyncRunStatus::Failed;
    }
    if roots
        .iter()
        .any(|root| root.status == ROOT_STATUS_SKIPPED || root.status == ROOT_STATUS_PARTIAL_FAILED)
    {
        return SyncRunStatus::PartialFailed;
    }
    if summary.failed_files > 0 {
        return if summary.uploaded_files > 0
            || summary.updated_files > 0
            || summary.deleted_files > 0
        {
            SyncRunStatus::PartialFailed
        } else {
            SyncRunStatus::Failed
        };
    }
    SyncRunStatus::Succeeded
}

fn add_root_summary(total: &mut SyncRunSummary, root: &RootSyncSummary) {
    total.scanned_files += root.scanned_files;
    total.uploaded_files += root.uploaded_files;
    total.updated_files += root.updated_files;
    total.deleted_files += root.deleted_files;
    total.skipped_files += root.skipped_files;
    total.failed_files += root.failed_files;
    total.bytes_uploaded += root.bytes_uploaded;
    if total.error_summary.is_none() {
        total.error_summary = root.error_summary.clone();
    }
}

fn relative_remote_path(canonical_remote_path: &str, remote_path: &str) -> String {
    remote_path
        .strip_prefix(canonical_remote_path.trim_end_matches('/'))
        .unwrap_or(remote_path)
        .trim_start_matches('/')
        .to_string()
}

pub(crate) fn remote_delete_candidate_relative_path(
    managed_root: &str,
    remote_path: &str,
) -> Result<String, &'static str> {
    let managed_root = managed_root.trim_end_matches('/');
    if remote_path == managed_root {
        return Err("empty_relative_path");
    }

    let prefix = format!("{managed_root}/");
    let Some(relative_path) = remote_path.strip_prefix(&prefix) else {
        return Err("outside_managed_root");
    };
    if relative_path.is_empty() {
        return Err("empty_relative_path");
    }
    if relative_path
        .split('/')
        .any(|segment| segment.eq_ignore_ascii_case("disk:"))
    {
        return Err("phantom_disk_segment");
    }

    Ok(relative_path.to_string())
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

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn ensure_valid_config(config: &AppConfig) -> Result<(), SyncError> {
    let report = validate_config(config);
    if report.is_valid() {
        return Ok(());
    }

    let errors = report
        .errors()
        .iter()
        .map(|error| error.message.clone())
        .collect::<Vec<_>>()
        .join("; ");
    Err(SyncError::ConfigInvalid(errors))
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(
        COMPONENT_NAME,
        "sync planner and one-way executor boundary is available",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use yds_core::{config::default_config, ComponentHealth};
    use yds_yandex_disk::{
        error::YandexDiskErrorKind,
        models::{DiskInfo, QuotaInfo},
        MockOperationKind, MockYandexDiskClient,
    };

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), ComponentHealth::Ok);
    }

    #[test]
    fn local_remote_collision_detects_case_insensitive_directory_conflict() {
        let mut report = ScanReport::new("root");
        report.directories.push(LocalDirectoryEntry {
            root_id: "root".to_string(),
            local_path: PathBuf::from(r"D:\root\Case"),
            relative_path: "Case".to_string(),
            remote_path: "disk:/root/Case".to_string(),
            normalized_remote_path: "disk:/root/case".to_string(),
        });
        report.directories.push(LocalDirectoryEntry {
            root_id: "root".to_string(),
            local_path: PathBuf::from(r"D:\root\case"),
            relative_path: "case".to_string(),
            remote_path: "disk:/root/case".to_string(),
            normalized_remote_path: "disk:/root/case".to_string(),
        });

        let message = local_remote_collision(&report.directories, &report.files).unwrap();

        assert!(message.contains("Case"));
        assert!(message.contains("case"));
    }

    #[tokio::test]
    async fn first_run_uploads_files_and_second_run_is_noop() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        fixture.write_file("nested/b.txt", "two");
        let client = MockYandexDiskClient::new();

        let first = fixture.run(&client).await;
        assert_eq!(first.status, SyncRunStatus::Succeeded);
        assert_eq!(first.summary.uploaded_files, 2);
        assert_eq!(first.summary.deleted_files, 0);

        client.clear_operation_log().unwrap();
        let second = fixture.run(&client).await;
        assert_eq!(second.status, SyncRunStatus::Succeeded);
        assert_eq!(second.summary.uploaded_files, 0);
        assert_eq!(second.summary.deleted_files, 0);
        let log = client.operation_log().unwrap();
        assert!(!log
            .iter()
            .any(|operation| matches!(operation.kind, MockOperationKind::Upload)));
        assert!(!log
            .iter()
            .any(|operation| matches!(operation.kind, MockOperationKind::DeletePermanently)));
    }

    #[tokio::test]
    async fn changed_local_file_triggers_update_upload() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();

        fixture.run(&client).await;
        client.clear_operation_log().unwrap();
        fixture.write_file("a.txt", "two");

        let report = fixture.run(&client).await;
        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.updated_files, 1);
        assert!(client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload
                && operation.remote_path == "disk:/test-root/a.txt"));
    }

    #[tokio::test]
    async fn removed_local_file_deletes_remote_extra_at_end() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        fixture.write_file("b.txt", "two");
        let client = MockYandexDiskClient::new();
        fixture.run(&client).await;

        fs::remove_file(fixture.root_path().join("a.txt")).unwrap();
        client.clear_operation_log().unwrap();
        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.deleted_files, 1);
        let log = client.operation_log().unwrap();
        let delete_index = log
            .iter()
            .position(|operation| operation.kind == MockOperationKind::DeletePermanently)
            .unwrap();
        let upload_index = log
            .iter()
            .position(|operation| operation.kind == MockOperationKind::Upload);
        assert!(upload_index.is_none_or(|index| index < delete_index));
    }

    #[tokio::test]
    async fn remote_extra_directory_is_deleted_deepest_first() {
        let fixture = TestFixture::new();
        fixture.write_file("keep.txt", "one");
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::directory("disk:/test-root/old"))
            .unwrap();
        client
            .insert_resource(ResourceMetadata::file("disk:/test-root/old/file.txt", 1))
            .unwrap();

        let report = fixture.run(&client).await;
        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.deleted_files, 1);
        let deletes: Vec<_> = client
            .operation_log()
            .unwrap()
            .into_iter()
            .filter(|operation| operation.kind == MockOperationKind::DeletePermanently)
            .map(|operation| operation.remote_path)
            .collect();
        assert_eq!(deletes, ["disk:/test-root/old"]);
    }

    #[tokio::test]
    async fn remote_delete_not_found_is_idempotent_success() {
        let fixture = TestFixture::new();
        fixture.write_file("keep.txt", "one");
        fixture.write_file("old.txt", "old");
        let client = MockYandexDiskClient::new();

        fixture.run(&client).await;
        fs::remove_file(fixture.root_path().join("old.txt")).unwrap();
        client.clear_operation_log().unwrap();
        client
            .fail_next(
                MockOperationKind::DeletePermanently,
                "disk:/test-root/old.txt",
                YandexDiskError::new(YandexDiskErrorKind::NotFound, "Resource not found"),
            )
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(report.summary.deleted_files, 0);
        assert!(fixture
            .repository
            .list_recent_failed_items(10)
            .unwrap()
            .is_empty());
        let operations = fixture.repository.list_recent_operations(10).unwrap();
        let delete_operation = operations
            .iter()
            .find(|operation| operation.operation == "delete_remote")
            .unwrap();
        assert_eq!(delete_operation.status, StateOperationStatus::Succeeded);
        assert_eq!(delete_operation.error_kind, None);
        assert_eq!(
            fixture
                .repository
                .get_file_state("root", "old.txt")
                .unwrap()
                .unwrap()
                .status,
            FileStateStatus::Deleted
        );
    }

    #[test]
    fn coalesced_delete_operations_ignore_invalid_remote_candidates() {
        let fixture = TestFixture::new();
        let root = &fixture.config.roots[0];
        let live_plan = LiveScanPlan {
            root_id: "root".to_string(),
            root_local_path: fixture.root_path(),
            files: Vec::new(),
            directories: Vec::new(),
            deleted_subtrees: Vec::new(),
        };
        let remote_inventory = BTreeMap::from([
            (
                "disk:/other/remote.txt".to_string(),
                ResourceMetadata::file("disk:/other/remote.txt", 1),
            ),
            (
                "disk:/test-root/disk:".to_string(),
                ResourceMetadata::directory("disk:/test-root/disk:"),
            ),
            (
                "disk:/test-root/disk:/child.txt".to_string(),
                ResourceMetadata::file("disk:/test-root/disk:/child.txt", 1),
            ),
            (
                "disk:/test-root/old.txt".to_string(),
                ResourceMetadata::file("disk:/test-root/old.txt", 1),
            ),
        ]);

        let deletes = collect_coalesced_delete_operations(
            root,
            "disk:/test-root",
            &live_plan,
            &remote_inventory,
        );

        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].remote_path, "disk:/test-root/old.txt");
    }

    #[tokio::test]
    async fn removed_local_subtree_is_deleted_in_one_remote_operation() {
        let fixture = TestFixture::new();
        fixture.write_file("gone/a.txt", "one");
        fixture.write_file("gone/nested/b.txt", "two");
        fixture.write_file("keep.txt", "keep");
        let client = MockYandexDiskClient::new();

        fixture.run(&client).await;
        fs::remove_dir_all(fixture.root_path().join("gone")).unwrap();
        client.clear_operation_log().unwrap();

        let report = fixture.run(&client).await;
        let deletes: Vec<_> = client
            .operation_log()
            .unwrap()
            .into_iter()
            .filter(|operation| operation.kind == MockOperationKind::DeletePermanently)
            .map(|operation| operation.remote_path)
            .collect();
        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.deleted_files, 1);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(deletes, ["disk:/test-root/gone"]);
    }

    #[tokio::test]
    async fn cached_remote_inventory_is_reused_until_forced_rescan() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();

        fixture.run(&client).await;
        client.clear_operation_log().unwrap();

        let second = fixture.run(&client).await;
        assert_eq!(second.status, SyncRunStatus::Succeeded);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::ListRecursive));

        client.clear_operation_log().unwrap();
        let forced = fixture
            .run_with_options(
                &client,
                SyncRunOptions {
                    force_remote_rescan: true,
                    ..SyncRunOptions::default()
                },
            )
            .await;
        assert_eq!(forced.status, SyncRunStatus::Succeeded);
        assert!(client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::ListRecursive));
    }

    #[tokio::test]
    async fn missing_root_records_skipped_and_does_not_delete_remote() {
        let fixture = TestFixture::new();
        let missing = fixture.root_path().join("missing");
        let mut config = fixture.config.clone();
        config.roots[0].local_path = path_to_stable_string(&missing);
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::file("disk:/test-root/remote.txt", 1))
            .unwrap();
        let repository = StateRepository::open_in_memory_for_tests().unwrap();
        let engine = SyncEngine::new(&config, &repository, Arc::new(client.clone()));

        let report = engine
            .run_once(SyncRunOptions::default(), &CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(report.status, SyncRunStatus::PartialFailed);
        assert_eq!(report.summary.skipped_files, 1);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::DeletePermanently));
        let status = repository.get_sync_root_status("root").unwrap().unwrap();
        assert_eq!(status.0.as_deref(), Some(ROOT_STATUS_SKIPPED));
    }

    #[tokio::test]
    async fn remote_inventory_failure_marks_partial_and_skips_destructive_phases() {
        let fixture = TestFixture::new();
        fixture.write_file("local.txt", "local");
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::directory("disk:/test-root"))
            .unwrap();
        client
            .insert_file("disk:/test-root/remote-extra.txt", b"remote")
            .unwrap();
        client
            .fail_next(
                MockOperationKind::ListRecursive,
                "disk:/test-root",
                YandexDiskError::new(YandexDiskErrorKind::Transient, "decode body failed"),
            )
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::PartialFailed);
        assert_eq!(report.roots[0].status, ROOT_STATUS_PARTIAL_FAILED);
        assert_eq!(report.summary.failed_files, 1);
        let log = client.operation_log().unwrap();
        assert!(log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::ListRecursive));
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::DeletePermanently));
        let failed = fixture.repository.list_recent_failed_items(10).unwrap();
        assert_eq!(failed[0].item.operation, "remote_inventory");
        assert_eq!(failed[0].item.error_kind, "transient");
    }

    #[tokio::test]
    async fn one_upload_failure_does_not_stop_other_files() {
        let fixture = TestFixture::new();
        fixture.write_file("bad.txt", "bad");
        fixture.write_file("good.txt", "good");
        let client = MockYandexDiskClient::new();
        client
            .fail_next(
                MockOperationKind::Upload,
                "disk:/test-root/bad.txt",
                YandexDiskError::new(YandexDiskErrorKind::Permanent, "bad file"),
            )
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::PartialFailed);
        assert_eq!(report.summary.failed_files, 1);
        assert!(client
            .resources()
            .unwrap()
            .iter()
            .any(|resource| resource.path == "disk:/test-root/good.txt"));
        assert!(fixture
            .repository
            .get_file_state("root", "good.txt")
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn persistent_upload_conflict_is_recorded_as_skipped() {
        let fixture = TestFixture::new();
        fixture.write_file("blocked.txt", "blocked");
        let client = MockYandexDiskClient::new();
        client
            .fail_next(
                MockOperationKind::Upload,
                "disk:/test-root/blocked.txt",
                YandexDiskError::persistent_uploader_conflict("disk:/test-root/blocked.txt"),
            )
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(report.summary.skipped_files, 1);
        let skipped = fixture.repository.list_recent_skipped_items(10).unwrap();
        assert_eq!(skipped.len(), 1);
        assert_eq!(
            skipped[0].item.reason_code,
            "remote_uploader_content_conflict"
        );
        assert_eq!(
            fixture
                .repository
                .get_file_state("root", "blocked.txt")
                .unwrap()
                .unwrap()
                .status,
            FileStateStatus::Skipped
        );
        assert!(fixture
            .repository
            .list_recent_failed_items(10)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn local_upload_source_unavailable_is_recorded_as_skipped() {
        let fixture = TestFixture::new();
        fixture.write_file("busy.1cd", "busy");
        let client = MockYandexDiskClient::new();
        client
            .fail_next(
                MockOperationKind::Upload,
                "disk:/test-root/busy.1cd",
                YandexDiskError::local_upload_source_unavailable(
                    fixture.root_path().join("busy.1cd").display(),
                    "file is locked by another process",
                ),
            )
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(report.summary.skipped_files, 1);
        let skipped = fixture.repository.list_recent_skipped_items(10).unwrap();
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].item.reason_code, "local_file_unavailable");
        assert!(fixture
            .repository
            .list_recent_failed_items(10)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn quota_exceeded_stops_before_uploads_and_deletes() {
        let fixture = TestFixture::new();
        fixture.write_file("big.txt", "content larger than quota");
        let client = MockYandexDiskClient::new();
        client
            .set_disk_info(DiskInfo {
                quota: QuotaInfo {
                    total_space: Some(1),
                    used_space: Some(0),
                    trash_size: Some(0),
                },
                system_folders: BTreeMap::new(),
            })
            .unwrap();
        client
            .insert_resource(ResourceMetadata::file("disk:/test-root/old.txt", 1))
            .unwrap();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Failed);
        assert!(report
            .summary
            .error_summary
            .as_deref()
            .unwrap_or_default()
            .contains("quota"));
        let log = client.operation_log().unwrap();
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::DeletePermanently));
    }

    #[tokio::test]
    async fn cancellation_stops_cleanly() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let engine = SyncEngine::new(&fixture.config, &fixture.repository, Arc::new(client));

        let report = engine
            .run_once(SyncRunOptions::default(), &cancellation)
            .await
            .unwrap();

        assert_eq!(report.status, SyncRunStatus::Cancelled);
    }

    #[tokio::test]
    async fn sqlite_staging_file_is_cleaned_after_sync() {
        let fixture = TestFixture::new();
        let db_path = fixture.root_path().join("data.sqlite");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)", [])
            .unwrap();
        conn.execute("INSERT INTO items DEFAULT VALUES", [])
            .unwrap();
        drop(conn);
        let client = MockYandexDiskClient::new();

        let report = fixture.run(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        let staging_entries: Vec<_> = fs::read_dir(fixture.staging_path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(staging_entries.is_empty());
    }

    #[test]
    fn file_status_detects_file_changed_after_scan() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let scanner = Scanner::new(ScanOptions::from_config(
            &fixture.config.sync,
            &fixture.config.global_excludes,
            &fixture.config.absolute_excludes,
            fixture.staging_path(),
        ));
        let report = scanner
            .scan_root_with_repository(
                &ScanRoot::new(
                    "root".to_string(),
                    fixture.root_path(),
                    "disk:/test-root".to_string(),
                    Vec::new(),
                ),
                &fixture.repository,
            )
            .unwrap();
        fixture.write_file("a.txt", "changed");

        assert_eq!(
            file_status_after_upload(&report.files[0]),
            FileStateStatus::SyncedChangedDuringUpload
        );
    }

    #[test]
    fn upload_batch_limits_large_files_to_one_slot() {
        let mut fixture = TestFixture::new();
        fixture.config.sync.large_file_name_size_only_min_bytes = 8;
        fixture.write_file("large-a.bin", "1234567890");
        fixture.write_file("large-b.bin", "abcdefghij");
        fixture.write_file("small.txt", "tiny");
        let scanner = Scanner::new(ScanOptions::from_config(
            &fixture.config.sync,
            &fixture.config.global_excludes,
            &fixture.config.absolute_excludes,
            fixture.staging_path(),
        ));
        let report = scanner
            .scan_root_with_repository(
                &ScanRoot::new(
                    "root".to_string(),
                    fixture.root_path(),
                    "disk:/test-root".to_string(),
                    Vec::new(),
                ),
                &fixture.repository,
            )
            .unwrap();
        let mut pending = report
            .files
            .iter()
            .map(|file| (file, false))
            .collect::<VecDeque<_>>();

        let batch = take_upload_batch(
            &mut pending,
            3,
            fixture.config.sync.large_file_name_size_only_min_bytes,
        );

        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch
                .iter()
                .filter(|(file, _)| {
                    file.size_bytes >= fixture.config.sync.large_file_name_size_only_min_bytes
                })
                .count(),
            1
        );
        assert_eq!(pending.len(), 1);
    }

    struct TestFixture {
        temp: tempfile::TempDir,
        config: AppConfig,
        repository: StateRepository,
    }

    impl TestFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("root");
            let staging = temp.path().join("staging");
            fs::create_dir_all(&root).unwrap();
            fs::create_dir_all(&staging).unwrap();
            let mut config = default_config();
            config.paths.state_db = path_to_stable_string(temp.path().join("state.sqlite"));
            config.paths.staging_dir = path_to_stable_string(&staging);
            config.global_excludes = Vec::new();
            config.absolute_excludes = Vec::new();
            config.roots = vec![SyncRootConfig {
                id: "root".to_string(),
                name: "Root".to_string(),
                enabled: true,
                local_path: path_to_stable_string(&root),
                remote_path_override: Some("disk:/test-root".to_string()),
                legacy_remote_paths: Vec::new(),
                excludes: Vec::new(),
            }];
            let repository = StateRepository::open_in_memory_for_tests().unwrap();
            Self {
                temp,
                config,
                repository,
            }
        }

        fn root_path(&self) -> PathBuf {
            self.temp.path().join("root")
        }

        fn staging_path(&self) -> PathBuf {
            self.temp.path().join("staging")
        }

        fn write_file(&self, relative_path: &str, content: &str) {
            let path = self.root_path().join(relative_path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            let mut file = fs::File::create(path).unwrap();
            file.write_all(content.as_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        async fn run(&self, client: &MockYandexDiskClient) -> SyncRunReport {
            self.run_with_options(client, SyncRunOptions::default())
                .await
        }

        async fn run_with_options(
            &self,
            client: &MockYandexDiskClient,
            options: SyncRunOptions,
        ) -> SyncRunReport {
            let engine = SyncEngine::new(&self.config, &self.repository, Arc::new(client.clone()));
            engine
                .run_once(options, &CancellationToken::new())
                .await
                .unwrap()
        }
    }
}
