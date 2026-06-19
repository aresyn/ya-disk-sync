use std::{
    collections::{BTreeMap, VecDeque},
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokio::task::JoinSet;
use uuid::Uuid;
use yds_core::{
    config::{validate_config, AppConfig, SyncRootConfig},
    path_mapping::{canonical_remote_path, PathMappingError},
};
use yds_scanner::{
    record_scan_findings, LocalDirectoryEntry, LocalFileEntry, ScanOptions, ScanRoot, Scanner,
    ScannerError,
};
use yds_state::{
    models::{
        DirectoryStateRecord, DirectoryStateStatus, FailedItemRecord, FileStateRecord,
        FileStateStatus, MigrationMapRecord, MigrationMapStatus, OperationRecord,
        OperationStatus as StateOperationStatus, RemoteResourceRecord, RemoteResourceType,
        RootRemoteInventoryStatus, SkippedItemRecord, SyncRunStatus, SyncRunSummary,
        SyncRunTrigger,
    },
    StateError, StateRepository,
};
use yds_yandex_disk::{
    error::{YandexDiskError, YandexDiskErrorKind},
    models::{ListRecursiveOptions, ResourceMetadata, ResourceType},
    UploadSource, YandexDiskClient,
};

use crate::{
    cached_resource_to_metadata, collect_coalesced_delete_operations, format_timestamp,
    is_remote_inventory_expired, remote_delete_candidate_relative_path, CancellationToken,
    LiveScanPlan,
};

const DEFAULT_STALE_LOCK_HOURS: i64 = 6;
const ROOT_STATUS_SUCCEEDED: &str = "succeeded";
const ROOT_STATUS_SKIPPED: &str = "skipped";
const ROOT_STATUS_PARTIAL_FAILED: &str = "partial_failed";
const ROOT_STATUS_FAILED: &str = "failed";
const ROOT_STATUS_CANCELLED: &str = "cancelled";
const ADOPTION_BATCH_SIZE: usize = 1_000;
const FILE_PHASE_PROGRESS_INTERVAL: usize = 5_000;

struct FileProcessingFailure<'a> {
    run_id: i64,
    root: &'a SyncRootConfig,
    file: &'a LocalFileEntry,
    operation: &'a str,
    operation_remote_path: &'a str,
    error: &'a MigrationError,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("config validation failed: {0}")]
    ConfigInvalid(String),
    #[error("migration run is already running")]
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
}

#[derive(Debug, Clone)]
pub struct MigrationRunOptions {
    pub lock_owner: String,
    pub stale_lock_after: Duration,
    pub force_remote_rescan: bool,
}

impl Default for MigrationRunOptions {
    fn default() -> Self {
        Self {
            lock_owner: format!("yds-migration-{}", Uuid::new_v4()),
            stale_lock_after: Duration::hours(DEFAULT_STALE_LOCK_HOURS),
            force_remote_rescan: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    pub operations: Vec<MigrationPlanOperation>,
}

impl MigrationPlan {
    #[must_use]
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }
}

impl Default for MigrationPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationPlanOperation {
    CreateCanonicalDirectory {
        remote_path: String,
    },
    MoveLegacyResource {
        from: String,
        to: String,
    },
    AdoptRemoteFile {
        relative_path: String,
        remote_path: String,
        method: String,
    },
    UploadFile {
        relative_path: String,
        remote_path: String,
        is_update: bool,
    },
    DeleteRemote {
        remote_path: String,
        resource_type: ResourceType,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationRootSummary {
    pub root_id: String,
    pub local_path: String,
    pub canonical_remote_path: String,
    pub legacy_remote_paths: Vec<String>,
    pub status: String,
    pub scanned_files: i64,
    pub adopted_files: i64,
    pub moved_resources: i64,
    pub uploaded_files: i64,
    pub updated_files: i64,
    pub deleted_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
    pub bytes_uploaded: i64,
    pub error_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRunReport {
    pub run_id: i64,
    pub status: SyncRunStatus,
    pub summary: SyncRunSummary,
    pub roots: Vec<MigrationRootSummary>,
    pub plan: MigrationPlan,
}

impl MigrationRunReport {
    #[must_use]
    pub fn is_successful(&self) -> bool {
        self.status == SyncRunStatus::Succeeded
    }
}

pub struct MigrationEngine<'a> {
    config: &'a AppConfig,
    repository: &'a StateRepository,
    client: Arc<dyn YandexDiskClient>,
}

impl<'a> MigrationEngine<'a> {
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
        options: MigrationRunOptions,
        cancellation: &CancellationToken,
    ) -> Result<MigrationRunReport, MigrationError> {
        ensure_valid_config(self.config)?;

        let guard = self
            .repository
            .try_acquire_run_lock(
                &options.lock_owner,
                OffsetDateTime::now_utc(),
                options.stale_lock_after,
            )?
            .ok_or(MigrationError::AlreadyRunning)?;

        self.repository.repair_interrupted_runs()?;
        let run_id = self.repository.begin_run(SyncRunTrigger::Migration)?;
        let mut report = MigrationRunReport {
            run_id,
            status: SyncRunStatus::Running,
            summary: SyncRunSummary::default(),
            roots: Vec::new(),
            plan: MigrationPlan::new(),
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
                report.summary.error_summary = Some("migration run cancelled".to_string());
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
        report: &mut MigrationRunReport,
        force_remote_rescan: bool,
    ) -> Result<(), MigrationError> {
        let mut available_remote_bytes = self.available_remote_bytes(report).await?;
        let scanner = Scanner::new(ScanOptions::from_config(
            &self.config.sync,
            &self.config.global_excludes,
            &self.config.absolute_excludes,
            PathBuf::from(&self.config.paths.staging_dir),
        ));

        for root in self.config.roots.iter().filter(|root| root.enabled) {
            if cancellation.is_cancelled() {
                report.summary.error_summary = Some("migration run cancelled".to_string());
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

            let result = self
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

            if let Some(consumed) = result.bytes_uploaded {
                if let Some(available) = &mut available_remote_bytes {
                    *available = available.saturating_sub(consumed);
                }
            }

            add_root_summary(&mut report.summary, &root_summary);
            let should_stop = result.should_stop;
            report.roots.push(root_summary);
            if should_stop || cancellation.is_cancelled() {
                break;
            }
        }

        Ok(())
    }

    async fn available_remote_bytes(
        &self,
        report: &mut MigrationRunReport,
    ) -> Result<Option<u64>, MigrationError> {
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

    fn initial_root_summary(
        &self,
        root: &SyncRootConfig,
    ) -> Result<MigrationRootSummary, MigrationError> {
        let canonical_remote_path = canonical_remote_path(
            &self.config.instance.remote_root,
            &root.local_path,
            root.remote_path_override.as_deref(),
        )
        .map_err(|source| MigrationError::PathMapping {
            root_id: root.id.clone(),
            source,
        })?;

        Ok(MigrationRootSummary {
            root_id: root.id.clone(),
            local_path: root.local_path.clone(),
            canonical_remote_path,
            legacy_remote_paths: root.legacy_remote_paths.clone(),
            status: ROOT_STATUS_SUCCEEDED.to_string(),
            ..MigrationRootSummary::default()
        })
    }

    fn record_missing_root(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut MigrationRootSummary,
    ) -> Result<(), MigrationError> {
        let message = format!(
            "local root is missing or not a directory: {}",
            root.local_path
        );
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
        for legacy in &root.legacy_remote_paths {
            self.repository.upsert_migration_map(&MigrationMapRecord {
                root_id: root.id.clone(),
                legacy_remote_path: legacy.clone(),
                canonical_remote_path: root_summary.canonical_remote_path.clone(),
                status: MigrationMapStatus::Skipped,
                last_error: Some(message.clone()),
            })?;
        }
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
        report: &mut MigrationRunReport,
        root_summary: &mut MigrationRootSummary,
        force_remote_rescan: bool,
    ) -> Result<MigrationRootResult, MigrationError> {
        let base_summary = report.summary.clone();
        let scan_root = ScanRoot::new(
            root.id.clone(),
            PathBuf::from(&root.local_path),
            root_summary.canonical_remote_path.clone(),
            root.excludes.clone(),
        );
        tracing::info!(
            root_id = root.id.as_str(),
            local_path = root.local_path.as_str(),
            remote_path = root_summary.canonical_remote_path.as_str(),
            "migration root scan start"
        );
        let scan_report = match scanner.scan_root_with_repository(&scan_root, self.repository) {
            Ok(scan_report) => scan_report,
            Err(error) => {
                let message = error.to_string();
                self.fail_root(root, root_summary, "scan", "scanner", &message)?;
                return Ok(MigrationRootResult::stop(false));
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
        tracing::info!(
            root_id = root.id.as_str(),
            files = root_summary.scanned_files,
            directories = live_plan.directories.len(),
            skipped = root_summary.skipped_files,
            failed = root_summary.failed_files,
            "migration root scan ok"
        );

        if let Some(message) =
            crate::local_remote_collision(&live_plan.directories, &live_plan.files)
        {
            self.fail_root(root, root_summary, "plan", "path_collision", &message)?;
            scan_report.cleanup_staging_files()?;
            return Ok(MigrationRootResult::stop(false));
        }

        tracing::info!(
            root_id = root.id.as_str(),
            remote_path = root_summary.canonical_remote_path.as_str(),
            "migration root remote inventory start"
        );
        let mut remote_sets = match self
            .load_remote_sets(
                root,
                &root_summary.canonical_remote_path,
                &live_plan,
                force_remote_rescan,
            )
            .await
        {
            Ok(remote_sets) => remote_sets,
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
                scan_report.cleanup_staging_files()?;
                return Ok(MigrationRootResult::stop(true));
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
                scan_report.cleanup_staging_files()?;
                return Ok(MigrationRootResult::stop(false));
            }
        };
        self.record_remote_resources(root, &remote_sets)?;

        let top_level_result = self
            .try_top_level_move(
                run_id,
                root,
                root_summary,
                cancellation,
                report,
                &mut remote_sets,
            )
            .await?;
        if top_level_result.should_stop || cancellation.is_cancelled() {
            scan_report.cleanup_staging_files()?;
            self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
            return Ok(MigrationRootResult::stop(true));
        }

        let merge_result = self
            .apply_merge_moves(
                run_id,
                root,
                &live_plan,
                root_summary,
                cancellation,
                report,
                &mut remote_sets,
                &base_summary,
            )
            .await?;
        if merge_result.should_stop || cancellation.is_cancelled() {
            scan_report.cleanup_staging_files()?;
            self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
            return Ok(MigrationRootResult::stop(true));
        }

        let upload_result = self
            .apply_adopt_or_upload(
                run_id,
                root,
                &mut live_plan,
                root_summary,
                cancellation,
                available_remote_bytes,
                report,
                &mut remote_sets,
                &base_summary,
            )
            .await?;
        scan_report.cleanup_staging_files()?;
        if upload_result.should_stop || cancellation.is_cancelled() {
            self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
            return Ok(MigrationRootResult {
                should_stop: true,
                bytes_uploaded: Some(upload_result.bytes_uploaded),
            });
        }

        let stop_after_delete = self
            .apply_deletes(
                run_id,
                root,
                &live_plan,
                root_summary,
                cancellation,
                report,
                &remote_sets,
                &base_summary,
            )
            .await?;
        self.finish_root_cancelled_or_failed(root, root_summary, cancellation)?;
        self.persist_root_progress(run_id, &base_summary, root_summary)?;

        Ok(MigrationRootResult {
            should_stop: stop_after_delete,
            bytes_uploaded: Some(upload_result.bytes_uploaded),
        })
    }

    async fn load_remote_sets(
        &self,
        root: &SyncRootConfig,
        canonical_remote_path: &str,
        live_plan: &LiveScanPlan,
        force_remote_rescan: bool,
    ) -> Result<RemoteSets, YandexDiskError> {
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
                    return Ok(split_cached_remote_sets(
                        root,
                        canonical_remote_path,
                        &cached,
                        true,
                    ));
                }
            }
        }

        let canonical = remote_inventory(
            self.client.as_ref(),
            canonical_remote_path,
            ListRecursiveOptions {
                prune_remote_prefixes: live_plan.prune_remote_prefixes(),
            },
        )
        .await?;
        let mut legacy = Vec::new();
        for legacy_root in &root.legacy_remote_paths {
            let inventory = remote_inventory(
                self.client.as_ref(),
                legacy_root,
                ListRecursiveOptions {
                    prune_remote_prefixes: live_plan
                        .deleted_subtrees
                        .iter()
                        .map(|subtree| legacy_remote_prefix(legacy_root, &subtree.relative_prefix))
                        .collect(),
                },
            )
            .await?;
            legacy.push(LegacyInventory {
                root_path: legacy_root.clone(),
                resources: inventory,
            });
        }
        Ok(RemoteSets {
            canonical,
            legacy,
            from_cache: false,
        })
    }

    fn record_remote_resources(
        &self,
        root: &SyncRootConfig,
        remote_sets: &RemoteSets,
    ) -> Result<(), MigrationError> {
        let records = remote_sets
            .canonical
            .values()
            .chain(
                remote_sets
                    .legacy
                    .iter()
                    .flat_map(|legacy| legacy.resources.values()),
            )
            .map(|resource| remote_resource_record(&root.id, resource))
            .collect::<Vec<_>>();
        let total = records.len();
        let started = Instant::now();
        tracing::info!(
            root_id = root.id.as_str(),
            total,
            "migration remote resource state upsert start"
        );
        self.repository
            .replace_remote_resources_for_root(&root.id, &records)?;
        let now = OffsetDateTime::now_utc();
        let expires_at =
            now + Duration::hours(self.config.sync.remote_inventory_cache_ttl_hours as i64);
        self.repository
            .set_root_remote_inventory_status(&RootRemoteInventoryStatus {
                root_id: root.id.clone(),
                is_complete: true,
                snapshot_completed_at_utc: Some(format_timestamp(now)),
                snapshot_expires_at_utc: Some(format_timestamp(expires_at)),
                resource_count: i64::try_from(total).unwrap_or(i64::MAX),
                last_error: None,
            })?;
        tracing::info!(
            root_id = root.id.as_str(),
            total,
            elapsed_ms = started.elapsed().as_millis(),
            "migration remote resource state upsert ok"
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_top_level_move(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        root_summary: &mut MigrationRootSummary,
        cancellation: &CancellationToken,
        report: &mut MigrationRunReport,
        remote_sets: &mut RemoteSets,
    ) -> Result<MigrationRootResult, MigrationError> {
        if cancellation.is_cancelled() || !remote_sets.canonical.is_empty() {
            return Ok(MigrationRootResult::stop(false));
        }

        let Some(legacy_index) = remote_sets
            .legacy
            .iter()
            .position(|legacy| legacy.resources.contains_key(&legacy.root_path))
        else {
            return Ok(MigrationRootResult::stop(false));
        };

        let from = remote_sets.legacy[legacy_index].root_path.clone();
        let to = root_summary.canonical_remote_path.clone();
        self.ensure_parent_directories(
            run_id,
            &to,
            cancellation,
            report,
            root_summary,
            &mut remote_sets.canonical,
        )
        .await?;
        if cancellation.is_cancelled() {
            return Ok(MigrationRootResult::stop(true));
        }

        let operation_id = self.repository.append_operation_started(&OperationRecord {
            run_id,
            operation: "migration_move_legacy_root".to_string(),
            local_path: None,
            remote_path: Some(from.clone()),
        })?;
        match self.client.move_resource(&from, &to, false).await {
            Ok(()) => {
                self.repository.finish_operation(
                    operation_id,
                    StateOperationStatus::Succeeded,
                    None,
                    None,
                )?;
                self.repository.upsert_migration_map(&MigrationMapRecord {
                    root_id: root.id.clone(),
                    legacy_remote_path: from.clone(),
                    canonical_remote_path: to.clone(),
                    status: MigrationMapStatus::Moved,
                    last_error: None,
                })?;
                report
                    .plan
                    .operations
                    .push(MigrationPlanOperation::MoveLegacyResource {
                        from: from.clone(),
                        to: to.clone(),
                    });
                root_summary.moved_resources += 1;
                remote_sets.canonical =
                    remote_inventory(self.client.as_ref(), &to, ListRecursiveOptions::default())
                        .await?;
                remote_sets.legacy[legacy_index].resources.clear();
                Ok(MigrationRootResult::stop(false))
            }
            Err(error) => {
                self.repository.finish_operation(
                    operation_id,
                    StateOperationStatus::Failed,
                    Some(error.kind().as_str()),
                    Some(error.message()),
                )?;
                self.record_migration_failure(
                    root,
                    Some(&from),
                    Some(&to),
                    "migration_move_legacy_root",
                    &error,
                )?;
                root_summary.failed_files += 1;
                root_summary.error_summary = Some(error.to_string());
                Ok(MigrationRootResult::stop(is_stop_error(error.kind())))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_merge_moves(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        live_plan: &LiveScanPlan,
        root_summary: &mut MigrationRootSummary,
        cancellation: &CancellationToken,
        report: &mut MigrationRunReport,
        remote_sets: &mut RemoteSets,
        base_summary: &SyncRunSummary,
    ) -> Result<MigrationRootResult, MigrationError> {
        let canonical_remote_path = root_summary.canonical_remote_path.clone();
        self.ensure_directory(
            run_id,
            &canonical_remote_path,
            cancellation,
            report,
            root_summary,
            &mut remote_sets.canonical,
        )
        .await?;
        if cancellation.is_cancelled() {
            return Ok(MigrationRootResult::stop(true));
        }

        let root_directory = LocalDirectoryEntry {
            root_id: root.id.clone(),
            local_path: PathBuf::from(&root.local_path),
            relative_path: String::new(),
            remote_path: root_summary.canonical_remote_path.clone(),
            normalized_remote_path: root_summary.canonical_remote_path.to_lowercase(),
        };
        self.repository
            .upsert_directory_state(&directory_state_record(&root_directory, run_id))?;

        for directory in &live_plan.directories {
            self.ensure_directory(
                run_id,
                &directory.remote_path,
                cancellation,
                report,
                root_summary,
                &mut remote_sets.canonical,
            )
            .await?;
            self.repository
                .upsert_directory_state(&directory_state_record(directory, run_id))?;
            if cancellation.is_cancelled() {
                return Ok(MigrationRootResult::stop(true));
            }
            self.persist_root_progress(run_id, base_summary, root_summary)?;
        }

        for file in &live_plan.files {
            if cancellation.is_cancelled() {
                return Ok(MigrationRootResult::stop(true));
            }
            if matches!(
                remote_sets
                    .canonical
                    .get(&file.remote_path)
                    .map(|resource| &resource.resource_type),
                Some(ResourceType::File)
            ) {
                continue;
            }

            let Some((legacy_index, legacy_path, legacy_resource)) =
                remote_sets.find_legacy_file(&file.relative_path)
            else {
                continue;
            };
            let decision = match remote_matches_local(
                self.client.as_ref(),
                file,
                &legacy_resource,
                self.config,
            )
            .await
            {
                Ok(decision) => decision,
                Err(error) if per_file_processing_error(&error).is_some() => {
                    self.record_file_processing_failure(
                        FileProcessingFailure {
                            run_id,
                            root,
                            file,
                            operation: "migration_compare_legacy_file",
                            operation_remote_path: &legacy_path,
                            error: &error,
                        },
                        root_summary,
                    )?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !decision.matches {
                continue;
            }

            self.ensure_parent_directories(
                run_id,
                &file.remote_path,
                cancellation,
                report,
                root_summary,
                &mut remote_sets.canonical,
            )
            .await?;
            if cancellation.is_cancelled() {
                return Ok(MigrationRootResult::stop(true));
            }

            let operation_id = self.repository.append_operation_started(&OperationRecord {
                run_id,
                operation: "migration_move_legacy_file".to_string(),
                local_path: Some(path_to_stable_string(&file.local_path)),
                remote_path: Some(legacy_path.clone()),
            })?;
            match self
                .client
                .move_resource(&legacy_path, &file.remote_path, false)
                .await
            {
                Ok(()) => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Succeeded,
                        None,
                        None,
                    )?;
                    self.repository.upsert_migration_map(&MigrationMapRecord {
                        root_id: root.id.clone(),
                        legacy_remote_path: legacy_path.clone(),
                        canonical_remote_path: file.remote_path.clone(),
                        status: MigrationMapStatus::Merged,
                        last_error: None,
                    })?;
                    let mut moved = legacy_resource;
                    moved.path = file.remote_path.clone();
                    moved.name = last_remote_segment(&file.remote_path);
                    remote_sets
                        .canonical
                        .insert(file.remote_path.clone(), moved);
                    remote_sets.legacy[legacy_index]
                        .resources
                        .remove(&legacy_path);
                    report
                        .plan
                        .operations
                        .push(MigrationPlanOperation::MoveLegacyResource {
                            from: legacy_path,
                            to: file.remote_path.clone(),
                        });
                    root_summary.moved_resources += 1;
                }
                Err(error) => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Failed,
                        Some(error.kind().as_str()),
                        Some(error.message()),
                    )?;
                    self.record_migration_failure(
                        root,
                        Some(&legacy_path),
                        Some(&file.remote_path),
                        "migration_move_legacy_file",
                        &error,
                    )?;
                    root_summary.failed_files += 1;
                    root_summary.error_summary = Some(error.to_string());
                    if is_stop_error(error.kind()) {
                        return Ok(MigrationRootResult::stop(true));
                    }
                }
            }
        }

        Ok(MigrationRootResult::stop(false))
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_adopt_or_upload(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        live_plan: &mut LiveScanPlan,
        root_summary: &mut MigrationRootSummary,
        cancellation: &CancellationToken,
        available_remote_bytes: Option<u64>,
        report: &mut MigrationRunReport,
        remote_sets: &mut RemoteSets,
        base_summary: &SyncRunSummary,
    ) -> Result<MigrationUploadResult, MigrationError> {
        let mut bytes_uploaded = 0_u64;
        let total_files = live_plan.files.len();
        let started = Instant::now();
        let mut processed_files = 0_usize;
        let mut adoption_batch = AdoptionBatch::default();
        let mut upload_batch = Vec::new();
        let upload_concurrency = self.config.sync.upload_concurrency.max(1);
        let previous_file_states = self
            .repository
            .list_file_states_for_root(&root.id)?
            .into_iter()
            .map(|record| (record.relative_path.clone(), record))
            .collect::<BTreeMap<_, _>>();
        tracing::info!(
            root_id = root.id.as_str(),
            total_files,
            upload_concurrency,
            resumable_file_states = previous_file_states.len(),
            "migration adopt/upload phase start"
        );

        let live_files = live_plan.files.clone();
        for file in &live_files {
            if cancellation.is_cancelled() {
                adoption_batch.flush(self.repository)?;
                return Ok(MigrationUploadResult {
                    should_stop: true,
                    bytes_uploaded,
                });
            }
            if live_plan.is_deleted_relative_path(&file.relative_path) {
                continue;
            }
            if let Some(subtree) = live_plan.register_missing_local_path(
                &file.local_path,
                &file.relative_path,
                &file.remote_path,
            ) {
                adoption_batch.flush(self.repository)?;
                self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                self.persist_root_progress(run_id, base_summary, root_summary)?;
                processed_files += 1;
                continue;
            }
            if !file.local_path.is_file() {
                adoption_batch.flush(self.repository)?;
                let error = MigrationError::Io {
                    path: file.local_path.clone(),
                    source: io::Error::new(
                        io::ErrorKind::NotFound,
                        "local file disappeared after scan",
                    ),
                };
                self.record_file_processing_failure(
                    FileProcessingFailure {
                        run_id,
                        root,
                        file,
                        operation: "migration_adopt_remote_file",
                        operation_remote_path: &file.remote_path,
                        error: &error,
                    },
                    root_summary,
                )?;
                self.persist_root_progress(run_id, base_summary, root_summary)?;
                processed_files += 1;
                continue;
            }

            let remote = remote_sets.canonical.get(&file.remote_path).cloned();
            let remote_is_file = matches!(
                remote.as_ref().map(|resource| &resource.resource_type),
                Some(ResourceType::File)
            );
            if let Some(remote) = remote.filter(|_| remote_is_file) {
                if let Some(previous) = previous_file_states.get(&file.relative_path) {
                    match can_resume_synced_file(previous, file, &remote, self.config).await {
                        Ok(true) => {
                            root_summary.adopted_files += 1;
                            processed_files += 1;
                            log_file_phase_progress(
                                root,
                                root_summary,
                                processed_files,
                                total_files,
                                started,
                            );
                            continue;
                        }
                        Ok(false) => {}
                        Err(error) if per_file_processing_error(&error).is_some() => {
                            adoption_batch.flush(self.repository)?;
                            self.record_file_processing_failure(
                                FileProcessingFailure {
                                    run_id,
                                    root,
                                    file,
                                    operation: "migration_resume_file",
                                    operation_remote_path: &file.remote_path,
                                    error: &error,
                                },
                                root_summary,
                            )?;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            processed_files += 1;
                            log_file_phase_progress(
                                root,
                                root_summary,
                                processed_files,
                                total_files,
                                started,
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                let decision =
                    match remote_matches_local(self.client.as_ref(), file, &remote, self.config)
                        .await
                    {
                        Ok(decision) => decision,
                        Err(error) if per_file_processing_error(&error).is_some() => {
                            adoption_batch.flush(self.repository)?;
                            self.record_file_processing_failure(
                                FileProcessingFailure {
                                    run_id,
                                    root,
                                    file,
                                    operation: "migration_adopt_remote_file",
                                    operation_remote_path: &file.remote_path,
                                    error: &error,
                                },
                                root_summary,
                            )?;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            processed_files += 1;
                            log_file_phase_progress(
                                root,
                                root_summary,
                                processed_files,
                                total_files,
                                started,
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                if decision.matches {
                    adoption_batch.push(
                        OperationRecord {
                            run_id,
                            operation: "migration_adopt_remote_file".to_string(),
                            local_path: Some(path_to_stable_string(&file.local_path)),
                            remote_path: Some(file.remote_path.clone()),
                        },
                        file_state_record(
                            file,
                            run_id,
                            FileStateStatus::Synced,
                            None,
                            remote.md5.clone(),
                        ),
                        remote_resource_record(&root.id, &remote),
                        MigrationMapRecord {
                            root_id: root.id.clone(),
                            legacy_remote_path: file.remote_path.clone(),
                            canonical_remote_path: file.remote_path.clone(),
                            status: MigrationMapStatus::Adopted,
                            last_error: None,
                        },
                    );
                    report
                        .plan
                        .operations
                        .push(MigrationPlanOperation::AdoptRemoteFile {
                            relative_path: file.relative_path.clone(),
                            remote_path: file.remote_path.clone(),
                            method: decision.method,
                        });
                    root_summary.adopted_files += 1;
                    processed_files += 1;
                    if adoption_batch.len() >= ADOPTION_BATCH_SIZE {
                        adoption_batch.flush(self.repository)?;
                    }
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                    log_file_phase_progress(
                        root,
                        root_summary,
                        processed_files,
                        total_files,
                        started,
                    );
                    continue;
                }
            }

            adoption_batch.flush(self.repository)?;
            if let Some(available) = available_remote_bytes {
                let pending_bytes = upload_batch
                    .iter()
                    .map(|candidate: &MigrationUploadCandidate| candidate.file.size_bytes)
                    .fold(0_u64, u64::saturating_add);
                if bytes_uploaded
                    .saturating_add(pending_bytes)
                    .saturating_add(file.size_bytes)
                    > available
                {
                    let message = format!(
                        "planned migration upload size exceeds available Yandex Disk quota {available} bytes"
                    );
                    self.fail_root(
                        root,
                        root_summary,
                        "migration_quota_check",
                        YandexDiskErrorKind::QuotaExceeded.as_str(),
                        &message,
                    )?;
                    return Ok(MigrationUploadResult {
                        should_stop: true,
                        bytes_uploaded,
                    });
                }
            }

            let is_update = remote_is_file;
            let operation = if is_update {
                "migration_update_file"
            } else {
                "migration_upload_file"
            };
            let operation_id = self.repository.append_operation_started(&OperationRecord {
                run_id,
                operation: operation.to_string(),
                local_path: Some(path_to_stable_string(&file.local_path)),
                remote_path: Some(file.remote_path.clone()),
            })?;
            report
                .plan
                .operations
                .push(MigrationPlanOperation::UploadFile {
                    relative_path: file.relative_path.clone(),
                    remote_path: file.remote_path.clone(),
                    is_update,
                });
            upload_batch.push(MigrationUploadCandidate {
                operation_id,
                file: file.clone(),
                is_update,
            });
            if upload_batch.len() >= upload_concurrency {
                let result = self
                    .apply_migration_upload_batch(
                        run_id,
                        root,
                        root_summary,
                        live_plan,
                        cancellation,
                        remote_sets,
                        &mut upload_batch,
                        &mut processed_files,
                        total_files,
                        started,
                        base_summary,
                    )
                    .await?;
                bytes_uploaded = bytes_uploaded.saturating_add(result.bytes_uploaded);
                if result.should_stop {
                    return Ok(MigrationUploadResult {
                        should_stop: true,
                        bytes_uploaded,
                    });
                }
            }
        }

        adoption_batch.flush(self.repository)?;
        if !upload_batch.is_empty() {
            let result = self
                .apply_migration_upload_batch(
                    run_id,
                    root,
                    root_summary,
                    live_plan,
                    cancellation,
                    remote_sets,
                    &mut upload_batch,
                    &mut processed_files,
                    total_files,
                    started,
                    base_summary,
                )
                .await?;
            bytes_uploaded = bytes_uploaded.saturating_add(result.bytes_uploaded);
            if result.should_stop {
                return Ok(MigrationUploadResult {
                    should_stop: true,
                    bytes_uploaded,
                });
            }
        }
        tracing::info!(
            root_id = root.id.as_str(),
            total_files,
            adopted = root_summary.adopted_files,
            uploaded = root_summary.uploaded_files,
            updated = root_summary.updated_files,
            failed = root_summary.failed_files,
            elapsed_ms = started.elapsed().as_millis(),
            "migration adopt/upload phase ok"
        );
        Ok(MigrationUploadResult {
            should_stop: false,
            bytes_uploaded,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_migration_upload_batch(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        root_summary: &mut MigrationRootSummary,
        live_plan: &mut LiveScanPlan,
        cancellation: &CancellationToken,
        remote_sets: &mut RemoteSets,
        upload_batch: &mut Vec<MigrationUploadCandidate>,
        processed_files: &mut usize,
        total_files: usize,
        started: Instant,
        base_summary: &SyncRunSummary,
    ) -> Result<MigrationUploadResult, MigrationError> {
        let mut bytes_uploaded = 0_u64;
        let mut should_stop = false;
        while !upload_batch.is_empty() {
            let mut pending = VecDeque::from(std::mem::take(upload_batch));
            let scheduled = take_migration_upload_batch(
                &mut pending,
                self.config.sync.upload_concurrency.max(1),
                self.config.sync.large_file_name_size_only_min_bytes,
            );
            *upload_batch = pending.into();

            let mut tasks = JoinSet::new();
            for candidate in scheduled {
                if live_plan.is_deleted_relative_path(&candidate.file.relative_path) {
                    continue;
                }
                if let Some(subtree) = live_plan.register_missing_local_path(
                    &candidate.file.local_path,
                    &candidate.file.relative_path,
                    &candidate.file.remote_path,
                ) {
                    self.repository.finish_operation(
                        candidate.operation_id,
                        StateOperationStatus::Cancelled,
                        Some("local_subtree_deleted"),
                        Some("local subtree deleted during migration run"),
                    )?;
                    self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                    *processed_files += 1;
                    continue;
                }
                let client = Arc::clone(&self.client);
                let cancellation = cancellation.clone();
                tasks.spawn(
                    async move { migration_upload_task(client, candidate, cancellation).await },
                );
            }

            while let Some(outcome) = tasks.join_next().await {
                let outcome = outcome.map_err(|error| {
                    MigrationError::YandexDisk(YandexDiskError::new(
                        YandexDiskErrorKind::Critical,
                        format!("migration upload task failed: {error}"),
                    ))
                })?;
                let operation = if outcome.is_update {
                    "migration_update_file"
                } else {
                    "migration_upload_file"
                };
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
                            outcome.file.fingerprint.local_md5.clone(),
                        ))?;
                        let mut remote = ResourceMetadata::file(
                            &outcome.file.remote_path,
                            outcome.file.size_bytes,
                        );
                        remote.md5 = outcome.file.fingerprint.local_md5.clone();
                        remote_sets
                            .canonical
                            .insert(outcome.file.remote_path.clone(), remote.clone());
                        self.repository
                            .upsert_remote_resource(&remote_resource_record(&root.id, &remote))?;
                        self.repository.upsert_migration_map(&MigrationMapRecord {
                            root_id: root.id.clone(),
                            legacy_remote_path: outcome.file.remote_path.clone(),
                            canonical_remote_path: outcome.file.remote_path.clone(),
                            status: MigrationMapStatus::Uploaded,
                            last_error: None,
                        })?;
                        if outcome.is_update {
                            root_summary.updated_files += 1;
                        } else {
                            root_summary.uploaded_files += 1;
                        }
                        root_summary.bytes_uploaded += u64_to_i64(outcome.file.size_bytes);
                        bytes_uploaded = bytes_uploaded.saturating_add(outcome.file.size_bytes);
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
                                Some("local subtree deleted during migration run"),
                            )?;
                            self.record_deleted_subtree(run_id, &subtree, root_summary)?;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            *processed_files += 1;
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
                                root_id: root.id.clone(),
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
                                None,
                            ))?;
                            self.repository.upsert_migration_map(&MigrationMapRecord {
                                root_id: root.id.clone(),
                                legacy_remote_path: outcome.file.remote_path.clone(),
                                canonical_remote_path: outcome.file.remote_path.clone(),
                                status: MigrationMapStatus::Skipped,
                                last_error: Some(failure.error_message.clone()),
                            })?;
                            root_summary.skipped_files += 1;
                            self.persist_root_progress(run_id, base_summary, root_summary)?;
                            *processed_files += 1;
                            continue;
                        }
                        self.repository.finish_operation(
                            outcome.operation_id,
                            StateOperationStatus::Failed,
                            Some(&failure.error_kind),
                            Some(&failure.error_message),
                        )?;
                        self.repository.mark_failed(&FailedItemRecord {
                            root_id: root.id.clone(),
                            local_path: None,
                            remote_path: Some(outcome.file.remote_path.clone()),
                            operation: operation.to_string(),
                            error_kind: failure.error_kind.clone(),
                            error_message: failure.error_message.clone(),
                            retry_count: 0,
                            next_retry_at_utc: None,
                        })?;
                        self.repository.upsert_migration_map(&MigrationMapRecord {
                            root_id: root.id.clone(),
                            legacy_remote_path: outcome.file.remote_path.clone(),
                            canonical_remote_path: outcome.file.remote_path.clone(),
                            status: MigrationMapStatus::Failed,
                            last_error: Some(failure.error_message.clone()),
                        })?;
                        root_summary.failed_files += 1;
                        root_summary.error_summary = Some(failure.error_message);
                        should_stop |= failure.should_stop;
                    }
                }
                *processed_files += 1;
                self.persist_root_progress(run_id, base_summary, root_summary)?;
                log_file_phase_progress(root, root_summary, *processed_files, total_files, started);
            }
        }

        Ok(MigrationUploadResult {
            should_stop,
            bytes_uploaded,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_deletes(
        &self,
        run_id: i64,
        root: &SyncRootConfig,
        live_plan: &LiveScanPlan,
        root_summary: &mut MigrationRootSummary,
        cancellation: &CancellationToken,
        report: &mut MigrationRunReport,
        remote_sets: &RemoteSets,
        base_summary: &SyncRunSummary,
    ) -> Result<bool, MigrationError> {
        let mut deletes = collect_coalesced_delete_operations(
            root,
            &root_summary.canonical_remote_path,
            live_plan,
            &remote_sets.canonical,
        )
        .into_iter()
        .map(|delete| DeleteCandidate {
            remote_path: delete.remote_path.clone(),
            relative_path: delete.relative_path,
            resource_type: delete.resource_type,
            canonical_path: delete.remote_path,
        })
        .collect::<Vec<_>>();
        for legacy in &remote_sets.legacy {
            deletes.extend(collect_legacy_delete_candidates(
                legacy,
                &root_summary.canonical_remote_path,
            ));
        }
        deletes.sort_by_key(|delete| {
            (
                std::cmp::Reverse(delete.remote_path.matches('/').count()),
                delete.remote_path.clone(),
            )
        });

        for delete in deletes {
            if cancellation.is_cancelled() {
                return Ok(true);
            }
            let operation_id = self.repository.append_operation_started(&OperationRecord {
                run_id,
                operation: "migration_delete_remote_extra".to_string(),
                local_path: None,
                remote_path: Some(delete.remote_path.clone()),
            })?;
            if remote_sets.from_cache {
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
                            .delete_remote_resource_subtree(&root.id, &delete.remote_path)?;
                        continue;
                    }
                    Err(error) => {
                        self.repository.finish_operation(
                            operation_id,
                            StateOperationStatus::Failed,
                            Some(error.kind().as_str()),
                            Some(error.message()),
                        )?;
                        self.record_migration_failure(
                            root,
                            Some(&delete.remote_path),
                            Some(&delete.canonical_path),
                            "migration_delete_remote_metadata_check",
                            &error,
                        )?;
                        root_summary.failed_files += 1;
                        root_summary.error_summary = Some(error.to_string());
                        self.persist_root_progress(run_id, base_summary, root_summary)?;
                        if is_stop_error(error.kind()) {
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
                            &root.id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                        ResourceType::Directory => self.repository.mark_directory_deleted(
                            &root.id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                    }
                    self.repository
                        .delete_remote_resource_subtree(&root.id, &delete.remote_path)?;
                    self.repository.upsert_migration_map(&MigrationMapRecord {
                        root_id: root.id.clone(),
                        legacy_remote_path: delete.remote_path.clone(),
                        canonical_remote_path: delete.canonical_path.clone(),
                        status: MigrationMapStatus::Deleted,
                        last_error: None,
                    })?;
                    report
                        .plan
                        .operations
                        .push(MigrationPlanOperation::DeleteRemote {
                            remote_path: delete.remote_path,
                            resource_type: delete.resource_type,
                        });
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
                            &root.id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                        ResourceType::Directory => self.repository.mark_directory_deleted(
                            &root.id,
                            &delete.relative_path,
                            &delete.remote_path,
                            Some(run_id),
                        )?,
                    }
                    self.repository
                        .delete_remote_resource_subtree(&root.id, &delete.remote_path)?;
                    self.repository.upsert_migration_map(&MigrationMapRecord {
                        root_id: root.id.clone(),
                        legacy_remote_path: delete.remote_path,
                        canonical_remote_path: delete.canonical_path,
                        status: MigrationMapStatus::Deleted,
                        last_error: None,
                    })?;
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                }
                Err(error) => {
                    self.repository.finish_operation(
                        operation_id,
                        StateOperationStatus::Failed,
                        Some(error.kind().as_str()),
                        Some(error.message()),
                    )?;
                    self.record_migration_failure(
                        root,
                        Some(&delete.remote_path),
                        Some(&delete.canonical_path),
                        "migration_delete_remote_extra",
                        &error,
                    )?;
                    root_summary.failed_files += 1;
                    root_summary.error_summary = Some(error.to_string());
                    self.persist_root_progress(run_id, base_summary, root_summary)?;
                    if is_stop_error(error.kind()) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    async fn ensure_parent_directories(
        &self,
        run_id: i64,
        remote_path: &str,
        cancellation: &CancellationToken,
        report: &mut MigrationRunReport,
        root_summary: &mut MigrationRootSummary,
        inventory: &mut BTreeMap<String, ResourceMetadata>,
    ) -> Result<(), MigrationError> {
        for parent in parent_remote_paths(remote_path) {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if matches!(
                inventory
                    .get(&parent)
                    .map(|resource| &resource.resource_type),
                Some(ResourceType::Directory)
            ) {
                continue;
            }
            self.create_directory_operation(run_id, &parent, report, root_summary)
                .await?;
            inventory.insert(parent.clone(), ResourceMetadata::directory(&parent));
        }
        Ok(())
    }

    async fn ensure_directory(
        &self,
        run_id: i64,
        remote_path: &str,
        cancellation: &CancellationToken,
        report: &mut MigrationRunReport,
        root_summary: &mut MigrationRootSummary,
        inventory: &mut BTreeMap<String, ResourceMetadata>,
    ) -> Result<(), MigrationError> {
        if matches!(
            inventory
                .get(remote_path)
                .map(|resource| &resource.resource_type),
            Some(ResourceType::Directory)
        ) {
            return Ok(());
        }

        self.ensure_parent_directories(
            run_id,
            remote_path,
            cancellation,
            report,
            root_summary,
            inventory,
        )
        .await?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        self.create_directory_operation(run_id, remote_path, report, root_summary)
            .await?;
        inventory.insert(
            remote_path.to_string(),
            ResourceMetadata::directory(remote_path),
        );
        Ok(())
    }

    async fn create_directory_operation(
        &self,
        run_id: i64,
        remote_path: &str,
        report: &mut MigrationRunReport,
        root_summary: &mut MigrationRootSummary,
    ) -> Result<(), MigrationError> {
        let operation_id = self.repository.append_operation_started(&OperationRecord {
            run_id,
            operation: "migration_create_directory".to_string(),
            local_path: None,
            remote_path: Some(remote_path.to_string()),
        })?;
        match self.client.create_directory(remote_path).await {
            Ok(()) => {
                self.repository.finish_operation(
                    operation_id,
                    StateOperationStatus::Succeeded,
                    None,
                    None,
                )?;
                report
                    .plan
                    .operations
                    .push(MigrationPlanOperation::CreateCanonicalDirectory {
                        remote_path: remote_path.to_string(),
                    });
                Ok(())
            }
            Err(error) => {
                self.repository.finish_operation(
                    operation_id,
                    StateOperationStatus::Failed,
                    Some(error.kind().as_str()),
                    Some(error.message()),
                )?;
                root_summary.failed_files += 1;
                root_summary.error_summary = Some(error.to_string());
                Err(MigrationError::YandexDisk(error))
            }
        }
    }

    fn fail_root(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut MigrationRootSummary,
        operation: &str,
        error_kind: &str,
        error_message: &str,
    ) -> Result<(), MigrationError> {
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
        root_summary: &mut MigrationRootSummary,
        operation: &str,
        error_kind: &str,
        error_message: &str,
    ) -> Result<(), MigrationError> {
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
        root_summary: &MigrationRootSummary,
    ) -> Result<(), MigrationError> {
        let mut summary = base_summary.clone();
        add_root_summary(&mut summary, root_summary);
        self.repository.update_run_progress(run_id, &summary)?;
        Ok(())
    }

    fn record_deleted_subtrees(
        &self,
        run_id: i64,
        live_plan: &mut LiveScanPlan,
        root_summary: &mut MigrationRootSummary,
    ) -> Result<(), MigrationError> {
        let subtrees = live_plan.deleted_subtrees.clone();
        for subtree in subtrees {
            self.record_deleted_subtree(run_id, &subtree, root_summary)?;
        }
        Ok(())
    }

    fn record_deleted_subtree(
        &self,
        run_id: i64,
        subtree: &crate::DeletedLocalSubtree,
        root_summary: &mut MigrationRootSummary,
    ) -> Result<(), MigrationError> {
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
                "local subtree deleted during migration preparation or execution".to_string(),
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

    fn record_migration_failure(
        &self,
        root: &SyncRootConfig,
        legacy_remote_path: Option<&str>,
        canonical_remote_path: Option<&str>,
        operation: &str,
        error: &YandexDiskError,
    ) -> Result<(), MigrationError> {
        let legacy_remote_path_value = legacy_remote_path
            .or(canonical_remote_path)
            .unwrap_or("")
            .to_string();
        let canonical_remote_path_value = canonical_remote_path
            .unwrap_or(legacy_remote_path_value.as_str())
            .to_string();
        self.repository.mark_failed(&FailedItemRecord {
            root_id: root.id.clone(),
            local_path: None,
            remote_path: Some(canonical_remote_path_value.clone()),
            operation: operation.to_string(),
            error_kind: error.kind().as_str().to_string(),
            error_message: error.message().to_string(),
            retry_count: 0,
            next_retry_at_utc: None,
        })?;
        self.repository.upsert_migration_map(&MigrationMapRecord {
            root_id: root.id.clone(),
            legacy_remote_path: legacy_remote_path_value,
            canonical_remote_path: canonical_remote_path_value,
            status: MigrationMapStatus::Failed,
            last_error: Some(error.message().to_string()),
        })?;
        Ok(())
    }

    fn record_file_processing_failure(
        &self,
        failure: FileProcessingFailure<'_>,
        root_summary: &mut MigrationRootSummary,
    ) -> Result<(), MigrationError> {
        let Some((error_kind, error_message)) = per_file_processing_error(failure.error) else {
            return Err(MigrationError::YandexDisk(YandexDiskError::new(
                YandexDiskErrorKind::Critical,
                format!(
                    "non per-file error passed to file failure recorder: {}",
                    failure.error
                ),
            )));
        };

        let operation_id = self.repository.append_operation_started(&OperationRecord {
            run_id: failure.run_id,
            operation: failure.operation.to_string(),
            local_path: Some(path_to_stable_string(&failure.file.local_path)),
            remote_path: Some(failure.operation_remote_path.to_string()),
        })?;
        if let Some((reason_code, reason_details)) = skippable_local_processing_error(failure.error)
        {
            self.repository.finish_operation(
                operation_id,
                StateOperationStatus::Cancelled,
                Some(&reason_code),
                Some(&reason_details),
            )?;
            self.repository.mark_skipped(&SkippedItemRecord {
                root_id: failure.root.id.clone(),
                local_path: path_to_stable_string(&failure.file.local_path),
                relative_path: Some(failure.file.relative_path.clone()),
                remote_path: Some(failure.file.remote_path.clone()),
                reason_code: reason_code.clone(),
                reason_details: Some(reason_details.clone()),
                size_bytes: Some(u64_to_i64(failure.file.size_bytes)),
            })?;
            self.repository.upsert_file_state(&file_state_record(
                failure.file,
                failure.run_id,
                FileStateStatus::Skipped,
                Some(reason_details.clone()),
                None,
            ))?;
            self.repository.upsert_migration_map(&MigrationMapRecord {
                root_id: failure.root.id.clone(),
                legacy_remote_path: failure.operation_remote_path.to_string(),
                canonical_remote_path: failure.file.remote_path.clone(),
                status: MigrationMapStatus::Skipped,
                last_error: Some(reason_details),
            })?;
            root_summary.skipped_files += 1;
            return Ok(());
        }

        self.repository.finish_operation(
            operation_id,
            StateOperationStatus::Failed,
            Some(&error_kind),
            Some(&error_message),
        )?;
        self.repository.mark_failed(&FailedItemRecord {
            root_id: failure.root.id.clone(),
            local_path: Some(path_to_stable_string(&failure.file.local_path)),
            remote_path: Some(failure.file.remote_path.clone()),
            operation: failure.operation.to_string(),
            error_kind: error_kind.clone(),
            error_message: error_message.clone(),
            retry_count: 0,
            next_retry_at_utc: None,
        })?;
        self.repository.upsert_file_state(&file_state_record(
            failure.file,
            failure.run_id,
            FileStateStatus::Failed,
            Some(error_message.clone()),
            None,
        ))?;
        self.repository.upsert_migration_map(&MigrationMapRecord {
            root_id: failure.root.id.clone(),
            legacy_remote_path: failure.operation_remote_path.to_string(),
            canonical_remote_path: failure.file.remote_path.clone(),
            status: MigrationMapStatus::Failed,
            last_error: Some(error_message.clone()),
        })?;
        root_summary.failed_files += 1;
        root_summary.error_summary = Some(error_message);
        Ok(())
    }

    fn finish_root_cancelled_or_failed(
        &self,
        root: &SyncRootConfig,
        root_summary: &mut MigrationRootSummary,
        cancellation: &CancellationToken,
    ) -> Result<(), MigrationError> {
        if cancellation.is_cancelled() {
            root_summary.status = ROOT_STATUS_CANCELLED.to_string();
            root_summary.error_summary = Some("migration run cancelled".to_string());
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

#[derive(Debug, Clone)]
struct RemoteSets {
    canonical: BTreeMap<String, ResourceMetadata>,
    legacy: Vec<LegacyInventory>,
    from_cache: bool,
}

impl RemoteSets {
    fn find_legacy_file(&self, relative_path: &str) -> Option<(usize, String, ResourceMetadata)> {
        for (index, legacy) in self.legacy.iter().enumerate() {
            let remote_path = remote_path_for_relative(&legacy.root_path, relative_path);
            if let Some(resource) = legacy.resources.get(&remote_path) {
                if resource.resource_type == ResourceType::File {
                    return Some((index, remote_path, resource.clone()));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
struct LegacyInventory {
    root_path: String,
    resources: BTreeMap<String, ResourceMetadata>,
}

fn split_cached_remote_sets(
    root: &SyncRootConfig,
    canonical_remote_path: &str,
    records: &[RemoteResourceRecord],
    from_cache: bool,
) -> RemoteSets {
    let mut canonical = BTreeMap::new();
    let mut legacy = root
        .legacy_remote_paths
        .iter()
        .cloned()
        .map(|root_path| LegacyInventory {
            root_path,
            resources: BTreeMap::new(),
        })
        .collect::<Vec<_>>();

    for record in records {
        let Some(resource) =
            cached_resource_to_metadata(canonical_remote_path, record).or_else(|| {
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
            })
        else {
            continue;
        };

        if resource.path == canonical_remote_path
            || resource
                .path
                .starts_with(&format!("{}/", canonical_remote_path.trim_end_matches('/')))
        {
            canonical.insert(resource.path.clone(), resource);
            continue;
        }

        for inventory in &mut legacy {
            if resource.path == inventory.root_path
                || resource
                    .path
                    .starts_with(&format!("{}/", inventory.root_path.trim_end_matches('/')))
            {
                inventory
                    .resources
                    .insert(resource.path.clone(), resource.clone());
                break;
            }
        }
    }

    RemoteSets {
        canonical,
        legacy,
        from_cache,
    }
}

#[derive(Debug, Clone, Copy)]
struct MigrationRootResult {
    should_stop: bool,
    bytes_uploaded: Option<u64>,
}

impl MigrationRootResult {
    const fn stop(should_stop: bool) -> Self {
        Self {
            should_stop,
            bytes_uploaded: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MigrationUploadResult {
    should_stop: bool,
    bytes_uploaded: u64,
}

#[derive(Debug)]
struct MigrationUploadCandidate {
    operation_id: i64,
    file: LocalFileEntry,
    is_update: bool,
}

#[derive(Debug)]
struct MigrationUploadTaskOutcome {
    operation_id: i64,
    file: LocalFileEntry,
    is_update: bool,
    result: Result<(), MigrationUploadTaskFailure>,
}

#[derive(Debug)]
struct MigrationUploadTaskFailure {
    error_kind: String,
    error_message: String,
    should_stop: bool,
    skip_reason_code: Option<String>,
}

#[derive(Debug, Default)]
struct AdoptionBatch {
    operations: Vec<OperationRecord>,
    file_states: Vec<FileStateRecord>,
    remote_resources: Vec<RemoteResourceRecord>,
    migration_maps: Vec<MigrationMapRecord>,
}

impl AdoptionBatch {
    fn len(&self) -> usize {
        self.file_states.len()
    }

    fn is_empty(&self) -> bool {
        self.file_states.is_empty()
    }

    fn push(
        &mut self,
        operation: OperationRecord,
        file_state: FileStateRecord,
        remote_resource: RemoteResourceRecord,
        migration_map: MigrationMapRecord,
    ) {
        self.operations.push(operation);
        self.file_states.push(file_state);
        self.remote_resources.push(remote_resource);
        self.migration_maps.push(migration_map);
    }

    fn flush(&mut self, repository: &StateRepository) -> Result<(), MigrationError> {
        if self.is_empty() {
            return Ok(());
        }

        repository.upsert_file_states(&self.file_states)?;
        repository.upsert_remote_resources(&self.remote_resources)?;
        repository.upsert_migration_maps(&self.migration_maps)?;
        repository.append_operations_succeeded(&self.operations)?;
        self.operations.clear();
        self.file_states.clear();
        self.remote_resources.clear();
        self.migration_maps.clear();
        Ok(())
    }
}

async fn migration_upload_task(
    client: Arc<dyn YandexDiskClient>,
    candidate: MigrationUploadCandidate,
    cancellation: CancellationToken,
) -> MigrationUploadTaskOutcome {
    if cancellation.is_cancelled() {
        return MigrationUploadTaskOutcome {
            operation_id: candidate.operation_id,
            file: candidate.file,
            is_update: candidate.is_update,
            result: Err(MigrationUploadTaskFailure {
                error_kind: "cancelled".to_string(),
                error_message: "migration run cancelled".to_string(),
                should_stop: true,
                skip_reason_code: None,
            }),
        };
    }

    let result = client
        .upload_source(
            &candidate.file.remote_path,
            UploadSource::File(candidate.file.upload_source.clone()),
            true,
        )
        .await
        .map_err(migration_upload_failure);

    MigrationUploadTaskOutcome {
        operation_id: candidate.operation_id,
        file: candidate.file,
        is_update: candidate.is_update,
        result,
    }
}

fn migration_upload_failure(error: YandexDiskError) -> MigrationUploadTaskFailure {
    let skip_reason_code = if error.is_persistent_uploader_conflict() {
        Some("remote_uploader_content_conflict".to_string())
    } else if error.is_local_upload_source_unavailable() {
        Some("local_file_unavailable".to_string())
    } else {
        None
    };
    MigrationUploadTaskFailure {
        error_kind: error.kind().as_str().to_string(),
        error_message: error.message().to_string(),
        should_stop: is_stop_error(error.kind()),
        skip_reason_code,
    }
}

fn log_file_phase_progress(
    root: &SyncRootConfig,
    root_summary: &MigrationRootSummary,
    processed_files: usize,
    total_files: usize,
    started: Instant,
) {
    if processed_files == total_files
        || processed_files.is_multiple_of(FILE_PHASE_PROGRESS_INTERVAL)
    {
        tracing::info!(
            root_id = root.id.as_str(),
            processed_files,
            total_files,
            adopted = root_summary.adopted_files,
            uploaded = root_summary.uploaded_files,
            updated = root_summary.updated_files,
            failed = root_summary.failed_files,
            elapsed_ms = started.elapsed().as_millis(),
            "migration adopt/upload phase progress"
        );
    }
}

#[derive(Debug, Clone)]
struct AdoptionDecision {
    matches: bool,
    method: String,
}

#[derive(Debug, Clone)]
struct DeleteCandidate {
    remote_path: String,
    relative_path: String,
    resource_type: ResourceType,
    canonical_path: String,
}

fn collect_legacy_delete_candidates(
    legacy: &LegacyInventory,
    canonical_root: &str,
) -> Vec<DeleteCandidate> {
    let mut suppressed_prefixes = Vec::<String>::new();
    let mut candidates = legacy.resources.values().cloned().collect::<Vec<_>>();
    candidates.sort_by_key(|resource| {
        (
            resource.resource_type != ResourceType::Directory,
            resource.path.matches('/').count(),
            resource.path.clone(),
        )
    });

    let mut deletes = Vec::new();
    for resource in candidates {
        let relative_path =
            match remote_delete_candidate_relative_path(&legacy.root_path, &resource.path) {
                Ok(relative_path) => relative_path,
                Err(reason) => {
                    tracing::warn!(
                        legacy_root = legacy.root_path,
                        canonical_root,
                        remote_path = resource.path,
                        reason,
                        "skipped invalid migration legacy delete candidate"
                    );
                    continue;
                }
            };
        if suppressed_prefixes.iter().any(|prefix| {
            resource.path == *prefix
                || resource
                    .path
                    .strip_prefix(prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }) {
            continue;
        }
        if resource.resource_type == ResourceType::Directory {
            suppressed_prefixes.push(resource.path.clone());
        }
        deletes.push(DeleteCandidate {
            remote_path: resource.path.clone(),
            relative_path,
            resource_type: resource.resource_type,
            canonical_path: legacy_to_canonical_path(
                &legacy.root_path,
                canonical_root,
                &resource.path,
            ),
        });
    }
    deletes
}

fn take_migration_upload_batch(
    pending: &mut VecDeque<MigrationUploadCandidate>,
    limit: usize,
    large_threshold: u64,
) -> Vec<MigrationUploadCandidate> {
    let mut selected = Vec::new();
    let mut deferred = VecDeque::new();
    let mut large_used = false;

    while let Some(candidate) = pending.pop_front() {
        let is_large = candidate.file.size_bytes >= large_threshold;
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

async fn remote_inventory(
    client: &dyn YandexDiskClient,
    remote_path: &str,
    options: ListRecursiveOptions,
) -> Result<BTreeMap<String, ResourceMetadata>, YandexDiskError> {
    let mut resources = BTreeMap::new();
    tracing::info!(remote_path, "migration remote metadata start");
    match client.metadata(remote_path).await {
        Ok(root) => {
            resources.insert(root.path.clone(), root);
            tracing::info!(remote_path, "migration remote metadata ok");
        }
        Err(error) if error.kind() == YandexDiskErrorKind::NotFound => return Ok(resources),
        Err(error) => return Err(error),
    }

    tracing::info!(remote_path, "migration remote recursive list start");
    match client
        .list_recursive_with_options(remote_path, options)
        .await
    {
        Ok(listed) => {
            tracing::info!(
                remote_path,
                resource_count = listed.len(),
                "migration remote recursive list ok"
            );
            for resource in listed {
                resources.insert(resource.path.clone(), resource);
            }
            Ok(resources)
        }
        Err(error) if error.kind() == YandexDiskErrorKind::NotFound => Ok(resources),
        Err(error) => Err(error),
    }
}

async fn remote_matches_local(
    client: &dyn YandexDiskClient,
    file: &LocalFileEntry,
    remote: &ResourceMetadata,
    config: &AppConfig,
) -> Result<AdoptionDecision, MigrationError> {
    if remote.resource_type != ResourceType::File || remote.size_bytes != Some(file.size_bytes) {
        return Ok(AdoptionDecision {
            matches: false,
            method: "type_or_size_mismatch".to_string(),
        });
    }

    if file.size_bytes <= config.sync.full_hash_max_bytes {
        if let Some(remote_md5) = &remote.md5 {
            return Ok(AdoptionDecision {
                matches: file
                    .fingerprint
                    .local_md5
                    .as_deref()
                    .is_some_and(|local_md5| local_md5.eq_ignore_ascii_case(remote_md5)),
                method: "md5".to_string(),
            });
        }

        let local = tokio::fs::read(&file.upload_source)
            .await
            .map_err(|source| MigrationError::Io {
                path: file.upload_source.clone(),
                source,
            })?;
        let remote = client.download(&remote.path).await?;
        return Ok(AdoptionDecision {
            matches: local == remote,
            method: "download_full_compare".to_string(),
        });
    }

    Ok(AdoptionDecision {
        matches: true,
        method: "path_name_size".to_string(),
    })
}

async fn can_resume_synced_file(
    previous: &FileStateRecord,
    file: &LocalFileEntry,
    remote: &ResourceMetadata,
    config: &AppConfig,
) -> Result<bool, MigrationError> {
    if previous.status != FileStateStatus::Synced {
        return Ok(false);
    }
    if previous.remote_path != file.remote_path
        || previous.normalized_remote_path != file.normalized_remote_path
        || previous.size_bytes != u64_to_i64(file.size_bytes)
        || previous.mtime_ns != file.mtime_ns
        || previous.fingerprint_kind != file.fingerprint.kind.as_str()
        || previous.fingerprint_value.as_deref() != Some(file.fingerprint.value.as_str())
        || previous.full_sha256 != file.fingerprint.full_sha256
    {
        return Ok(false);
    }
    if remote.resource_type != ResourceType::File || remote.size_bytes != Some(file.size_bytes) {
        return Ok(false);
    }

    if file.size_bytes <= config.sync.full_hash_max_bytes {
        if let Some(remote_md5) = &remote.md5 {
            if let Some(local_md5) = file.fingerprint.local_md5.as_deref() {
                return Ok(local_md5.eq_ignore_ascii_case(remote_md5));
            }
            if let Some(previous_remote_md5) = previous.remote_md5.as_deref() {
                return Ok(previous_remote_md5.eq_ignore_ascii_case(remote_md5));
            }
            return Ok(false);
        }
        return Ok(false);
    }

    Ok(true)
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

fn directory_state_record(directory: &LocalDirectoryEntry, run_id: i64) -> DirectoryStateRecord {
    DirectoryStateRecord {
        root_id: directory.root_id.clone(),
        local_path: path_to_stable_string(&directory.local_path),
        relative_path: directory.relative_path.clone(),
        remote_path: directory.remote_path.clone(),
        normalized_remote_path: directory.normalized_remote_path.clone(),
        last_run_id: Some(run_id),
        status: DirectoryStateStatus::Synced,
        last_error: None,
    }
}

fn file_state_record(
    file: &LocalFileEntry,
    run_id: i64,
    status: FileStateStatus,
    last_error: Option<String>,
    remote_md5: Option<String>,
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
        remote_md5,
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

fn parent_remote_paths(path: &str) -> Vec<String> {
    let Some(body) = path.strip_prefix("disk:/") else {
        return Vec::new();
    };
    let segments: Vec<_> = body
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.len() <= 1 {
        return Vec::new();
    }

    (1..segments.len())
        .map(|index| format!("disk:/{}", segments[..index].join("/")))
        .collect()
}

fn remote_path_for_relative(root: &str, relative_path: &str) -> String {
    if relative_path.is_empty() {
        root.to_string()
    } else {
        format!("{}/{}", root.trim_end_matches('/'), relative_path)
    }
}

fn legacy_remote_prefix(legacy_root: &str, relative_prefix: &str) -> String {
    remote_path_for_relative(legacy_root, relative_prefix)
}

fn legacy_to_canonical_path(legacy_root: &str, canonical_root: &str, legacy_path: &str) -> String {
    let relative = relative_remote_path(legacy_root, legacy_path);
    remote_path_for_relative(canonical_root, &relative)
}

fn relative_remote_path(root: &str, remote_path: &str) -> String {
    remote_path
        .strip_prefix(root.trim_end_matches('/'))
        .unwrap_or(remote_path)
        .trim_start_matches('/')
        .to_string()
}

fn last_remote_segment(path: &str) -> Option<String> {
    path.rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
}

fn path_to_stable_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
}

fn system_time_to_ns(value: std::time::SystemTime) -> Option<i64> {
    value
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn is_stop_error(kind: YandexDiskErrorKind) -> bool {
    matches!(
        kind,
        YandexDiskErrorKind::AuthUnavailable
            | YandexDiskErrorKind::QuotaExceeded
            | YandexDiskErrorKind::Critical
    )
}

fn per_file_processing_error(error: &MigrationError) -> Option<(String, String)> {
    match error {
        MigrationError::Io { .. } => Some(("local_io".to_string(), error.to_string())),
        MigrationError::YandexDisk(error) if !is_stop_error(error.kind()) => Some((
            error.kind().as_str().to_string(),
            error.message().to_string(),
        )),
        _ => None,
    }
}

fn skippable_local_processing_error(error: &MigrationError) -> Option<(String, String)> {
    match error {
        MigrationError::Io { source, .. } if is_skippable_local_io_error(source) => {
            Some(("local_file_unavailable".to_string(), error.to_string()))
        }
        MigrationError::YandexDisk(error) if error.is_local_upload_source_unavailable() => Some((
            "local_file_unavailable".to_string(),
            error.message().to_string(),
        )),
        _ => None,
    }
}

fn is_skippable_local_io_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || matches!(error.raw_os_error(), Some(32 | 33))
}

fn status_from_summary(summary: &SyncRunSummary, roots: &[MigrationRootSummary]) -> SyncRunStatus {
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

fn add_root_summary(total: &mut SyncRunSummary, root: &MigrationRootSummary) {
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

fn ensure_valid_config(config: &AppConfig) -> Result<(), MigrationError> {
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
    Err(MigrationError::ConfigInvalid(errors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use yds_core::config::default_config;
    use yds_state::models::SyncRunStatus;
    use yds_yandex_disk::{
        auth::{KeyringTokenStore, TokenStore},
        models::{DiskInfo, QuotaInfo},
        HttpYandexDiskClient, MockOperationKind, MockYandexDiskClient, RetryPolicy,
    };

    use crate::{SyncEngine, SyncRunOptions};

    #[tokio::test]
    async fn legacy_only_root_is_moved_and_adopted_without_upload() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();
        client.insert_file("disk:/legacy/a.txt", b"one").unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.roots[0].moved_resources, 1);
        assert_eq!(report.roots[0].adopted_files, 1);
        assert_eq!(report.summary.uploaded_files, 0);
        assert!(fixture
            .repository
            .get_file_state("root", "a.txt")
            .unwrap()
            .is_some());
        let log = client.operation_log().unwrap();
        assert!(log.iter().any(|operation| {
            operation.kind == MockOperationKind::Move
                && operation.remote_path == "disk:/legacy"
                && operation.secondary_path.as_deref() == Some("disk:/canonical")
        }));
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
    }

    #[tokio::test]
    async fn canonical_only_root_is_adopted_without_upload() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();
        client.insert_file("disk:/canonical/a.txt", b"one").unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.roots[0].moved_resources, 0);
        assert_eq!(report.roots[0].adopted_files, 1);
        assert_eq!(report.summary.uploaded_files, 0);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
    }

    #[tokio::test]
    async fn canonical_and_legacy_roots_are_merged_and_legacy_extras_deleted() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        fixture.write_file("nested/b.txt", "two");
        let client = MockYandexDiskClient::new();
        client.insert_file("disk:/canonical/a.txt", b"one").unwrap();
        client
            .insert_file("disk:/legacy/nested/b.txt", b"two")
            .unwrap();
        client.insert_file("disk:/legacy/old.txt", b"old").unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.roots[0].moved_resources, 1);
        assert_eq!(report.roots[0].adopted_files, 2);
        assert!(report.summary.deleted_files >= 1);
        let resources: Vec<_> = client
            .resources()
            .unwrap()
            .into_iter()
            .map(|resource| resource.path)
            .collect();
        assert!(resources.contains(&"disk:/canonical/nested/b.txt".to_string()));
        assert!(!resources.contains(&"disk:/legacy/old.txt".to_string()));
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
    }

    #[tokio::test]
    async fn small_file_mismatch_uploads_local_source() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "local");
        let client = MockYandexDiskClient::new();
        client
            .insert_file("disk:/canonical/a.txt", b"remote")
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.updated_files, 1);
        let downloaded = client.download("disk:/canonical/a.txt").await.unwrap();
        assert_eq!(downloaded, b"local");
    }

    #[tokio::test]
    async fn medium_file_uses_size_fallback_when_remote_mtime_is_unavailable() {
        let fixture = TestFixture::new();
        fixture.write_file("medium.bin", "0123456789");
        fixture.config_sync_thresholds(2, 100);
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::file("disk:/canonical/medium.bin", 10))
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.roots[0].adopted_files, 1);
        assert_eq!(report.summary.uploaded_files, 0);
    }

    #[tokio::test]
    async fn medium_file_adopts_by_path_name_and_size_ignoring_remote_mtime() {
        let fixture = TestFixture::new();
        fixture.write_file("medium.bin", "0123456789");
        fixture.config_sync_thresholds(2, 100);
        let client = MockYandexDiskClient::new();
        let mut resource = ResourceMetadata::file("disk:/canonical/medium.bin", 10);
        resource.modified = Some("2001-01-01T00:00:00Z".to_string());
        client.insert_resource(resource).unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.roots[0].adopted_files, 1);
        assert_eq!(report.summary.uploaded_files, 0);
        assert_eq!(report.summary.updated_files, 0);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
    }

    #[tokio::test]
    async fn quota_exceeded_stops_before_delete_phase() {
        let fixture = TestFixture::new();
        fixture.write_file("missing.txt", "content larger than quota");
        let client = MockYandexDiskClient::new();
        client
            .insert_file("disk:/canonical/extra.txt", b"extra")
            .unwrap();
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

        let report = fixture.run_migration(&client).await;

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
    async fn missing_local_root_is_skipped_without_remote_delete() {
        let fixture = TestFixture::new();
        let missing = fixture.root_path().join("missing");
        fixture.set_local_path(path_to_stable_string(&missing));
        let client = MockYandexDiskClient::new();
        client
            .insert_file("disk:/canonical/remote.txt", b"remote")
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::PartialFailed);
        assert_eq!(report.summary.skipped_files, 1);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(|operation| operation.kind == MockOperationKind::DeletePermanently));
    }

    #[tokio::test]
    async fn remote_inventory_failure_marks_partial_and_skips_upload_delete() {
        let fixture = TestFixture::new();
        fixture.write_file("local.txt", "local");
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::directory("disk:/canonical"))
            .unwrap();
        client
            .insert_file("disk:/canonical/remote-extra.txt", b"remote")
            .unwrap();
        client
            .fail_next(
                MockOperationKind::ListRecursive,
                "disk:/canonical",
                YandexDiskError::new(YandexDiskErrorKind::Transient, "decode body failed"),
            )
            .unwrap();

        let report = fixture.run_migration(&client).await;

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
    async fn migration_remote_delete_not_found_is_idempotent_success() {
        let fixture = TestFixture::new();
        fixture.write_file("keep.txt", "one");
        let client = MockYandexDiskClient::new();
        client
            .insert_file("disk:/canonical/old.txt", b"old")
            .unwrap();
        client
            .fail_next(
                MockOperationKind::DeletePermanently,
                "disk:/canonical/old.txt",
                YandexDiskError::new(YandexDiskErrorKind::NotFound, "Resource not found"),
            )
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(report.summary.deleted_files, 0);
        assert!(fixture
            .repository
            .list_recent_failed_items(10)
            .unwrap()
            .is_empty());
        let operations = fixture.repository.list_recent_operations(20).unwrap();
        let delete_operation = operations
            .iter()
            .find(|operation| operation.operation == "migration_delete_remote_extra")
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
    fn legacy_delete_candidates_ignore_invalid_remote_candidates() {
        let legacy = LegacyInventory {
            root_path: "disk:/legacy".to_string(),
            resources: BTreeMap::from([
                (
                    "disk:/canonical/outside.txt".to_string(),
                    ResourceMetadata::file("disk:/canonical/outside.txt", 1),
                ),
                (
                    "disk:/legacy/disk:".to_string(),
                    ResourceMetadata::directory("disk:/legacy/disk:"),
                ),
                (
                    "disk:/legacy/disk:/child.txt".to_string(),
                    ResourceMetadata::file("disk:/legacy/disk:/child.txt", 1),
                ),
                (
                    "disk:/legacy/old.txt".to_string(),
                    ResourceMetadata::file("disk:/legacy/old.txt", 1),
                ),
            ]),
        };

        let deletes = collect_legacy_delete_candidates(&legacy, "disk:/canonical");

        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].remote_path, "disk:/legacy/old.txt");
        assert_eq!(deletes[0].canonical_path, "disk:/canonical/old.txt");
    }

    #[tokio::test]
    async fn migration_ignores_phantom_disk_segment_inside_canonical_root() {
        let fixture = TestFixture::new();
        let client = MockYandexDiskClient::new();
        client
            .insert_resource(ResourceMetadata::directory("disk:/canonical/disk:"))
            .unwrap();
        client
            .insert_resource(ResourceMetadata::file("disk:/canonical/disk:/child.txt", 1))
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert!(!client
            .operation_log()
            .unwrap()
            .iter()
            .any(
                |operation| operation.kind == MockOperationKind::DeletePermanently
                    && operation.remote_path.contains("/disk:")
            ));
    }

    #[tokio::test]
    async fn persistent_upload_conflict_is_recorded_as_migration_skipped() {
        let fixture = TestFixture::new();
        fixture.write_file("blocked.txt", "blocked");
        let client = MockYandexDiskClient::new();
        client
            .fail_next(
                MockOperationKind::Upload,
                "disk:/canonical/blocked.txt",
                YandexDiskError::persistent_uploader_conflict("disk:/canonical/blocked.txt"),
            )
            .unwrap();

        let report = fixture.run_migration(&client).await;

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
    async fn local_upload_source_unavailable_is_recorded_as_migration_skipped() {
        let fixture = TestFixture::new();
        fixture.write_file("busy.1cd", "busy");
        let client = MockYandexDiskClient::new();
        client
            .fail_next(
                MockOperationKind::Upload,
                "disk:/canonical/busy.1cd",
                YandexDiskError::local_upload_source_unavailable(
                    fixture.root_path().join("busy.1cd").display(),
                    "file is locked by another process",
                ),
            )
            .unwrap();

        let report = fixture.run_migration(&client).await;

        assert_eq!(report.status, SyncRunStatus::Succeeded);
        assert_eq!(report.summary.failed_files, 0);
        assert_eq!(report.summary.skipped_files, 1);
        let skipped = fixture.repository.list_recent_skipped_items(10).unwrap();
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].item.reason_code, "local_file_unavailable");
        assert_eq!(
            fixture
                .repository
                .get_file_state("root", "busy.1cd")
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
    async fn sync_run_auto_migrates_empty_state_and_next_run_is_noop() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();
        client.insert_file("disk:/legacy/a.txt", b"one").unwrap();

        let config = fixture.config();
        let engine = SyncEngine::new(&config, &fixture.repository, Arc::new(client.clone()));
        let first = engine
            .run_once(SyncRunOptions::default(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(first.status, SyncRunStatus::Succeeded);
        assert_eq!(first.summary.uploaded_files, 0);

        client.clear_operation_log().unwrap();
        let second = engine
            .run_once(SyncRunOptions::default(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(second.status, SyncRunStatus::Succeeded);
        assert_eq!(second.summary.uploaded_files, 0);
        assert_eq!(second.summary.deleted_files, 0);
        let log = client.operation_log().unwrap();
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Upload));
        assert!(!log
            .iter()
            .any(|operation| operation.kind == MockOperationKind::Move));
    }

    #[tokio::test]
    async fn migration_resume_skips_unchanged_synced_file_without_new_journal_entry() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        let client = MockYandexDiskClient::new();
        let mut remote = ResourceMetadata::file("disk:/canonical/a.txt", 3);
        remote.md5 = Some("f97c5d29941bfb1b2fdab0874906ab82".to_string());
        client.insert_resource(remote).unwrap();

        let first = fixture.run_migration(&client).await;
        assert_eq!(first.status, SyncRunStatus::Succeeded);
        let operations_after_first = fixture.repository.list_recent_operations(10).unwrap();
        assert_eq!(operations_after_first.len(), 1);
        assert_eq!(
            operations_after_first[0].operation,
            "migration_adopt_remote_file"
        );

        let second = fixture.run_migration(&client).await;
        assert_eq!(second.status, SyncRunStatus::Succeeded);
        assert_eq!(second.summary.uploaded_files, 0);
        assert_eq!(second.summary.updated_files, 0);
        assert_eq!(second.roots[0].adopted_files, 1);

        let operations_after_second = fixture.repository.list_recent_operations(10).unwrap();
        assert_eq!(operations_after_second, operations_after_first);
    }

    #[tokio::test]
    async fn disappeared_file_during_adoption_is_skipped_and_continues() {
        let fixture = TestFixture::new();
        fixture.write_file("a.txt", "one");
        fixture.write_file("b.txt", "two");
        let config = fixture.config();
        let root = &config.roots[0];
        let client = MockYandexDiskClient::new();
        let engine = MigrationEngine::new(&config, &fixture.repository, Arc::new(client.clone()));
        let mut root_summary = engine.initial_root_summary(root).unwrap();
        let scanner = Scanner::new(ScanOptions::from_config(
            &config.sync,
            &config.global_excludes,
            &config.absolute_excludes,
            PathBuf::from(&config.paths.staging_dir),
        ));
        let scan_root = ScanRoot::new(
            root.id.clone(),
            PathBuf::from(&root.local_path),
            root_summary.canonical_remote_path.clone(),
            root.excludes.clone(),
        );
        let scan_report = scanner
            .scan_root_with_repository(&scan_root, &fixture.repository)
            .unwrap();

        fs::remove_file(fixture.root_path().join("a.txt")).unwrap();

        let mut remote_a = ResourceMetadata::file("disk:/canonical/a.txt", 3);
        remote_a.md5 = Some("f97c5d29941bfb1b2fdab0874906ab82".to_string());
        let mut remote_b = ResourceMetadata::file("disk:/canonical/b.txt", 3);
        remote_b.md5 = Some("b8a9f715dbb64fd5c56e7783c6820a61".to_string());
        let mut remote_sets = RemoteSets {
            canonical: BTreeMap::from([
                (remote_a.path.clone(), remote_a),
                (remote_b.path.clone(), remote_b),
            ]),
            legacy: Vec::new(),
            from_cache: false,
        };
        let run_id = fixture
            .repository
            .begin_run(SyncRunTrigger::Migration)
            .unwrap();
        let mut report = MigrationRunReport {
            run_id,
            status: SyncRunStatus::Running,
            summary: SyncRunSummary::default(),
            roots: Vec::new(),
            plan: MigrationPlan::new(),
        };

        let result = engine
            .apply_adopt_or_upload(
                run_id,
                root,
                &mut LiveScanPlan::from_scan_report(fixture.root_path(), &scan_report),
                &mut root_summary,
                &CancellationToken::new(),
                None,
                &mut report,
                &mut remote_sets,
                &SyncRunSummary::default(),
            )
            .await
            .unwrap();

        assert!(!result.should_stop);
        assert_eq!(root_summary.adopted_files, 1);
        assert_eq!(root_summary.failed_files, 0);
        assert_eq!(root_summary.skipped_files, 1);
        assert_eq!(
            fixture
                .repository
                .get_file_state("root", "a.txt")
                .unwrap()
                .unwrap()
                .status,
            FileStateStatus::Skipped
        );
        assert_eq!(
            fixture
                .repository
                .get_file_state("root", "b.txt")
                .unwrap()
                .unwrap()
                .status,
            FileStateStatus::Synced
        );
        assert!(fixture
            .repository
            .list_recent_failed_items(10)
            .unwrap()
            .is_empty());
        let skipped = fixture.repository.list_recent_skipped_items(10).unwrap();
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].item.reason_code, "local_file_unavailable");
    }

    #[tokio::test]
    #[ignore = "requires YDS_LIVE_MIGRATION_TESTS=1 and local keyring sandbox"]
    async fn live_migration_smoke_is_gated_by_env() {
        if std::env::var("YDS_LIVE_MIGRATION_TESTS").ok().as_deref() != Some("1") {
            return;
        }

        let account_alias =
            std::env::var("YDS_TEST_KEYRING_ACCOUNT_ALIAS").unwrap_or_else(|_| "default".into());
        let local_base = PathBuf::from(
            std::env::var("YDS_LIVE_LOCAL_ROOT")
                .unwrap_or_else(|_| r"C:\Data\YaDiskSyncSandbox".to_string()),
        );
        let remote_base = std::env::var("YDS_LIVE_REMOTE_ROOT")
            .unwrap_or_else(|_| "disk:/Backup/YaDiskSyncSandbox".to_string());
        assert_live_sandbox(&local_base, &remote_base);

        let unique = format!("yds-iteration7-live-{}", Uuid::new_v4());
        let local_root = local_base.join(&unique);
        let remote_case_root = format!("{}/{}", remote_base.trim_end_matches('/'), unique);
        let canonical = format!("{remote_case_root}/canonical");
        let legacy = format!("{remote_case_root}/legacy");

        let token_store: Arc<dyn TokenStore> =
            Arc::new(KeyringTokenStore::default_store().unwrap());
        let client = HttpYandexDiskClient::new(
            account_alias,
            token_store,
            RetryPolicy::new(3, std::time::Duration::from_secs(1)),
        )
        .unwrap();
        let client = Arc::new(client);

        cleanup_live_local_cases(&local_base);
        cleanup_live_remote_cases(client.as_ref(), &remote_base).await;
        let _ = client.delete_permanently(&remote_case_root).await;
        let _ = fs::remove_dir_all(&local_root);
        fs::create_dir_all(local_root.join("nested")).unwrap();
        fs::write(local_root.join("adopted.txt"), b"same").unwrap();
        fs::write(local_root.join("changed.txt"), b"local").unwrap();
        fs::write(local_root.join("missing.txt"), b"upload").unwrap();
        fs::write(local_root.join("nested").join("legacy.txt"), b"legacy").unwrap();

        create_remote_directory(client.as_ref(), &canonical).await;
        create_remote_directory(client.as_ref(), &format!("{legacy}/nested")).await;
        client
            .upload(&format!("{canonical}/adopted.txt"), b"same".to_vec(), true)
            .await
            .unwrap();
        client
            .upload(
                &format!("{canonical}/changed.txt"),
                b"remote".to_vec(),
                true,
            )
            .await
            .unwrap();
        client
            .upload(&format!("{canonical}/extra.txt"), b"extra".to_vec(), true)
            .await
            .unwrap();
        client
            .upload(
                &format!("{legacy}/nested/legacy.txt"),
                b"legacy".to_vec(),
                true,
            )
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let mut config = default_config();
        config.paths.state_db = path_to_stable_string(temp.path().join("state.sqlite"));
        config.paths.staging_dir = path_to_stable_string(temp.path().join("staging"));
        config.yandex_disk.account_alias = "default".to_string();
        config.global_excludes = Vec::new();
        config.absolute_excludes = Vec::new();
        config.roots = vec![SyncRootConfig {
            id: "live".to_string(),
            name: "Live".to_string(),
            enabled: true,
            local_path: path_to_stable_string(&local_root),
            remote_path_override: Some(canonical.clone()),
            legacy_remote_paths: vec![legacy.clone()],
            excludes: Vec::new(),
        }];
        let repository = StateRepository::open(&config.paths.state_db).unwrap();
        let migration = MigrationEngine::new(&config, &repository, client.clone());
        let migration_report = migration
            .run_once(MigrationRunOptions::default(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(migration_report.status, SyncRunStatus::Succeeded);
        assert!(migration_report.roots[0].adopted_files >= 2);
        assert!(migration_report.summary.deleted_files >= 1);

        let sync = SyncEngine::new(&config, &repository, client.clone());
        let sync_report = sync
            .run_once(SyncRunOptions::default(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(sync_report.status, SyncRunStatus::Succeeded);
        assert_eq!(sync_report.summary.uploaded_files, 0);
        assert_eq!(sync_report.summary.deleted_files, 0);

        client.delete_permanently(&remote_case_root).await.unwrap();
        fs::remove_dir_all(&local_root).unwrap();
    }

    struct TestFixture {
        temp: tempfile::TempDir,
        config: std::cell::RefCell<AppConfig>,
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
                remote_path_override: Some("disk:/canonical".to_string()),
                legacy_remote_paths: vec!["disk:/legacy".to_string()],
                excludes: Vec::new(),
            }];
            let repository = StateRepository::open_in_memory_for_tests().unwrap();
            Self {
                temp,
                config: std::cell::RefCell::new(config),
                repository,
            }
        }

        fn config(&self) -> AppConfig {
            self.config.borrow().clone()
        }

        fn root_path(&self) -> PathBuf {
            self.temp.path().join("root")
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

        fn set_local_path(&self, local_path: String) {
            self.config.borrow_mut().roots[0].local_path = local_path;
        }

        fn config_sync_thresholds(&self, full_hash_max_bytes: u64, large_min_bytes: u64) {
            let mut config = self.config.borrow_mut();
            config.sync.full_hash_max_bytes = full_hash_max_bytes;
            config.sync.large_file_name_size_only_min_bytes = large_min_bytes;
        }

        async fn run_migration(&self, client: &MockYandexDiskClient) -> MigrationRunReport {
            let config = self.config();
            let engine = MigrationEngine::new(&config, &self.repository, Arc::new(client.clone()));
            engine
                .run_once(MigrationRunOptions::default(), &CancellationToken::new())
                .await
                .unwrap()
        }
    }

    async fn create_remote_directory(client: &dyn YandexDiskClient, remote_path: &str) {
        for parent in parent_remote_paths(remote_path) {
            client.create_directory(&parent).await.unwrap();
        }
        client.create_directory(remote_path).await.unwrap();
    }

    fn assert_live_sandbox(local_base: &Path, remote_base: &str) {
        let local = path_to_stable_string(local_base);
        assert!(
            local == "C:/Data/YaDiskSyncSandbox" || local.starts_with("C:/Data/YaDiskSyncSandbox/"),
            "live migration local root must stay under C:/Data/YaDiskSyncSandbox"
        );
        assert!(
            remote_base == "disk:/Backup/YaDiskSyncSandbox"
                || remote_base.starts_with("disk:/Backup/YaDiskSyncSandbox/"),
            "live migration remote root must stay under disk:/Backup/YaDiskSyncSandbox"
        );
    }

    fn cleanup_live_local_cases(local_base: &Path) {
        let Ok(entries) = fs::read_dir(local_base) else {
            return;
        };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            if file_name
                .to_string_lossy()
                .starts_with("yds-iteration7-live-")
            {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }

    async fn cleanup_live_remote_cases(client: &dyn YandexDiskClient, remote_base: &str) {
        let Ok(resources) = client.list_recursive(remote_base).await else {
            return;
        };
        let prefix = format!("{}/", remote_base.trim_end_matches('/'));
        let mut roots = std::collections::BTreeSet::new();
        for resource in resources {
            let Some(relative) = resource.path.strip_prefix(&prefix) else {
                continue;
            };
            let Some(first_segment) = relative.split('/').next() else {
                continue;
            };
            if first_segment.starts_with("yds-iteration7-live-") {
                roots.insert(format!("{}{}", prefix, first_segment));
            }
        }
        for root in roots {
            let _ = client.delete_permanently(&root).await;
        }
    }
}
