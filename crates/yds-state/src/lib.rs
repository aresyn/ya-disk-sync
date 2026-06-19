pub mod models;

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use models::{
    DeletedSubtreeRecord, DirectoryStateRecord, DirectoryStateStatus, FailedItemRecord,
    FileStateRecord, FileStateStatus, MigrationMapRecord, MigrationMapStatus,
    OperationJournalRecord, OperationRecord, OperationStatus, RecentFailedItemRecord,
    RecentSkippedItemRecord, RemoteResourceRecord, RootRemoteInventoryStatus, RunLockGuard,
    SkippedItemRecord, SyncRootRecord, SyncRunRecord, SyncRunStatus, SyncRunSummary,
    SyncRunTrigger, UnknownEnumValue,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime, UtcOffset};
use yds_core::ComponentStatus;

pub const COMPONENT_NAME: &str = "state";
const RUN_LOCK_NAME: &str = "sync_run";
const CURRENT_SCHEMA_VERSION: i64 = 2;
const INITIAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations(
  version INTEGER PRIMARY KEY,
  applied_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  trigger TEXT NOT NULL,
  status TEXT NOT NULL,
  scanned_files INTEGER NOT NULL DEFAULT 0,
  uploaded_files INTEGER NOT NULL DEFAULT 0,
  updated_files INTEGER NOT NULL DEFAULT 0,
  deleted_files INTEGER NOT NULL DEFAULT 0,
  skipped_files INTEGER NOT NULL DEFAULT 0,
  failed_files INTEGER NOT NULL DEFAULT 0,
  bytes_uploaded INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT
);

CREATE TABLE IF NOT EXISTS sync_roots(
  id TEXT PRIMARY KEY,
  local_path TEXT NOT NULL,
  remote_root TEXT NOT NULL,
  canonical_remote_path TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  last_seen_at_utc TEXT,
  last_status TEXT,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS file_state(
  root_id TEXT NOT NULL,
  local_path TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  remote_path TEXT NOT NULL,
  normalized_remote_path TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  mtime_ns INTEGER,
  ctime_ns INTEGER,
  fingerprint_kind TEXT NOT NULL,
  fingerprint_value TEXT,
  full_sha256 TEXT,
  remote_etag TEXT,
  remote_md5 TEXT,
  last_synced_at_utc TEXT NOT NULL,
  last_run_id INTEGER,
  status TEXT NOT NULL,
  last_error TEXT,
  PRIMARY KEY(root_id, relative_path)
);

CREATE TABLE IF NOT EXISTS directory_state(
  root_id TEXT NOT NULL,
  local_path TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  remote_path TEXT NOT NULL,
  normalized_remote_path TEXT NOT NULL,
  last_seen_at_utc TEXT NOT NULL,
  last_run_id INTEGER,
  status TEXT NOT NULL,
  last_error TEXT,
  PRIMARY KEY(root_id, relative_path)
);

CREATE TABLE IF NOT EXISTS remote_resource_state(
  root_id TEXT NOT NULL,
  remote_path TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  size_bytes INTEGER,
  mtime_remote TEXT,
  remote_md5 TEXT,
  last_seen_at_utc TEXT NOT NULL,
  PRIMARY KEY(root_id, remote_path)
);

CREATE TABLE IF NOT EXISTS skipped_items(
  root_id TEXT NOT NULL,
  local_path TEXT NOT NULL,
  relative_path TEXT,
  remote_path TEXT,
  reason_code TEXT NOT NULL,
  reason_details TEXT,
  size_bytes INTEGER,
  last_seen_at_utc TEXT NOT NULL,
  PRIMARY KEY(root_id, local_path)
);

CREATE TABLE IF NOT EXISTS failed_items(
  root_id TEXT NOT NULL,
  local_path TEXT NOT NULL,
  remote_path TEXT NOT NULL,
  operation TEXT NOT NULL,
  error_kind TEXT NOT NULL,
  error_message TEXT NOT NULL,
  retry_count INTEGER NOT NULL,
  last_failed_at_utc TEXT NOT NULL,
  next_retry_at_utc TEXT,
  PRIMARY KEY(root_id, operation, local_path, remote_path)
);

CREATE TABLE IF NOT EXISTS operation_journal(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  operation TEXT NOT NULL,
  local_path TEXT,
  remote_path TEXT,
  status TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT,
  error_kind TEXT,
  error_message TEXT
);

CREATE TABLE IF NOT EXISTS migration_map_state(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  root_id TEXT NOT NULL,
  legacy_remote_path TEXT NOT NULL,
  canonical_remote_path TEXT NOT NULL,
  status TEXT NOT NULL,
  last_error TEXT,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS state_locks(
  name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  acquired_at_utc TEXT NOT NULL,
  heartbeat_at_utc TEXT NOT NULL,
  heartbeat_unix_seconds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS root_remote_inventory_status(
  root_id TEXT PRIMARY KEY,
  is_complete INTEGER NOT NULL,
  snapshot_completed_at_utc TEXT,
  snapshot_expires_at_utc TEXT,
  resource_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS deleted_subtrees(
  root_id TEXT NOT NULL,
  relative_prefix TEXT NOT NULL,
  remote_prefix TEXT NOT NULL,
  last_run_id INTEGER,
  deleted_at_utc TEXT NOT NULL,
  PRIMARY KEY(root_id, relative_prefix)
);

CREATE INDEX IF NOT EXISTS idx_file_state_remote_path
  ON file_state(root_id, remote_path);
CREATE INDEX IF NOT EXISTS idx_directory_state_remote_path
  ON directory_state(root_id, remote_path);
CREATE INDEX IF NOT EXISTS idx_remote_resource_state_seen
  ON remote_resource_state(root_id, last_seen_at_utc);
CREATE INDEX IF NOT EXISTS idx_operation_journal_run
  ON operation_journal(run_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_migration_map_unique
  ON migration_map_state(root_id, legacy_remote_path, canonical_remote_path);
CREATE INDEX IF NOT EXISTS idx_remote_inventory_status_complete
  ON root_remote_inventory_status(is_complete, snapshot_expires_at_utc);
CREATE INDEX IF NOT EXISTS idx_deleted_subtrees_remote_prefix
  ON deleted_subtrees(root_id, remote_prefix);
"#;

pub type StateResult<T> = Result<T, StateError>;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("time formatting error: {0}")]
    TimeFormat(#[from] time::error::Format),
    #[error("{0}")]
    UnknownEnumValue(#[from] UnknownEnumValue),
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: i64 },
    #[error("{entity} not found: {id}")]
    NotFoundText { entity: &'static str, id: String },
}

pub struct StateRepository {
    conn: Connection,
}

impl StateRepository {
    pub fn open(path: impl AsRef<Path>) -> StateResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| StateError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let conn = Connection::open(path)?;
        configure_connection(&conn, true)?;
        let repository = Self { conn };
        repository.migrate()?;
        Ok(repository)
    }

    pub fn open_in_memory_for_tests() -> StateResult<Self> {
        let conn = Connection::open_in_memory()?;
        configure_connection(&conn, false)?;
        let repository = Self { conn };
        repository.migrate()?;
        Ok(repository)
    }

    pub fn migrate(&self) -> StateResult<()> {
        let applied_at_utc = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        tx.execute_batch(INITIAL_SCHEMA)?;
        let current_version: i64 = tx.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at_utc) VALUES (?1, ?2)",
            params![CURRENT_SCHEMA_VERSION, applied_at_utc],
        )?;
        if current_version < CURRENT_SCHEMA_VERSION {
            tx.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at_utc) VALUES (?1, ?2)",
                params![CURRENT_SCHEMA_VERSION, applied_at_utc],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn begin_run(&self, trigger: SyncRunTrigger) -> StateResult<i64> {
        let started_at_utc = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO sync_runs(started_at_utc, trigger, status) VALUES (?1, ?2, ?3)",
            params![
                started_at_utc,
                trigger.as_str(),
                SyncRunStatus::Running.as_str()
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn repair_interrupted_runs(&self) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "UPDATE operation_journal
             SET status = ?1,
                 finished_at_utc = ?2,
                 error_kind = COALESCE(error_kind, 'cancelled'),
                 error_message = COALESCE(error_message, 'run interrupted before completion')
             WHERE status = ?3",
            params![
                OperationStatus::Cancelled.as_str(),
                now,
                OperationStatus::Running.as_str()
            ],
        )?;
        tx.execute(
            "UPDATE sync_runs
             SET status = ?1,
                 finished_at_utc = ?2,
                 error_summary = COALESCE(error_summary, 'run interrupted before completion')
             WHERE status = ?3 AND finished_at_utc IS NULL",
            params![
                SyncRunStatus::Cancelled.as_str(),
                now,
                SyncRunStatus::Running.as_str()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn finish_run(
        &self,
        run_id: i64,
        status: SyncRunStatus,
        summary: &SyncRunSummary,
    ) -> StateResult<()> {
        let finished_at_utc = utc_text(OffsetDateTime::now_utc())?;
        let updated = self.conn.execute(
            "UPDATE sync_runs
             SET finished_at_utc = ?1,
                 status = ?2,
                 scanned_files = ?3,
                 uploaded_files = ?4,
                 updated_files = ?5,
                 deleted_files = ?6,
                 skipped_files = ?7,
                 failed_files = ?8,
                 bytes_uploaded = ?9,
                 error_summary = ?10
             WHERE id = ?11",
            params![
                finished_at_utc,
                status.as_str(),
                summary.scanned_files,
                summary.uploaded_files,
                summary.updated_files,
                summary.deleted_files,
                summary.skipped_files,
                summary.failed_files,
                summary.bytes_uploaded,
                summary.error_summary,
                run_id
            ],
        )?;
        ensure_updated(updated, "sync_run", run_id)
    }

    pub fn update_run_progress(&self, run_id: i64, summary: &SyncRunSummary) -> StateResult<()> {
        let updated = self.conn.execute(
            "UPDATE sync_runs
             SET scanned_files = ?1,
                 uploaded_files = ?2,
                 updated_files = ?3,
                 deleted_files = ?4,
                 skipped_files = ?5,
                 failed_files = ?6,
                 bytes_uploaded = ?7,
                 error_summary = ?8
             WHERE id = ?9",
            params![
                summary.scanned_files,
                summary.uploaded_files,
                summary.updated_files,
                summary.deleted_files,
                summary.skipped_files,
                summary.failed_files,
                summary.bytes_uploaded,
                summary.error_summary,
                run_id
            ],
        )?;
        ensure_updated(updated, "sync_run", run_id)
    }

    pub fn get_latest_run(&self) -> StateResult<Option<SyncRunRecord>> {
        self.conn
            .query_row(
                sync_run_select_sql("ORDER BY id DESC LIMIT 1").as_str(),
                [],
                read_sync_run_record,
            )
            .optional()
            .map_err(StateError::from)
    }

    pub fn get_latest_successful_run(&self) -> StateResult<Option<SyncRunRecord>> {
        self.conn
            .query_row(
                sync_run_select_sql("WHERE status = 'succeeded' ORDER BY id DESC LIMIT 1").as_str(),
                [],
                read_sync_run_record,
            )
            .optional()
            .map_err(StateError::from)
    }

    pub fn list_recent_runs(&self, limit: usize) -> StateResult<Vec<SyncRunRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut statement = self
            .conn
            .prepare(sync_run_select_sql("ORDER BY id DESC LIMIT ?1").as_str())?;
        let rows = statement.query_map(
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            read_sync_run_record,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn upsert_sync_root(
        &self,
        id: &str,
        local_path: &str,
        remote_root: &str,
        canonical_remote_path: &str,
        enabled: bool,
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO sync_roots(
                 id, local_path, remote_root, canonical_remote_path, enabled, last_seen_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
                 local_path = excluded.local_path,
                 remote_root = excluded.remote_root,
                 canonical_remote_path = excluded.canonical_remote_path,
                 enabled = excluded.enabled,
                 last_seen_at_utc = excluded.last_seen_at_utc",
            params![
                id,
                local_path,
                remote_root,
                canonical_remote_path,
                bool_to_i64(enabled),
                now
            ],
        )?;
        Ok(())
    }

    pub fn update_sync_root_status(
        &self,
        id: &str,
        last_status: &str,
        last_error: Option<&str>,
    ) -> StateResult<()> {
        let updated = self.conn.execute(
            "UPDATE sync_roots
             SET last_status = ?1,
                 last_error = ?2
             WHERE id = ?3",
            params![last_status, last_error, id],
        )?;
        if updated == 1 {
            Ok(())
        } else {
            Err(StateError::NotFoundText {
                entity: "sync_root",
                id: id.to_string(),
            })
        }
    }

    pub fn get_sync_root_status(
        &self,
        id: &str,
    ) -> StateResult<Option<(Option<String>, Option<String>)>> {
        self.conn
            .query_row(
                "SELECT last_status, last_error FROM sync_roots WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(StateError::from)
    }

    pub fn list_sync_roots(&self) -> StateResult<Vec<SyncRootRecord>> {
        let mut statement = self.conn.prepare(
            "SELECT id, local_path, remote_root, canonical_remote_path, enabled,
                    last_seen_at_utc, last_status, last_error
             FROM sync_roots
             ORDER BY id",
        )?;
        let rows = statement.query_map([], read_sync_root_record)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn get_root_remote_inventory_status(
        &self,
        root_id: &str,
    ) -> StateResult<Option<RootRemoteInventoryStatus>> {
        self.conn
            .query_row(
                "SELECT root_id, is_complete, snapshot_completed_at_utc, snapshot_expires_at_utc,
                        resource_count, last_error
                 FROM root_remote_inventory_status
                 WHERE root_id = ?1",
                params![root_id],
                read_root_remote_inventory_status,
            )
            .optional()
            .map_err(StateError::from)
    }

    pub fn set_root_remote_inventory_status(
        &self,
        status: &RootRemoteInventoryStatus,
    ) -> StateResult<()> {
        self.conn.execute(
            "INSERT INTO root_remote_inventory_status(
                 root_id, is_complete, snapshot_completed_at_utc, snapshot_expires_at_utc,
                 resource_count, last_error
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(root_id) DO UPDATE SET
                 is_complete = excluded.is_complete,
                 snapshot_completed_at_utc = excluded.snapshot_completed_at_utc,
                 snapshot_expires_at_utc = excluded.snapshot_expires_at_utc,
                 resource_count = excluded.resource_count,
                 last_error = excluded.last_error",
            params![
                status.root_id,
                bool_to_i64(status.is_complete),
                status.snapshot_completed_at_utc,
                status.snapshot_expires_at_utc,
                status.resource_count,
                status.last_error
            ],
        )?;
        Ok(())
    }

    pub fn upsert_file_state(&self, record: &FileStateRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO file_state(
                 root_id, local_path, relative_path, remote_path, normalized_remote_path,
                 size_bytes, mtime_ns, ctime_ns, fingerprint_kind, fingerprint_value,
                 full_sha256, remote_etag, remote_md5, last_synced_at_utc, last_run_id,
                 status, last_error
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
             ON CONFLICT(root_id, relative_path) DO UPDATE SET
                 local_path = excluded.local_path,
                 remote_path = excluded.remote_path,
                 normalized_remote_path = excluded.normalized_remote_path,
                 size_bytes = excluded.size_bytes,
                 mtime_ns = excluded.mtime_ns,
                 ctime_ns = excluded.ctime_ns,
                 fingerprint_kind = excluded.fingerprint_kind,
                 fingerprint_value = excluded.fingerprint_value,
                 full_sha256 = excluded.full_sha256,
                 remote_etag = excluded.remote_etag,
                 remote_md5 = excluded.remote_md5,
                 last_synced_at_utc = excluded.last_synced_at_utc,
                 last_run_id = excluded.last_run_id,
                 status = excluded.status,
                 last_error = excluded.last_error",
            params![
                record.root_id,
                record.local_path,
                record.relative_path,
                record.remote_path,
                record.normalized_remote_path,
                record.size_bytes,
                record.mtime_ns,
                record.ctime_ns,
                record.fingerprint_kind,
                record.fingerprint_value,
                record.full_sha256,
                record.remote_etag,
                record.remote_md5,
                now,
                record.last_run_id,
                record.status.as_str(),
                record.last_error
            ],
        )?;
        Ok(())
    }

    pub fn upsert_file_states(&self, records: &[FileStateRecord]) -> StateResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO file_state(
                     root_id, local_path, relative_path, remote_path, normalized_remote_path,
                     size_bytes, mtime_ns, ctime_ns, fingerprint_kind, fingerprint_value,
                     full_sha256, remote_etag, remote_md5, last_synced_at_utc, last_run_id,
                     status, last_error
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                 ON CONFLICT(root_id, relative_path) DO UPDATE SET
                     local_path = excluded.local_path,
                     remote_path = excluded.remote_path,
                     normalized_remote_path = excluded.normalized_remote_path,
                     size_bytes = excluded.size_bytes,
                     mtime_ns = excluded.mtime_ns,
                     ctime_ns = excluded.ctime_ns,
                     fingerprint_kind = excluded.fingerprint_kind,
                     fingerprint_value = excluded.fingerprint_value,
                     full_sha256 = excluded.full_sha256,
                     remote_etag = excluded.remote_etag,
                     remote_md5 = excluded.remote_md5,
                     last_synced_at_utc = excluded.last_synced_at_utc,
                     last_run_id = excluded.last_run_id,
                     status = excluded.status,
                     last_error = excluded.last_error",
            )?;
            for record in records {
                statement.execute(params![
                    record.root_id,
                    record.local_path,
                    record.relative_path,
                    record.remote_path,
                    record.normalized_remote_path,
                    record.size_bytes,
                    record.mtime_ns,
                    record.ctime_ns,
                    record.fingerprint_kind,
                    record.fingerprint_value,
                    record.full_sha256,
                    record.remote_etag,
                    record.remote_md5,
                    now,
                    record.last_run_id,
                    record.status.as_str(),
                    record.last_error
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_file_state(
        &self,
        root_id: &str,
        relative_path: &str,
    ) -> StateResult<Option<FileStateRecord>> {
        self.conn
            .query_row(
                "SELECT root_id, local_path, relative_path, remote_path, normalized_remote_path,
                        size_bytes, mtime_ns, ctime_ns, fingerprint_kind, fingerprint_value,
                        full_sha256, remote_etag, remote_md5, last_run_id, status, last_error
                 FROM file_state
                 WHERE root_id = ?1 AND relative_path = ?2",
                params![root_id, relative_path],
                read_file_state_record,
            )
            .optional()
            .map_err(StateError::from)
    }

    pub fn list_file_states_for_root(&self, root_id: &str) -> StateResult<Vec<FileStateRecord>> {
        let mut statement = self.conn.prepare(
            "SELECT root_id, local_path, relative_path, remote_path, normalized_remote_path,
                    size_bytes, mtime_ns, ctime_ns, fingerprint_kind, fingerprint_value,
                    full_sha256, remote_etag, remote_md5, last_run_id, status, last_error
             FROM file_state
             WHERE root_id = ?1
             ORDER BY relative_path",
        )?;
        let rows = statement.query_map(params![root_id], read_file_state_record)?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn has_snapshot_for_root(&self, root_id: &str) -> StateResult<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT
                 (SELECT COUNT(*) FROM file_state
                  WHERE root_id = ?1 AND status <> 'deleted')
               + (SELECT COUNT(*) FROM directory_state
                  WHERE root_id = ?1 AND status <> 'deleted')",
            params![root_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn upsert_directory_state(&self, record: &DirectoryStateRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO directory_state(
                 root_id, local_path, relative_path, remote_path, normalized_remote_path,
                 last_seen_at_utc, last_run_id, status, last_error
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(root_id, relative_path) DO UPDATE SET
                 local_path = excluded.local_path,
                 remote_path = excluded.remote_path,
                 normalized_remote_path = excluded.normalized_remote_path,
                 last_seen_at_utc = excluded.last_seen_at_utc,
                 last_run_id = excluded.last_run_id,
                 status = excluded.status,
                 last_error = excluded.last_error",
            params![
                record.root_id,
                record.local_path,
                record.relative_path,
                record.remote_path,
                record.normalized_remote_path,
                now,
                record.last_run_id,
                record.status.as_str(),
                record.last_error
            ],
        )?;
        Ok(())
    }

    pub fn mark_skipped(&self, record: &SkippedItemRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO skipped_items(
                 root_id, local_path, relative_path, remote_path, reason_code,
                 reason_details, size_bytes, last_seen_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(root_id, local_path) DO UPDATE SET
                 relative_path = excluded.relative_path,
                 remote_path = excluded.remote_path,
                 reason_code = excluded.reason_code,
                 reason_details = excluded.reason_details,
                 size_bytes = excluded.size_bytes,
                 last_seen_at_utc = excluded.last_seen_at_utc",
            params![
                record.root_id,
                record.local_path,
                record.relative_path,
                record.remote_path,
                record.reason_code,
                record.reason_details,
                record.size_bytes,
                now
            ],
        )?;
        Ok(())
    }

    pub fn list_recent_skipped_items(
        &self,
        limit: usize,
    ) -> StateResult<Vec<RecentSkippedItemRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut statement = self.conn.prepare(
            "SELECT root_id, local_path, relative_path, remote_path, reason_code,
                    reason_details, size_bytes, last_seen_at_utc
             FROM skipped_items
             ORDER BY last_seen_at_utc DESC, root_id, local_path
             LIMIT ?1",
        )?;
        let rows = statement.query_map(
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            read_recent_skipped_item_record,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn mark_failed(&self, record: &FailedItemRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        let local_path = record.local_path.as_deref().unwrap_or("");
        let remote_path = record.remote_path.as_deref().unwrap_or("");
        self.conn.execute(
            "INSERT INTO failed_items(
                 root_id, local_path, remote_path, operation, error_kind, error_message,
                 retry_count, last_failed_at_utc, next_retry_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(root_id, operation, local_path, remote_path) DO UPDATE SET
                 error_kind = excluded.error_kind,
                 error_message = excluded.error_message,
                 retry_count = excluded.retry_count,
                 last_failed_at_utc = excluded.last_failed_at_utc,
                 next_retry_at_utc = excluded.next_retry_at_utc",
            params![
                record.root_id,
                local_path,
                remote_path,
                record.operation,
                record.error_kind,
                record.error_message,
                record.retry_count,
                now,
                record.next_retry_at_utc
            ],
        )?;
        Ok(())
    }

    pub fn list_recent_failed_items(
        &self,
        limit: usize,
    ) -> StateResult<Vec<RecentFailedItemRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut statement = self.conn.prepare(
            "SELECT root_id, local_path, remote_path, operation, error_kind,
                    error_message, retry_count, next_retry_at_utc, last_failed_at_utc
             FROM failed_items
             ORDER BY last_failed_at_utc DESC, root_id, operation, local_path, remote_path
             LIMIT ?1",
        )?;
        let rows = statement.query_map(
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            read_recent_failed_item_record,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn mark_deleted(
        &self,
        root_id: &str,
        relative_path: &str,
        remote_path: &str,
        last_run_id: Option<i64>,
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO file_state(
                 root_id, local_path, relative_path, remote_path, normalized_remote_path,
                 size_bytes, fingerprint_kind, last_synced_at_utc, last_run_id, status
             )
             VALUES (?1, ?2, ?3, ?4, ?5, 0, 'deleted', ?6, ?7, ?8)
             ON CONFLICT(root_id, relative_path) DO UPDATE SET
                 last_synced_at_utc = excluded.last_synced_at_utc,
                 last_run_id = excluded.last_run_id,
                 status = excluded.status,
                 last_error = NULL",
            params![
                root_id,
                relative_path,
                relative_path,
                remote_path,
                remote_path,
                now,
                last_run_id,
                FileStateStatus::Deleted.as_str()
            ],
        )?;
        Ok(())
    }

    pub fn mark_directory_deleted(
        &self,
        root_id: &str,
        relative_path: &str,
        remote_path: &str,
        last_run_id: Option<i64>,
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO directory_state(
                 root_id, local_path, relative_path, remote_path, normalized_remote_path,
                 last_seen_at_utc, last_run_id, status
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(root_id, relative_path) DO UPDATE SET
                 last_seen_at_utc = excluded.last_seen_at_utc,
                 last_run_id = excluded.last_run_id,
                 status = excluded.status,
                 last_error = NULL",
            params![
                root_id,
                relative_path,
                relative_path,
                remote_path,
                remote_path,
                now,
                last_run_id,
                DirectoryStateStatus::Deleted.as_str()
            ],
        )?;
        Ok(())
    }

    pub fn upsert_remote_resource(&self, record: &RemoteResourceRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO remote_resource_state(
                 root_id, remote_path, resource_type, size_bytes,
                 mtime_remote, remote_md5, last_seen_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(root_id, remote_path) DO UPDATE SET
                 resource_type = excluded.resource_type,
                 size_bytes = excluded.size_bytes,
                 mtime_remote = excluded.mtime_remote,
                 remote_md5 = excluded.remote_md5,
                 last_seen_at_utc = excluded.last_seen_at_utc",
            params![
                record.root_id,
                record.remote_path,
                record.resource_type.as_str(),
                record.size_bytes,
                record.mtime_remote,
                record.remote_md5,
                now
            ],
        )?;
        Ok(())
    }

    pub fn upsert_remote_resources(&self, records: &[RemoteResourceRecord]) -> StateResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO remote_resource_state(
                     root_id, remote_path, resource_type, size_bytes,
                     mtime_remote, remote_md5, last_seen_at_utc
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(root_id, remote_path) DO UPDATE SET
                     resource_type = excluded.resource_type,
                     size_bytes = excluded.size_bytes,
                     mtime_remote = excluded.mtime_remote,
                     remote_md5 = excluded.remote_md5,
                     last_seen_at_utc = excluded.last_seen_at_utc",
            )?;
            for record in records {
                statement.execute(params![
                    record.root_id,
                    record.remote_path,
                    record.resource_type.as_str(),
                    record.size_bytes,
                    record.mtime_remote,
                    record.remote_md5,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn replace_remote_resources_for_root(
        &self,
        root_id: &str,
        records: &[RemoteResourceRecord],
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM remote_resource_state WHERE root_id = ?1",
            params![root_id],
        )?;
        if !records.is_empty() {
            let mut statement = tx.prepare(
                "INSERT INTO remote_resource_state(
                     root_id, remote_path, resource_type, size_bytes,
                     mtime_remote, remote_md5, last_seen_at_utc
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for record in records {
                statement.execute(params![
                    record.root_id,
                    record.remote_path,
                    record.resource_type.as_str(),
                    record.size_bytes,
                    record.mtime_remote,
                    record.remote_md5,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_remote_resources_for_root(
        &self,
        root_id: &str,
    ) -> StateResult<Vec<RemoteResourceRecord>> {
        let mut statement = self.conn.prepare(
            "SELECT root_id, remote_path, resource_type, size_bytes, mtime_remote, remote_md5
             FROM remote_resource_state
             WHERE root_id = ?1
             ORDER BY remote_path",
        )?;
        let rows = statement.query_map(params![root_id], read_remote_resource_record)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn delete_remote_resource_subtree(
        &self,
        root_id: &str,
        remote_prefix: &str,
    ) -> StateResult<()> {
        self.conn.execute(
            "DELETE FROM remote_resource_state
             WHERE root_id = ?1
               AND (remote_path = ?2 OR remote_path LIKE ?3)",
            params![root_id, remote_prefix, format!("{}/%", remote_prefix)],
        )?;
        Ok(())
    }

    pub fn list_known_remote_paths(&self, root_id: &str) -> StateResult<Vec<String>> {
        let mut statement = self.conn.prepare(
            "SELECT remote_path FROM file_state WHERE root_id = ?1
             UNION
             SELECT remote_path FROM directory_state WHERE root_id = ?1
             UNION
             SELECT remote_path FROM remote_resource_state WHERE root_id = ?1
             ORDER BY remote_path",
        )?;
        let rows = statement.query_map(params![root_id], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn upsert_migration_map(&self, record: &MigrationMapRecord) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO migration_map_state(
                 root_id, legacy_remote_path, canonical_remote_path,
                 status, last_error, updated_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(root_id, legacy_remote_path, canonical_remote_path) DO UPDATE SET
                 status = excluded.status,
                 last_error = excluded.last_error,
                 updated_at_utc = excluded.updated_at_utc",
            params![
                record.root_id,
                record.legacy_remote_path,
                record.canonical_remote_path,
                record.status.as_str(),
                record.last_error,
                now
            ],
        )?;
        Ok(())
    }

    pub fn upsert_migration_maps(&self, records: &[MigrationMapRecord]) -> StateResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO migration_map_state(
                     root_id, legacy_remote_path, canonical_remote_path,
                     status, last_error, updated_at_utc
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(root_id, legacy_remote_path, canonical_remote_path) DO UPDATE SET
                     status = excluded.status,
                     last_error = excluded.last_error,
                     updated_at_utc = excluded.updated_at_utc",
            )?;
            for record in records {
                statement.execute(params![
                    record.root_id,
                    record.legacy_remote_path,
                    record.canonical_remote_path,
                    record.status.as_str(),
                    record.last_error,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_migration_map(
        &self,
        root_id: Option<&str>,
    ) -> StateResult<Vec<MigrationMapRecord>> {
        if let Some(root_id) = root_id {
            let mut statement = self.conn.prepare(
                "SELECT root_id, legacy_remote_path, canonical_remote_path, status, last_error
                 FROM migration_map_state
                 WHERE root_id = ?1
                 ORDER BY root_id, legacy_remote_path, canonical_remote_path",
            )?;
            let rows = statement.query_map(params![root_id], read_migration_map_record)?;
            return rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(StateError::from);
        }

        let mut statement = self.conn.prepare(
            "SELECT root_id, legacy_remote_path, canonical_remote_path, status, last_error
             FROM migration_map_state
             ORDER BY root_id, legacy_remote_path, canonical_remote_path",
        )?;
        let rows = statement.query_map([], read_migration_map_record)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn append_operation_started(&self, record: &OperationRecord) -> StateResult<i64> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO operation_journal(
                 run_id, operation, local_path, remote_path, status, started_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                record.run_id,
                record.operation,
                record.local_path,
                record.remote_path,
                OperationStatus::Running.as_str(),
                now
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn append_operations_succeeded(&self, records: &[OperationRecord]) -> StateResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let now = utc_text(OffsetDateTime::now_utc())?;
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut statement = tx.prepare(
                "INSERT INTO operation_journal(
                     run_id, operation, local_path, remote_path, status,
                     started_at_utc, finished_at_utc
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for record in records {
                statement.execute(params![
                    record.run_id,
                    record.operation,
                    record.local_path,
                    record.remote_path,
                    OperationStatus::Succeeded.as_str(),
                    now,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn finish_operation(
        &self,
        operation_id: i64,
        status: OperationStatus,
        error_kind: Option<&str>,
        error_message: Option<&str>,
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        let updated = self.conn.execute(
            "UPDATE operation_journal
             SET status = ?1,
                 finished_at_utc = ?2,
                 error_kind = ?3,
                 error_message = ?4
             WHERE id = ?5",
            params![
                status.as_str(),
                now,
                error_kind,
                error_message,
                operation_id
            ],
        )?;
        ensure_updated(updated, "operation", operation_id)
    }

    pub fn list_recent_operations(&self, limit: usize) -> StateResult<Vec<OperationJournalRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut statement = self.conn.prepare(
            "SELECT id, run_id, operation, local_path, remote_path, status,
                    started_at_utc, finished_at_utc, error_kind, error_message
             FROM operation_journal
             ORDER BY id DESC
             LIMIT ?1",
        )?;
        let rows = statement.query_map(
            params![i64::try_from(limit).unwrap_or(i64::MAX)],
            read_operation_journal_record,
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn mark_subtree_deleted(
        &self,
        root_id: &str,
        relative_prefix: &str,
        remote_prefix: &str,
        run_id: i64,
    ) -> StateResult<()> {
        let now = utc_text(OffsetDateTime::now_utc())?;
        self.conn.execute(
            "INSERT INTO deleted_subtrees(
                 root_id, relative_prefix, remote_prefix, last_run_id, deleted_at_utc
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(root_id, relative_prefix) DO UPDATE SET
                 remote_prefix = excluded.remote_prefix,
                 last_run_id = excluded.last_run_id,
                 deleted_at_utc = excluded.deleted_at_utc",
            params![root_id, relative_prefix, remote_prefix, run_id, now],
        )?;
        Ok(())
    }

    pub fn list_deleted_subtrees(&self, root_id: &str) -> StateResult<Vec<DeletedSubtreeRecord>> {
        let mut statement = self.conn.prepare(
            "SELECT root_id, relative_prefix, remote_prefix, last_run_id
             FROM deleted_subtrees
             WHERE root_id = ?1
             ORDER BY relative_prefix",
        )?;
        let rows = statement.query_map(params![root_id], read_deleted_subtree_record)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    pub fn try_acquire_run_lock(
        &self,
        owner: &str,
        now: OffsetDateTime,
        stale_after: Duration,
    ) -> StateResult<Option<RunLockGuard>> {
        let now_text = utc_text(now)?;
        let now_unix_seconds = now.unix_timestamp();
        let stale_before_unix_seconds = now_unix_seconds - stale_after.whole_seconds();

        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO state_locks(
                 name, owner, acquired_at_utc, heartbeat_at_utc, heartbeat_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![RUN_LOCK_NAME, owner, now_text, now_text, now_unix_seconds],
        )?;
        if inserted == 1 {
            return Ok(Some(RunLockGuard {
                name: RUN_LOCK_NAME.to_string(),
                owner: owner.to_string(),
                acquired_at_utc: now_text,
            }));
        }

        let replaced = self.conn.execute(
            "UPDATE state_locks
             SET owner = ?1,
                 acquired_at_utc = ?2,
                 heartbeat_at_utc = ?3,
                 heartbeat_unix_seconds = ?4
             WHERE name = ?5 AND heartbeat_unix_seconds < ?6",
            params![
                owner,
                now_text,
                now_text,
                now_unix_seconds,
                RUN_LOCK_NAME,
                stale_before_unix_seconds
            ],
        )?;
        if replaced == 1 {
            return Ok(Some(RunLockGuard {
                name: RUN_LOCK_NAME.to_string(),
                owner: owner.to_string(),
                acquired_at_utc: now_text,
            }));
        }

        Ok(None)
    }

    pub fn heartbeat_run_lock(
        &self,
        guard: &RunLockGuard,
        now: OffsetDateTime,
    ) -> StateResult<bool> {
        let now_text = utc_text(now)?;
        let updated = self.conn.execute(
            "UPDATE state_locks
             SET heartbeat_at_utc = ?1,
                 heartbeat_unix_seconds = ?2
             WHERE name = ?3 AND owner = ?4",
            params![now_text, now.unix_timestamp(), guard.name, guard.owner],
        )?;
        Ok(updated == 1)
    }

    pub fn release_run_lock(&self, guard: &RunLockGuard) -> StateResult<bool> {
        let deleted = self.conn.execute(
            "DELETE FROM state_locks WHERE name = ?1 AND owner = ?2",
            params![guard.name, guard.owner],
        )?;
        Ok(deleted == 1)
    }
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(COMPONENT_NAME, "state repository boundary is available")
}

fn configure_connection(conn: &Connection, file_database: bool) -> StateResult<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    if file_database {
        conn.pragma_update(None, "journal_mode", "WAL")?;
    }
    Ok(())
}

fn utc_text(value: OffsetDateTime) -> StateResult<String> {
    Ok(value.to_offset(UtcOffset::UTC).format(&Rfc3339)?)
}

const fn bool_to_i64(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn ensure_updated(updated: usize, entity: &'static str, id: i64) -> StateResult<()> {
    if updated == 1 {
        Ok(())
    } else {
        Err(StateError::NotFound { entity, id })
    }
}

fn read_file_state_record(row: &Row<'_>) -> rusqlite::Result<FileStateRecord> {
    let status: String = row.get(14)?;
    let status = FileStateStatus::try_from(status.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(14, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(FileStateRecord {
        root_id: row.get(0)?,
        local_path: row.get(1)?,
        relative_path: row.get(2)?,
        remote_path: row.get(3)?,
        normalized_remote_path: row.get(4)?,
        size_bytes: row.get(5)?,
        mtime_ns: row.get(6)?,
        ctime_ns: row.get(7)?,
        fingerprint_kind: row.get(8)?,
        fingerprint_value: row.get(9)?,
        full_sha256: row.get(10)?,
        remote_etag: row.get(11)?,
        remote_md5: row.get(12)?,
        last_run_id: row.get(13)?,
        status,
        last_error: row.get(15)?,
    })
}

fn read_sync_root_record(row: &Row<'_>) -> rusqlite::Result<SyncRootRecord> {
    let enabled: i64 = row.get(4)?;
    Ok(SyncRootRecord {
        id: row.get(0)?,
        local_path: row.get(1)?,
        remote_root: row.get(2)?,
        canonical_remote_path: row.get(3)?,
        enabled: enabled != 0,
        last_seen_at_utc: row.get(5)?,
        last_status: row.get(6)?,
        last_error: row.get(7)?,
    })
}

fn read_root_remote_inventory_status(row: &Row<'_>) -> rusqlite::Result<RootRemoteInventoryStatus> {
    let is_complete: i64 = row.get(1)?;
    Ok(RootRemoteInventoryStatus {
        root_id: row.get(0)?,
        is_complete: is_complete != 0,
        snapshot_completed_at_utc: row.get(2)?,
        snapshot_expires_at_utc: row.get(3)?,
        resource_count: row.get(4)?,
        last_error: row.get(5)?,
    })
}

fn read_recent_skipped_item_record(row: &Row<'_>) -> rusqlite::Result<RecentSkippedItemRecord> {
    Ok(RecentSkippedItemRecord {
        item: SkippedItemRecord {
            root_id: row.get(0)?,
            local_path: row.get(1)?,
            relative_path: row.get(2)?,
            remote_path: row.get(3)?,
            reason_code: row.get(4)?,
            reason_details: row.get(5)?,
            size_bytes: row.get(6)?,
        },
        last_seen_at_utc: row.get(7)?,
    })
}

fn read_recent_failed_item_record(row: &Row<'_>) -> rusqlite::Result<RecentFailedItemRecord> {
    let local_path: String = row.get(1)?;
    let remote_path: String = row.get(2)?;
    Ok(RecentFailedItemRecord {
        item: FailedItemRecord {
            root_id: row.get(0)?,
            local_path: (!local_path.is_empty()).then_some(local_path),
            remote_path: (!remote_path.is_empty()).then_some(remote_path),
            operation: row.get(3)?,
            error_kind: row.get(4)?,
            error_message: row.get(5)?,
            retry_count: row.get(6)?,
            next_retry_at_utc: row.get(7)?,
        },
        last_failed_at_utc: row.get(8)?,
    })
}

fn read_operation_journal_record(row: &Row<'_>) -> rusqlite::Result<OperationJournalRecord> {
    let status: String = row.get(5)?;
    let status = OperationStatus::try_from(status.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(OperationJournalRecord {
        id: row.get(0)?,
        run_id: row.get(1)?,
        operation: row.get(2)?,
        local_path: row.get(3)?,
        remote_path: row.get(4)?,
        status,
        started_at_utc: row.get(6)?,
        finished_at_utc: row.get(7)?,
        error_kind: row.get(8)?,
        error_message: row.get(9)?,
    })
}

fn read_remote_resource_record(row: &Row<'_>) -> rusqlite::Result<RemoteResourceRecord> {
    let resource_type: String = row.get(2)?;
    let resource_type =
        models::RemoteResourceType::try_from(resource_type.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;

    Ok(RemoteResourceRecord {
        root_id: row.get(0)?,
        remote_path: row.get(1)?,
        resource_type,
        size_bytes: row.get(3)?,
        mtime_remote: row.get(4)?,
        remote_md5: row.get(5)?,
    })
}

fn sync_run_select_sql(suffix: &str) -> String {
    format!(
        "SELECT id, started_at_utc, finished_at_utc, trigger, status,
                scanned_files, uploaded_files, updated_files, deleted_files,
                skipped_files, failed_files, bytes_uploaded, error_summary
         FROM sync_runs {suffix}"
    )
}

fn read_sync_run_record(row: &Row<'_>) -> rusqlite::Result<SyncRunRecord> {
    let trigger: String = row.get(3)?;
    let trigger = SyncRunTrigger::try_from(trigger.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let status: String = row.get(4)?;
    let status = SyncRunStatus::try_from(status.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(SyncRunRecord {
        id: row.get(0)?,
        started_at_utc: row.get(1)?,
        finished_at_utc: row.get(2)?,
        trigger,
        status,
        summary: SyncRunSummary {
            scanned_files: row.get(5)?,
            uploaded_files: row.get(6)?,
            updated_files: row.get(7)?,
            deleted_files: row.get(8)?,
            skipped_files: row.get(9)?,
            failed_files: row.get(10)?,
            bytes_uploaded: row.get(11)?,
            error_summary: row.get(12)?,
        },
    })
}

fn read_migration_map_record(row: &Row<'_>) -> rusqlite::Result<MigrationMapRecord> {
    let status: String = row.get(3)?;
    let status = MigrationMapStatus::try_from(status.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
    })?;

    Ok(MigrationMapRecord {
        root_id: row.get(0)?,
        legacy_remote_path: row.get(1)?,
        canonical_remote_path: row.get(2)?,
        status,
        last_error: row.get(4)?,
    })
}

fn read_deleted_subtree_record(row: &Row<'_>) -> rusqlite::Result<DeletedSubtreeRecord> {
    Ok(DeletedSubtreeRecord {
        root_id: row.get(0)?,
        relative_prefix: row.get(1)?,
        remote_prefix: row.get(2)?,
        last_run_id: row.get(3)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use models::{DirectoryStateStatus, RemoteResourceType, SyncRunTrigger, UnknownEnumValue};
    use rusqlite::{params, OptionalExtension};
    use yds_core::ComponentHealth;

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), ComponentHealth::Ok);
    }

    #[test]
    fn new_database_is_created_and_migrated() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("state").join("state.sqlite");

        let repository = StateRepository::open(&db_path).unwrap();
        let version: i64 = repository
            .conn
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        let table_count: i64 = repository
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'file_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(version, CURRENT_SCHEMA_VERSION);
        assert_eq!(table_count, 1);
    }

    #[test]
    fn repeated_migration_is_idempotent() {
        let repository = test_repository();

        repository.migrate().unwrap();
        repository.migrate().unwrap();
        let count: i64 = repository
            .conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(count, 1);
    }

    #[test]
    fn begin_and_finish_run_store_status_and_summary() {
        let repository = test_repository();
        let run_id = repository.begin_run(SyncRunTrigger::Cli).unwrap();
        let summary = SyncRunSummary {
            scanned_files: 10,
            uploaded_files: 2,
            updated_files: 3,
            deleted_files: 4,
            skipped_files: 1,
            failed_files: 0,
            bytes_uploaded: 1234,
            error_summary: None,
        };

        repository
            .finish_run(run_id, SyncRunStatus::Succeeded, &summary)
            .unwrap();

        let stored = repository
            .conn
            .query_row(
                "SELECT status, scanned_files, bytes_uploaded, finished_at_utc
                 FROM sync_runs WHERE id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(
            SyncRunStatus::try_from(stored.0.as_str()).unwrap(),
            SyncRunStatus::Succeeded
        );
        assert_eq!(stored.1, 10);
        assert_eq!(stored.2, 1234);
        assert!(stored.3.is_some());
    }

    #[test]
    fn sync_run_read_api_returns_latest_and_recent_records() {
        let repository = test_repository();
        assert!(repository.get_latest_run().unwrap().is_none());

        let first_id = repository.begin_run(SyncRunTrigger::Scheduled).unwrap();
        repository
            .finish_run(
                first_id,
                SyncRunStatus::Succeeded,
                &SyncRunSummary {
                    scanned_files: 1,
                    uploaded_files: 2,
                    ..SyncRunSummary::default()
                },
            )
            .unwrap();
        let second_id = repository.begin_run(SyncRunTrigger::Manual).unwrap();
        repository
            .finish_run(
                second_id,
                SyncRunStatus::Failed,
                &SyncRunSummary {
                    failed_files: 3,
                    error_summary: Some("failed".to_string()),
                    ..SyncRunSummary::default()
                },
            )
            .unwrap();

        let latest = repository.get_latest_run().unwrap().unwrap();
        let latest_success = repository.get_latest_successful_run().unwrap().unwrap();
        let recent = repository.list_recent_runs(2).unwrap();

        assert_eq!(latest.id, second_id);
        assert_eq!(latest.trigger, SyncRunTrigger::Manual);
        assert_eq!(latest.status, SyncRunStatus::Failed);
        assert_eq!(latest.summary.failed_files, 3);
        assert_eq!(latest_success.id, first_id);
        assert_eq!(latest_success.summary.uploaded_files, 2);
        assert_eq!(
            recent.iter().map(|record| record.id).collect::<Vec<_>>(),
            [second_id, first_id]
        );
        assert!(repository.list_recent_runs(0).unwrap().is_empty());
    }

    #[test]
    fn upsert_sync_root_and_file_state_are_idempotent() {
        let repository = test_repository();
        repository
            .upsert_sync_root("root", r"D:\Root", "disk:/host", "disk:/host/D/Root", true)
            .unwrap();

        let mut record = file_record("root", "a.txt", 10);
        repository.upsert_file_state(&record).unwrap();
        record.size_bytes = 42;
        record.fingerprint_value = Some("changed".to_string());
        repository.upsert_file_state(&record).unwrap();

        let stored: (i64, Option<String>, i64) = repository
            .conn
            .query_row(
                "SELECT size_bytes, fingerprint_value, COUNT(*) OVER ()
                 FROM file_state WHERE root_id = 'root' AND relative_path = 'a.txt'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(stored.0, 42);
        assert_eq!(stored.1.as_deref(), Some("changed"));
        assert_eq!(stored.2, 1);
    }

    #[test]
    fn list_sync_roots_returns_sorted_status_records() {
        let repository = test_repository();
        repository
            .upsert_sync_root("b-root", "/b", "disk:/host", "disk:/host/b", true)
            .unwrap();
        repository
            .upsert_sync_root("a-root", "/a", "disk:/host", "disk:/host/a", false)
            .unwrap();
        repository
            .update_sync_root_status("a-root", "skipped", Some("missing"))
            .unwrap();

        let roots = repository.list_sync_roots().unwrap();

        assert_eq!(
            roots
                .iter()
                .map(|root| root.id.as_str())
                .collect::<Vec<_>>(),
            ["a-root", "b-root"]
        );
        assert!(!roots[0].enabled);
        assert_eq!(roots[0].last_status.as_deref(), Some("skipped"));
        assert_eq!(roots[0].last_error.as_deref(), Some("missing"));
    }

    #[test]
    fn file_state_read_api_returns_sorted_records_and_optional_lookup() {
        let repository = test_repository();
        assert!(!repository.has_snapshot_for_root("root").unwrap());

        repository
            .upsert_file_state(&file_record("root", "b.txt", 1))
            .unwrap();
        repository
            .upsert_file_state(&file_record("root", "a.txt", 1))
            .unwrap();

        let listed = repository.list_file_states_for_root("root").unwrap();
        let found = repository.get_file_state("root", "a.txt").unwrap();
        let missing = repository.get_file_state("root", "missing.txt").unwrap();

        assert_eq!(
            listed
                .iter()
                .map(|record| record.relative_path.as_str())
                .collect::<Vec<_>>(),
            ["a.txt", "b.txt"]
        );
        assert_eq!(found.unwrap().relative_path, "a.txt");
        assert!(missing.is_none());
        assert!(repository.has_snapshot_for_root("root").unwrap());
    }

    #[test]
    fn successful_file_state_survives_failed_and_skipped_items() {
        let repository = test_repository();
        repository
            .upsert_file_state(&file_record("root", "stable.txt", 10))
            .unwrap();
        repository
            .mark_failed(&FailedItemRecord {
                root_id: "root".to_string(),
                local_path: Some("stable.txt".to_string()),
                remote_path: Some("disk:/root/stable.txt".to_string()),
                operation: "upload".to_string(),
                error_kind: "transient".to_string(),
                error_message: "network".to_string(),
                retry_count: 1,
                next_retry_at_utc: None,
            })
            .unwrap();
        repository
            .mark_skipped(&SkippedItemRecord {
                root_id: "root".to_string(),
                local_path: "stable.txt".to_string(),
                relative_path: Some("stable.txt".to_string()),
                remote_path: Some("disk:/root/stable.txt".to_string()),
                reason_code: "file_too_large".to_string(),
                reason_details: None,
                size_bytes: Some(10),
            })
            .unwrap();

        let status: String = repository
            .conn
            .query_row(
                "SELECT status FROM file_state WHERE root_id = 'root' AND relative_path = 'stable.txt'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let failed_count: i64 = repository
            .conn
            .query_row("SELECT COUNT(*) FROM failed_items", [], |row| row.get(0))
            .unwrap();
        let skipped_count: i64 = repository
            .conn
            .query_row("SELECT COUNT(*) FROM skipped_items", [], |row| row.get(0))
            .unwrap();

        assert_eq!(
            FileStateStatus::try_from(status.as_str()).unwrap(),
            FileStateStatus::Synced
        );
        assert_eq!(failed_count, 1);
        assert_eq!(skipped_count, 1);
    }

    #[test]
    fn mark_deleted_updates_file_state_status() {
        let repository = test_repository();
        repository
            .upsert_file_state(&file_record("root", "delete-me.txt", 7))
            .unwrap();

        repository
            .mark_deleted("root", "delete-me.txt", "disk:/root/delete-me.txt", Some(9))
            .unwrap();

        let status: String = repository
            .conn
            .query_row(
                "SELECT status FROM file_state WHERE root_id = 'root' AND relative_path = 'delete-me.txt'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            FileStateStatus::try_from(status.as_str()).unwrap(),
            FileStateStatus::Deleted
        );
    }

    #[test]
    fn list_known_remote_paths_returns_sorted_union() {
        let repository = test_repository();
        repository
            .upsert_file_state(&file_record("root", "b.txt", 1))
            .unwrap();
        repository
            .upsert_directory_state(&DirectoryStateRecord {
                root_id: "root".to_string(),
                local_path: "a".to_string(),
                relative_path: "a".to_string(),
                remote_path: "disk:/root/a".to_string(),
                normalized_remote_path: "disk:/root/a".to_string(),
                last_run_id: None,
                status: DirectoryStateStatus::Synced,
                last_error: None,
            })
            .unwrap();
        repository
            .upsert_remote_resource(&RemoteResourceRecord {
                root_id: "root".to_string(),
                remote_path: "disk:/root/c.txt".to_string(),
                resource_type: RemoteResourceType::File,
                size_bytes: Some(2),
                mtime_remote: None,
                remote_md5: None,
            })
            .unwrap();

        let paths = repository.list_known_remote_paths("root").unwrap();

        assert_eq!(
            paths,
            ["disk:/root/a", "disk:/root/b.txt", "disk:/root/c.txt"]
        );
    }

    #[test]
    fn operation_journal_records_status_transitions() {
        let repository = test_repository();
        let run_id = repository.begin_run(SyncRunTrigger::Manual).unwrap();
        let operation_id = repository
            .append_operation_started(&OperationRecord {
                run_id,
                operation: "upload".to_string(),
                local_path: Some("file.txt".to_string()),
                remote_path: Some("disk:/root/file.txt".to_string()),
            })
            .unwrap();

        repository
            .finish_operation(
                operation_id,
                OperationStatus::Failed,
                Some("io"),
                Some("denied"),
            )
            .unwrap();

        let stored: (String, Option<String>, Option<String>) = repository
            .conn
            .query_row(
                "SELECT status, error_kind, error_message FROM operation_journal WHERE id = ?1",
                params![operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(
            OperationStatus::try_from(stored.0.as_str()).unwrap(),
            OperationStatus::Failed
        );
        assert_eq!(stored.1.as_deref(), Some("io"));
        assert_eq!(stored.2.as_deref(), Some("denied"));
    }

    #[test]
    fn recent_ui_read_apis_return_deterministic_records() {
        let repository = test_repository();
        let run_id = repository.begin_run(SyncRunTrigger::Manual).unwrap();
        let operation_id = repository
            .append_operation_started(&OperationRecord {
                run_id,
                operation: "delete".to_string(),
                local_path: None,
                remote_path: Some("disk:/root/extra.txt".to_string()),
            })
            .unwrap();
        repository
            .finish_operation(operation_id, OperationStatus::Succeeded, None, None)
            .unwrap();
        repository
            .mark_skipped(&SkippedItemRecord {
                root_id: "root".to_string(),
                local_path: "too-large.bin".to_string(),
                relative_path: Some("too-large.bin".to_string()),
                remote_path: Some("disk:/root/too-large.bin".to_string()),
                reason_code: "file_too_large".to_string(),
                reason_details: None,
                size_bytes: Some(51),
            })
            .unwrap();
        repository
            .mark_failed(&FailedItemRecord {
                root_id: "root".to_string(),
                local_path: Some("a.txt".to_string()),
                remote_path: Some("disk:/root/a.txt".to_string()),
                operation: "upload".to_string(),
                error_kind: "transient".to_string(),
                error_message: "timeout".to_string(),
                retry_count: 1,
                next_retry_at_utc: None,
            })
            .unwrap();

        let operations = repository.list_recent_operations(10).unwrap();
        let skipped = repository.list_recent_skipped_items(10).unwrap();
        let failed = repository.list_recent_failed_items(10).unwrap();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].status, OperationStatus::Succeeded);
        assert_eq!(operations[0].operation, "delete");
        assert_eq!(skipped[0].item.reason_code, "file_too_large");
        assert_eq!(failed[0].item.error_message, "timeout");
        assert!(repository.list_recent_operations(0).unwrap().is_empty());
        assert!(repository.list_recent_skipped_items(0).unwrap().is_empty());
        assert!(repository.list_recent_failed_items(0).unwrap().is_empty());
    }

    #[test]
    fn migration_map_upsert_and_list_are_deterministic() {
        let repository = test_repository();
        let mut record = MigrationMapRecord {
            root_id: "root".to_string(),
            legacy_remote_path: "disk:/legacy/a.txt".to_string(),
            canonical_remote_path: "disk:/canonical/a.txt".to_string(),
            status: MigrationMapStatus::Moved,
            last_error: None,
        };

        repository.upsert_migration_map(&record).unwrap();
        record.status = MigrationMapStatus::Adopted;
        repository.upsert_migration_map(&record).unwrap();
        repository
            .upsert_migration_map(&MigrationMapRecord {
                root_id: "other".to_string(),
                legacy_remote_path: "disk:/legacy/b.txt".to_string(),
                canonical_remote_path: "disk:/canonical/b.txt".to_string(),
                status: MigrationMapStatus::Skipped,
                last_error: Some("missing local root".to_string()),
            })
            .unwrap();

        let root_records = repository.list_migration_map(Some("root")).unwrap();
        let all_records = repository.list_migration_map(None).unwrap();

        assert_eq!(root_records.len(), 1);
        assert_eq!(root_records[0].status, MigrationMapStatus::Adopted);
        assert_eq!(all_records.len(), 2);
        assert_eq!(all_records[0].root_id, "other");
        assert_eq!(all_records[1].root_id, "root");
    }

    #[test]
    fn batch_upserts_are_idempotent_and_operations_are_finished() {
        let repository = test_repository();
        let mut first_file = file_record("root", "a.txt", 10);
        let second_file = file_record("root", "b.txt", 20);
        repository
            .upsert_file_states(&[first_file.clone(), second_file])
            .unwrap();
        first_file.size_bytes = 42;
        repository.upsert_file_states(&[first_file]).unwrap();

        repository
            .upsert_remote_resources(&[
                RemoteResourceRecord {
                    root_id: "root".to_string(),
                    remote_path: "disk:/root/a.txt".to_string(),
                    resource_type: RemoteResourceType::File,
                    size_bytes: Some(10),
                    mtime_remote: None,
                    remote_md5: Some("old".to_string()),
                },
                RemoteResourceRecord {
                    root_id: "root".to_string(),
                    remote_path: "disk:/root/a.txt".to_string(),
                    resource_type: RemoteResourceType::File,
                    size_bytes: Some(42),
                    mtime_remote: None,
                    remote_md5: Some("new".to_string()),
                },
            ])
            .unwrap();
        repository
            .upsert_migration_maps(&[
                MigrationMapRecord {
                    root_id: "root".to_string(),
                    legacy_remote_path: "disk:/root/a.txt".to_string(),
                    canonical_remote_path: "disk:/root/a.txt".to_string(),
                    status: MigrationMapStatus::Adopted,
                    last_error: None,
                },
                MigrationMapRecord {
                    root_id: "root".to_string(),
                    legacy_remote_path: "disk:/root/a.txt".to_string(),
                    canonical_remote_path: "disk:/root/a.txt".to_string(),
                    status: MigrationMapStatus::Uploaded,
                    last_error: None,
                },
            ])
            .unwrap();
        repository
            .append_operations_succeeded(&[
                OperationRecord {
                    run_id: 7,
                    operation: "migration_adopt_remote_file".to_string(),
                    local_path: Some("a.txt".to_string()),
                    remote_path: Some("disk:/root/a.txt".to_string()),
                },
                OperationRecord {
                    run_id: 7,
                    operation: "migration_adopt_remote_file".to_string(),
                    local_path: Some("b.txt".to_string()),
                    remote_path: Some("disk:/root/b.txt".to_string()),
                },
            ])
            .unwrap();

        let file_count: i64 = repository
            .conn
            .query_row("SELECT COUNT(*) FROM file_state", [], |row| row.get(0))
            .unwrap();
        let file_size: i64 = repository
            .conn
            .query_row(
                "SELECT size_bytes FROM file_state WHERE relative_path = 'a.txt'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let remote = repository
            .conn
            .query_row(
                "SELECT size_bytes, remote_md5 FROM remote_resource_state WHERE remote_path = 'disk:/root/a.txt'",
                [],
                |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .unwrap();
        let migration_status: String = repository
            .conn
            .query_row(
                "SELECT status FROM migration_map_state WHERE canonical_remote_path = 'disk:/root/a.txt'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let operation_count: i64 = repository
            .conn
            .query_row(
                "SELECT COUNT(*) FROM operation_journal WHERE status = 'succeeded' AND finished_at_utc IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(file_count, 2);
        assert_eq!(file_size, 42);
        assert_eq!(remote, (Some(42), Some("new".to_string())));
        assert_eq!(migration_status, "uploaded");
        assert_eq!(operation_count, 2);
    }

    #[test]
    fn run_lock_cannot_be_taken_twice_and_can_be_released() {
        let repository = test_repository();
        let now = OffsetDateTime::UNIX_EPOCH;
        let stale_after = Duration::seconds(60);

        let first = repository
            .try_acquire_run_lock("owner-1", now, stale_after)
            .unwrap()
            .unwrap();
        let second = repository
            .try_acquire_run_lock("owner-2", now, stale_after)
            .unwrap();

        assert!(second.is_none());
        assert!(repository
            .heartbeat_run_lock(&first, now + Duration::seconds(5))
            .unwrap());
        assert!(repository.release_run_lock(&first).unwrap());
        assert!(repository
            .try_acquire_run_lock("owner-2", now + Duration::seconds(6), stale_after)
            .unwrap()
            .is_some());
    }

    #[test]
    fn stale_run_lock_is_replaced() {
        let repository = test_repository();
        let now = OffsetDateTime::UNIX_EPOCH;
        let stale_after = Duration::seconds(60);

        repository
            .try_acquire_run_lock("owner-1", now, stale_after)
            .unwrap()
            .unwrap();
        let replaced = repository
            .try_acquire_run_lock("owner-2", now + Duration::seconds(120), stale_after)
            .unwrap();

        assert!(replaced.is_some());
    }

    #[test]
    fn unknown_enum_values_are_typed_errors() {
        let error = SyncRunStatus::try_from("surprise").unwrap_err();

        assert_eq!(
            error,
            UnknownEnumValue {
                enum_name: "SyncRunStatus",
                value: "surprise".to_string()
            }
        );
    }

    #[test]
    fn missing_operation_finish_returns_not_found() {
        let repository = test_repository();
        let error = repository
            .finish_operation(404, OperationStatus::Succeeded, None, None)
            .unwrap_err();

        assert!(matches!(
            error,
            StateError::NotFound {
                entity: "operation",
                id: 404
            }
        ));
    }

    #[test]
    fn failed_items_upsert_by_operation_and_paths() {
        let repository = test_repository();
        let mut record = FailedItemRecord {
            root_id: "root".to_string(),
            local_path: Some("a.txt".to_string()),
            remote_path: Some("disk:/root/a.txt".to_string()),
            operation: "upload".to_string(),
            error_kind: "transient".to_string(),
            error_message: "one".to_string(),
            retry_count: 1,
            next_retry_at_utc: None,
        };

        repository.mark_failed(&record).unwrap();
        record.retry_count = 2;
        record.error_message = "two".to_string();
        repository.mark_failed(&record).unwrap();

        let stored: (i64, String, i64) = repository
            .conn
            .query_row(
                "SELECT retry_count, error_message, COUNT(*) OVER () FROM failed_items",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(stored.0, 2);
        assert_eq!(stored.1, "two");
        assert_eq!(stored.2, 1);
    }

    #[test]
    fn sync_run_status_reading_is_strict() {
        let repository = test_repository();
        let run_id = repository.begin_run(SyncRunTrigger::Cli).unwrap();
        repository
            .conn
            .execute(
                "UPDATE sync_runs SET status = 'unknown' WHERE id = ?1",
                params![run_id],
            )
            .unwrap();

        let stored: Option<String> = repository
            .conn
            .query_row(
                "SELECT status FROM sync_runs WHERE id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()
            .unwrap();

        assert!(matches!(
            SyncRunStatus::try_from(stored.unwrap().as_str()),
            Err(UnknownEnumValue { .. })
        ));
    }

    fn test_repository() -> StateRepository {
        StateRepository::open_in_memory_for_tests().unwrap()
    }

    fn file_record(root_id: &str, relative_path: &str, size_bytes: i64) -> FileStateRecord {
        FileStateRecord {
            root_id: root_id.to_string(),
            local_path: relative_path.to_string(),
            relative_path: relative_path.to_string(),
            remote_path: format!("disk:/root/{relative_path}"),
            normalized_remote_path: format!("disk:/root/{relative_path}"),
            size_bytes,
            mtime_ns: Some(1),
            ctime_ns: Some(2),
            fingerprint_kind: "metadata".to_string(),
            fingerprint_value: Some("fingerprint".to_string()),
            full_sha256: None,
            remote_etag: None,
            remote_md5: None,
            last_run_id: Some(1),
            status: FileStateStatus::Synced,
            last_error: None,
        }
    }
}
