use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownEnumValue {
    pub enum_name: &'static str,
    pub value: String,
}

impl fmt::Display for UnknownEnumValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} value: {}",
            self.enum_name, self.value
        )
    }
}

impl Error for UnknownEnumValue {}

macro_rules! db_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl TryFrom<&str> for $name {
            type Error = UnknownEnumValue;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(UnknownEnumValue {
                        enum_name: stringify!($name),
                        value: other.to_string(),
                    }),
                }
            }
        }
    };
}

db_enum! {
    pub enum SyncRunTrigger {
        Scheduled => "scheduled",
        Manual => "manual",
        Cli => "cli",
        Migration => "migration",
    }
}

db_enum! {
    pub enum SyncRunStatus {
        Running => "running",
        Succeeded => "succeeded",
        PartialFailed => "partial_failed",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

db_enum! {
    pub enum FileStateStatus {
        Synced => "synced",
        Failed => "failed",
        Skipped => "skipped",
        Deleted => "deleted",
        SyncedChangedDuringUpload => "synced_changed_during_upload",
    }
}

db_enum! {
    pub enum DirectoryStateStatus {
        Synced => "synced",
        Failed => "failed",
        Skipped => "skipped",
        Deleted => "deleted",
    }
}

db_enum! {
    pub enum RemoteResourceType {
        File => "file",
        Dir => "dir",
    }
}

db_enum! {
    pub enum OperationStatus {
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

db_enum! {
    pub enum MigrationMapStatus {
        Moved => "moved",
        Merged => "merged",
        Adopted => "adopted",
        Uploaded => "uploaded",
        Deleted => "deleted",
        Skipped => "skipped",
        Failed => "failed",
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncRunSummary {
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
pub struct SyncRunRecord {
    pub id: i64,
    pub started_at_utc: String,
    pub finished_at_utc: Option<String>,
    pub trigger: SyncRunTrigger,
    pub status: SyncRunStatus,
    pub summary: SyncRunSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRootRecord {
    pub id: String,
    pub local_path: String,
    pub remote_root: String,
    pub canonical_remote_path: String,
    pub enabled: bool,
    pub last_seen_at_utc: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootRemoteInventoryStatus {
    pub root_id: String,
    pub is_complete: bool,
    pub snapshot_completed_at_utc: Option<String>,
    pub snapshot_expires_at_utc: Option<String>,
    pub resource_count: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStateRecord {
    pub root_id: String,
    pub local_path: String,
    pub relative_path: String,
    pub remote_path: String,
    pub normalized_remote_path: String,
    pub size_bytes: i64,
    pub mtime_ns: Option<i64>,
    pub ctime_ns: Option<i64>,
    pub fingerprint_kind: String,
    pub fingerprint_value: Option<String>,
    pub full_sha256: Option<String>,
    pub remote_etag: Option<String>,
    pub remote_md5: Option<String>,
    pub last_run_id: Option<i64>,
    pub status: FileStateStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryStateRecord {
    pub root_id: String,
    pub local_path: String,
    pub relative_path: String,
    pub remote_path: String,
    pub normalized_remote_path: String,
    pub last_run_id: Option<i64>,
    pub status: DirectoryStateStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteResourceRecord {
    pub root_id: String,
    pub remote_path: String,
    pub resource_type: RemoteResourceType,
    pub size_bytes: Option<i64>,
    pub mtime_remote: Option<String>,
    pub remote_md5: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedItemRecord {
    pub root_id: String,
    pub local_path: String,
    pub relative_path: Option<String>,
    pub remote_path: Option<String>,
    pub reason_code: String,
    pub reason_details: Option<String>,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentSkippedItemRecord {
    pub item: SkippedItemRecord,
    pub last_seen_at_utc: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedItemRecord {
    pub root_id: String,
    pub local_path: Option<String>,
    pub remote_path: Option<String>,
    pub operation: String,
    pub error_kind: String,
    pub error_message: String,
    pub retry_count: i64,
    pub next_retry_at_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentFailedItemRecord {
    pub item: FailedItemRecord,
    pub last_failed_at_utc: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRecord {
    pub run_id: i64,
    pub operation: String,
    pub local_path: Option<String>,
    pub remote_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationJournalRecord {
    pub id: i64,
    pub run_id: i64,
    pub operation: String,
    pub local_path: Option<String>,
    pub remote_path: Option<String>,
    pub status: OperationStatus,
    pub started_at_utc: String,
    pub finished_at_utc: Option<String>,
    pub error_kind: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationMapRecord {
    pub root_id: String,
    pub legacy_remote_path: String,
    pub canonical_remote_path: String,
    pub status: MigrationMapStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedSubtreeRecord {
    pub root_id: String,
    pub relative_prefix: String,
    pub remote_prefix: String,
    pub last_run_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLockGuard {
    pub name: String,
    pub owner: String,
    pub acquired_at_utc: String,
}
