use std::{
    collections::{HashMap, HashSet},
    env, fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    exclusions::{ExclusionError, ExclusionMatcher},
    path_mapping::{
        canonical_remote_path, detect_remote_path_collision_key, is_valid_remote_root,
        sanitize_remote_path, PathMappingError,
    },
};

pub const CONFIG_VERSION: u32 = 1;
pub const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 50 * 1024 * 1024 * 1024;
pub const DEFAULT_FULL_HASH_MAX_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_LARGE_FILE_NAME_SIZE_ONLY_MIN_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;
pub const DEFAULT_SCAN_CONCURRENCY: usize = 0;
pub const DEFAULT_CREATE_DIRECTORY_CONCURRENCY: usize = 8;
pub const DEFAULT_RETRY_ATTEMPTS: u32 = 3;
pub const DEFAULT_RETRY_DELAY_SECONDS: u64 = 30;
pub const DEFAULT_REMOTE_INVENTORY_CACHE_TTL_HOURS: u64 = 24 * 7;

const fn default_create_directory_concurrency() -> usize {
    DEFAULT_CREATE_DIRECTORY_CONCURRENCY
}

const fn default_force_remote_rescan() -> bool {
    false
}

const fn default_remote_inventory_cache_ttl_hours() -> u64 {
    DEFAULT_REMOTE_INVENTORY_CACHE_TTL_HOURS
}
pub const DEFAULT_SCHEDULE_START_TIME_UTC: &str = "23:00";
pub const DEFAULT_WEB_UI_PORT: u16 = 17_691;
pub const DEFAULT_LOG_RETENTION_DAYS: u32 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppConfig {
    pub version: u32,
    pub instance: InstanceConfig,
    pub paths: PathsConfig,
    pub schedule: ScheduleConfig,
    pub sync: SyncConfig,
    pub yandex_disk: YandexDiskConfig,
    pub web_ui: WebUiConfig,
    pub logging: LoggingConfig,
    pub global_excludes: Vec<String>,
    pub absolute_excludes: Vec<String>,
    pub roots: Vec<SyncRootConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub name: String,
    pub remote_root: String,
    pub auto_include_hostname: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathsConfig {
    pub state_db: String,
    pub logs_dir: String,
    pub staging_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub enabled: bool,
    pub start_time_utc: String,
    pub run_missed_on_startup: bool,
    pub allow_manual_run: bool,
    pub allow_continuous_debug_mode: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncConfig {
    pub max_file_size_bytes: u64,
    pub full_hash_max_bytes: u64,
    pub large_file_name_size_only_min_bytes: u64,
    pub upload_concurrency: usize,
    #[serde(default = "default_create_directory_concurrency")]
    pub create_directory_concurrency: usize,
    pub scan_concurrency: usize,
    pub retry_attempts: u32,
    pub retry_delay_seconds: u64,
    #[serde(default = "default_force_remote_rescan")]
    pub force_remote_rescan: bool,
    #[serde(default = "default_remote_inventory_cache_ttl_hours")]
    pub remote_inventory_cache_ttl_hours: u64,
    pub delete_remote_extras: bool,
    pub delete_permanently: bool,
    pub delete_after_uploads: bool,
    pub dry_run_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct YandexDiskConfig {
    pub oauth_scope: String,
    pub account_alias: String,
    pub import_yacli_auth_if_available: bool,
    pub client_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebUiConfig {
    pub enabled: bool,
    pub bind_address: String,
    pub port: u16,
    pub require_auth_when_non_loopback: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub format: String,
    pub retention_days: u32,
    pub level: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRootConfig {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub local_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_path_override: Option<String>,
    #[serde(default)]
    pub legacy_remote_paths: Vec<String>,
    #[serde(default)]
    pub excludes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValidationReport {
    errors: Vec<ConfigValidationError>,
}

impl ConfigValidationReport {
    #[must_use]
    pub const fn new() -> Self {
        Self { errors: Vec::new() }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    #[must_use]
    pub fn errors(&self) -> &[ConfigValidationError] {
        &self.errors
    }

    fn push(
        &mut self,
        field: impl Into<String>,
        message: impl Into<String>,
        root_index: Option<usize>,
        rule_index: Option<usize>,
    ) {
        self.errors.push(ConfigValidationError {
            field: field.into(),
            message: message.into(),
            root_index,
            rule_index,
        });
    }
}

impl Default for ConfigValidationReport {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValidationError {
    pub field: String,
    pub message: String,
    pub root_index: Option<usize>,
    pub rule_index: Option<usize>,
}

#[derive(Debug, Error)]
pub enum ConfigIoError {
    #[error("failed to read config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to create config directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to serialize config: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to write config {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[must_use]
pub fn default_config() -> AppConfig {
    AppConfig {
        version: CONFIG_VERSION,
        instance: InstanceConfig {
            name: "local-workstation".to_string(),
            remote_root: "disk:/Backup".to_string(),
            auto_include_hostname: false,
        },
        paths: PathsConfig {
            state_db: r"C:\ProgramData\YaDiskSync\state\state.sqlite".to_string(),
            logs_dir: r"C:\ProgramData\YaDiskSync\logs".to_string(),
            staging_dir: r"C:\ProgramData\YaDiskSync\staging".to_string(),
        },
        schedule: ScheduleConfig {
            enabled: true,
            start_time_utc: DEFAULT_SCHEDULE_START_TIME_UTC.to_string(),
            run_missed_on_startup: false,
            allow_manual_run: true,
            allow_continuous_debug_mode: true,
        },
        sync: SyncConfig {
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
            full_hash_max_bytes: DEFAULT_FULL_HASH_MAX_BYTES,
            large_file_name_size_only_min_bytes: DEFAULT_LARGE_FILE_NAME_SIZE_ONLY_MIN_BYTES,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
            create_directory_concurrency: DEFAULT_CREATE_DIRECTORY_CONCURRENCY,
            scan_concurrency: DEFAULT_SCAN_CONCURRENCY,
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            retry_delay_seconds: DEFAULT_RETRY_DELAY_SECONDS,
            force_remote_rescan: false,
            remote_inventory_cache_ttl_hours: DEFAULT_REMOTE_INVENTORY_CACHE_TTL_HOURS,
            delete_remote_extras: true,
            delete_permanently: true,
            delete_after_uploads: true,
            dry_run_enabled: false,
        },
        yandex_disk: YandexDiskConfig {
            oauth_scope: "disk".to_string(),
            account_alias: "default".to_string(),
            import_yacli_auth_if_available: true,
            client_name: "ya-disk-sync".to_string(),
            oauth_client_id: None,
        },
        web_ui: WebUiConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: DEFAULT_WEB_UI_PORT,
            require_auth_when_non_loopback: true,
        },
        logging: LoggingConfig {
            format: "text".to_string(),
            retention_days: DEFAULT_LOG_RETENTION_DAYS,
            level: "info".to_string(),
        },
        global_excludes: default_global_excludes(),
        absolute_excludes: Vec::new(),
        roots: default_example_roots(),
    }
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig, ConfigIoError> {
    let path = path.as_ref();
    let content = fs::read_to_string(path).map_err(|source| ConfigIoError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    serde_json::from_str(&content).map_err(|source| ConfigIoError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn save_config(path: impl AsRef<Path>, config: &AppConfig) -> Result<(), ConfigIoError> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| ConfigIoError::CreateDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let mut content = serde_json::to_string_pretty(config)?;
    content.push('\n');
    fs::write(path, content).map_err(|source| ConfigIoError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[must_use]
pub fn resolve_config_path(explicit_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit_path {
        return path;
    }

    if let Ok(path) = env::var("YDS_CONFIG_PATH") {
        return PathBuf::from(path);
    }

    default_config_path()
}

#[must_use]
pub fn default_config_path() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"C:\ProgramData\YaDiskSync\config\config.json")
    } else {
        PathBuf::from("/etc/ya-disk-sync/config.json")
    }
}

#[must_use]
pub fn validate_config(config: &AppConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::new();

    validate_top_level(config, &mut report);
    validate_exclude_rules(
        "global_excludes",
        &config.global_excludes,
        None,
        &mut report,
    );
    validate_roots(config, &mut report);

    report
}

#[must_use]
pub fn default_global_excludes() -> Vec<String> {
    [
        "**/__pycache__/**",
        "**/.pytest_cache/**",
        "**/~$*",
        "**/*.tmp",
        "**/*.temp",
        "**/.DS_Store",
        "**/Thumbs.db",
        "**/*.sqlite-wal",
        "**/*.sqlite-shm",
        "**/*.db-wal",
        "**/*.db-shm",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn default_example_roots() -> Vec<SyncRootConfig> {
    vec![
        root("projects", "Рабочие проекты", r"C:\Data\Projects", &[]),
        root("documents", "Документы", r"C:\Data\Documents", &[]),
    ]
}

fn root(id: &str, name: &str, local_path: &str, legacy_remote_paths: &[&str]) -> SyncRootConfig {
    SyncRootConfig {
        id: id.to_string(),
        name: name.to_string(),
        enabled: false,
        local_path: local_path.to_string(),
        remote_path_override: None,
        legacy_remote_paths: legacy_remote_paths
            .iter()
            .map(|path| (*path).to_string())
            .collect(),
        excludes: Vec::new(),
    }
}

fn validate_top_level(config: &AppConfig, report: &mut ConfigValidationReport) {
    if config.version != CONFIG_VERSION {
        report.push("version", "must be 1", None, None);
    }
    if config.instance.name.trim().is_empty() {
        report.push("instance.name", "must not be empty", None, None);
    }
    if !is_valid_remote_root(&config.instance.remote_root) {
        report.push(
            "instance.remote_root",
            "must be a valid disk:/ remote root",
            None,
            None,
        );
    }
    if config.instance.auto_include_hostname {
        report.push(
            "instance.auto_include_hostname",
            "must be false in config v1",
            None,
            None,
        );
    }
    validate_non_empty("paths.state_db", &config.paths.state_db, report);
    validate_non_empty("paths.logs_dir", &config.paths.logs_dir, report);
    validate_non_empty("paths.staging_dir", &config.paths.staging_dir, report);
    if !is_valid_utc_time(&config.schedule.start_time_utc) {
        report.push(
            "schedule.start_time_utc",
            "must use HH:MM UTC format",
            None,
            None,
        );
    }
    if config.schedule.run_missed_on_startup {
        report.push(
            "schedule.run_missed_on_startup",
            "must be false in config v1",
            None,
            None,
        );
    }
    validate_sync_config(&config.sync, report);
    if config.yandex_disk.oauth_scope != "disk" {
        report.push("yandex_disk.oauth_scope", "must be disk", None, None);
    }
    validate_non_empty(
        "yandex_disk.account_alias",
        &config.yandex_disk.account_alias,
        report,
    );
    validate_non_empty(
        "yandex_disk.client_name",
        &config.yandex_disk.client_name,
        report,
    );
    if let Some(client_id) = &config.yandex_disk.oauth_client_id {
        validate_non_empty("yandex_disk.oauth_client_id", client_id, report);
    }
    if !is_loopback_bind(&config.web_ui.bind_address)
        && !config.web_ui.require_auth_when_non_loopback
    {
        report.push(
            "web_ui.require_auth_when_non_loopback",
            "must be true when bind_address is not loopback",
            None,
            None,
        );
    }
    if config.logging.format != "text" {
        report.push("logging.format", "must be text", None, None);
    }
    if config.logging.retention_days == 0 {
        report.push(
            "logging.retention_days",
            "must be greater than 0",
            None,
            None,
        );
    }
    if !["trace", "debug", "info", "warn", "error"].contains(&config.logging.level.as_str()) {
        report.push(
            "logging.level",
            "must be one of trace, debug, info, warn, error",
            None,
            None,
        );
    }
}

fn validate_sync_config(sync: &SyncConfig, report: &mut ConfigValidationReport) {
    if sync.max_file_size_bytes == 0 {
        report.push(
            "sync.max_file_size_bytes",
            "must be greater than 0",
            None,
            None,
        );
    }
    if sync.full_hash_max_bytes == 0 {
        report.push(
            "sync.full_hash_max_bytes",
            "must be greater than 0",
            None,
            None,
        );
    }
    if sync.large_file_name_size_only_min_bytes <= sync.full_hash_max_bytes {
        report.push(
            "sync.large_file_name_size_only_min_bytes",
            "must be greater than full_hash_max_bytes",
            None,
            None,
        );
    }
    if sync.upload_concurrency == 0 {
        report.push(
            "sync.upload_concurrency",
            "must be greater than 0",
            None,
            None,
        );
    }
    if sync.create_directory_concurrency == 0 {
        report.push(
            "sync.create_directory_concurrency",
            "must be greater than 0",
            None,
            None,
        );
    }
    if sync.retry_attempts == 0 {
        report.push("sync.retry_attempts", "must be greater than 0", None, None);
    }
    if sync.retry_delay_seconds == 0 {
        report.push(
            "sync.retry_delay_seconds",
            "must be greater than 0",
            None,
            None,
        );
    }
    if sync.remote_inventory_cache_ttl_hours == 0 {
        report.push(
            "sync.remote_inventory_cache_ttl_hours",
            "must be greater than 0",
            None,
            None,
        );
    }
    if !sync.delete_remote_extras {
        report.push(
            "sync.delete_remote_extras",
            "must be true in v1",
            None,
            None,
        );
    }
    if !sync.delete_permanently {
        report.push("sync.delete_permanently", "must be true in v1", None, None);
    }
    if !sync.delete_after_uploads {
        report.push(
            "sync.delete_after_uploads",
            "must be true in v1",
            None,
            None,
        );
    }
    if sync.dry_run_enabled {
        report.push("sync.dry_run_enabled", "must be false in v1", None, None);
    }
}

fn validate_roots(config: &AppConfig, report: &mut ConfigValidationReport) {
    if config.roots.is_empty() {
        report.push("roots", "must contain at least one root", None, None);
        return;
    }

    let mut ids = HashSet::new();
    let mut canonical_paths: HashMap<String, usize> = HashMap::new();

    for (index, root) in config.roots.iter().enumerate() {
        if root.id.trim().is_empty() {
            report.push("roots[].id", "must not be empty", Some(index), None);
        } else if !ids.insert(root.id.clone()) {
            report.push("roots[].id", "duplicate root id", Some(index), None);
        }
        if root.name.trim().is_empty() {
            report.push("roots[].name", "must not be empty", Some(index), None);
        }
        if root.local_path.trim().is_empty() {
            report.push("roots[].local_path", "must not be empty", Some(index), None);
        }
        validate_exclude_rules("roots[].excludes", &root.excludes, Some(index), report);

        validate_legacy_paths(root, index, report);
        validate_canonical_path(config, root, index, &mut canonical_paths, report);
    }
}

fn validate_legacy_paths(
    root: &SyncRootConfig,
    root_index: usize,
    report: &mut ConfigValidationReport,
) {
    for (rule_index, legacy_path) in root.legacy_remote_paths.iter().enumerate() {
        if let Err(error) = sanitize_remote_path(legacy_path) {
            report.push(
                "roots[].legacy_remote_paths",
                format_path_error(error),
                Some(root_index),
                Some(rule_index),
            );
        }
    }
}

fn validate_canonical_path(
    config: &AppConfig,
    root: &SyncRootConfig,
    root_index: usize,
    canonical_paths: &mut HashMap<String, usize>,
    report: &mut ConfigValidationReport,
) {
    match canonical_remote_path(
        &config.instance.remote_root,
        &root.local_path,
        root.remote_path_override.as_deref(),
    )
    .and_then(|path| detect_remote_path_collision_key(&path))
    {
        Ok(key) => {
            if let Some(previous_index) = canonical_paths.insert(key, root_index) {
                report.push(
                    "roots[].local_path",
                    format!("canonical remote path collides with root index {previous_index}"),
                    Some(root_index),
                    None,
                );
            }
        }
        Err(error) => {
            report.push(
                "roots[].local_path",
                format_path_error(error),
                Some(root_index),
                None,
            );
        }
    }
}

fn validate_exclude_rules(
    field: &str,
    rules: &[String],
    root_index: Option<usize>,
    report: &mut ConfigValidationReport,
) {
    for (index, rule) in rules.iter().enumerate() {
        if let Err(error) = ExclusionMatcher::new(std::slice::from_ref(rule), &[], &[]) {
            report.push(
                field,
                format_exclusion_error(error),
                root_index,
                Some(index),
            );
        }
    }
}

fn validate_non_empty(field: &str, value: &str, report: &mut ConfigValidationReport) {
    if value.trim().is_empty() {
        report.push(field, "must not be empty", None, None);
    }
}

fn is_valid_utc_time(value: &str) -> bool {
    let Some((hour, minute)) = value.split_once(':') else {
        return false;
    };
    if hour.len() != 2 || minute.len() != 2 {
        return false;
    }

    let Ok(hour) = hour.parse::<u8>() else {
        return false;
    };
    let Ok(minute) = minute.parse::<u8>() else {
        return false;
    };

    hour < 24 && minute < 60
}

fn is_loopback_bind(value: &str) -> bool {
    matches!(value, "127.0.0.1" | "localhost" | "::1")
}

fn format_path_error(error: PathMappingError) -> String {
    error.to_string()
}

fn format_exclusion_error(error: ExclusionError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates_and_round_trips() {
        let config = default_config();
        assert!(validate_config(&config).is_valid());

        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: AppConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, config);
        assert_eq!(parsed.roots.len(), 2);
        assert!(parsed.roots.iter().all(|root| !root.enabled));
    }

    #[test]
    fn legacy_config_without_new_sync_fields_uses_defaults() {
        let config = default_config();
        let mut json = serde_json::to_value(&config).unwrap();
        let sync = json
            .get_mut("sync")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        sync.remove("create_directory_concurrency");
        sync.remove("force_remote_rescan");
        sync.remove("remote_inventory_cache_ttl_hours");

        let parsed: AppConfig = serde_json::from_value(json).unwrap();

        assert_eq!(
            parsed.sync.create_directory_concurrency,
            DEFAULT_CREATE_DIRECTORY_CONCURRENCY
        );
        assert!(!parsed.sync.force_remote_rescan);
        assert_eq!(
            parsed.sync.remote_inventory_cache_ttl_hours,
            DEFAULT_REMOTE_INVENTORY_CACHE_TTL_HOURS
        );
    }

    #[test]
    fn config_can_be_saved_and_loaded() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("config").join("config.json");
        let config = default_config();

        save_config(&path, &config).unwrap();
        let loaded = load_config(&path).unwrap();

        assert_eq!(loaded, config);
    }

    #[test]
    fn invalid_top_level_values_are_reported() {
        let mut config = default_config();
        config.version = 2;
        config.instance.remote_root = "app:/bad".to_string();
        config.schedule.start_time_utc = "25:00".to_string();
        config.sync.delete_remote_extras = false;
        config.sync.create_directory_concurrency = 0;
        config.sync.dry_run_enabled = true;
        config.sync.remote_inventory_cache_ttl_hours = 0;
        config.web_ui.bind_address = "0.0.0.0".to_string();
        config.web_ui.require_auth_when_non_loopback = false;

        let report = validate_config(&config);

        assert!(!report.is_valid());
        assert_has_field(&report, "version");
        assert_has_field(&report, "instance.remote_root");
        assert_has_field(&report, "schedule.start_time_utc");
        assert_has_field(&report, "sync.create_directory_concurrency");
        assert_has_field(&report, "sync.delete_remote_extras");
        assert_has_field(&report, "sync.dry_run_enabled");
        assert_has_field(&report, "sync.remote_inventory_cache_ttl_hours");
        assert_has_field(&report, "web_ui.require_auth_when_non_loopback");
    }

    #[test]
    fn duplicate_root_ids_are_reported() {
        let mut config = default_config();
        config.roots[1].id = config.roots[0].id.clone();

        let report = validate_config(&config);

        assert_has_field(&report, "roots[].id");
    }

    #[test]
    fn duplicate_canonical_paths_are_reported() {
        let mut config = default_config();
        config.roots[1].local_path = config.roots[0].local_path.clone();

        let report = validate_config(&config);

        assert_has_field(&report, "roots[].local_path");
    }

    #[test]
    fn duplicate_canonical_paths_are_case_insensitive() {
        let mut config = default_config();
        config.roots.truncate(2);
        config.roots[0].local_path = r"C:\Data\CaseCollision".to_string();
        config.roots[1].local_path = r"C:\Data\casecollision".to_string();

        let report = validate_config(&config);

        assert_has_field(&report, "roots[].local_path");
    }

    #[test]
    fn remote_path_override_can_create_canonical_collision() {
        let mut config = default_config();
        config.roots[1].remote_path_override = Some("disk:/Backup/C/Data/Projects".to_string());

        let report = validate_config(&config);

        assert_has_field(&report, "roots[].local_path");
    }

    #[test]
    fn invalid_exclude_rule_is_reported() {
        let mut config = default_config();
        config.roots[0].excludes.push("bad\0rule".to_string());

        let report = validate_config(&config);

        assert_has_field(&report, "roots[].excludes");
    }

    #[test]
    fn default_excludes_match_spec_and_do_not_exclude_secrets() {
        let excludes = default_global_excludes();

        assert!(excludes.contains(&"**/*.sqlite-wal".to_string()));
        assert!(excludes.contains(&"**/*.db-shm".to_string()));
        assert!(!excludes.contains(&".env".to_string()));
        assert!(!excludes.contains(&"*.pem".to_string()));
        assert!(!excludes.contains(&"node_modules".to_string()));
        assert!(!excludes.contains(&"target".to_string()));
    }

    fn assert_has_field(report: &ConfigValidationReport, field: &str) {
        assert!(
            report.errors().iter().any(|error| error.field == field),
            "expected validation error for {field}, got {:?}",
            report.errors()
        );
    }
}
