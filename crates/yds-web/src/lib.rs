use std::{fmt, path::PathBuf};

use keyring_core::Entry;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use yds_core::{config::AppConfig, ComponentStatus};
use yds_state::{
    models::{
        OperationJournalRecord, RecentFailedItemRecord, RecentSkippedItemRecord, SyncRootRecord,
        SyncRunRecord,
    },
    StateRepository,
};

pub const COMPONENT_NAME: &str = "web";
pub const WEB_KEYRING_SERVICE: &str = "ya-disk-sync.web";
pub const WEB_KEYRING_ACCOUNT: &str = "web-ui";

#[derive(Debug, Error)]
pub enum WebError {
    #[error("keyring access failed: {0}")]
    Keyring(String),
    #[error("state error: {0}")]
    State(#[from] yds_state::StateError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct WebTokenStore {
    service_name: String,
}

impl fmt::Debug for WebTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebTokenStore")
            .field("service_name", &self.service_name)
            .finish_non_exhaustive()
    }
}

impl WebTokenStore {
    pub fn new(service_name: impl Into<String>) -> Result<Self, WebError> {
        keyring::use_native_store(false).map_err(map_keyring_error)?;
        Ok(Self {
            service_name: service_name.into(),
        })
    }

    pub fn default_store() -> Result<Self, WebError> {
        Self::new(WEB_KEYRING_SERVICE)
    }

    pub fn token_status(&self) -> Result<WebTokenStatus, WebError> {
        Ok(WebTokenStatus {
            configured: self.load_token()?.is_some(),
        })
    }

    pub fn load_token(&self) -> Result<Option<String>, WebError> {
        let entry = self.entry()?;
        match entry.get_password() {
            Ok(token) => Ok(Some(token)),
            Err(keyring_core::Error::NoEntry) => Ok(None),
            Err(error) => Err(map_keyring_error(error)),
        }
    }

    pub fn rotate_token(&self) -> Result<String, WebError> {
        let token = format!("yds-web-{}", uuid::Uuid::new_v4().simple());
        self.entry()?
            .set_password(&token)
            .map_err(map_keyring_error)?;
        Ok(token)
    }

    fn entry(&self) -> Result<Entry, WebError> {
        Entry::new(&self.service_name, WEB_KEYRING_ACCOUNT).map_err(map_keyring_error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebTokenStatus {
    pub configured: bool,
}

#[derive(Clone)]
pub enum WebAuth {
    Disabled,
    Bearer { token: String },
}

impl fmt::Debug for WebAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("WebAuth::Disabled"),
            Self::Bearer { .. } => formatter.write_str("WebAuth::Bearer { token: [REDACTED] }"),
        }
    }
}

impl WebAuth {
    #[must_use]
    pub fn disabled() -> Self {
        Self::Disabled
    }

    #[must_use]
    pub fn required_static(token: impl Into<String>) -> Self {
        Self::Bearer {
            token: token.into(),
        }
    }

    pub fn from_bind_address(bind_address: &str, store: &WebTokenStore) -> Result<Self, WebError> {
        if is_loopback_bind(bind_address) {
            return Ok(Self::Disabled);
        }

        let token = store.load_token()?.ok_or_else(|| {
            WebError::Keyring(
                "web token is required for non-loopback bind; run `ya-disk-sync web token rotate`"
                    .to_string(),
            )
        })?;
        Ok(Self::Bearer { token })
    }

    #[must_use]
    pub fn authorize_header(&self, authorization: Option<&str>) -> bool {
        match self {
            Self::Disabled => true,
            Self::Bearer { token } => authorization
                .and_then(|header| header.strip_prefix("Bearer "))
                .is_some_and(|provided| constant_time_eq(provided.as_bytes(), token.as_bytes())),
        }
    }

    #[must_use]
    pub const fn requires_auth(&self) -> bool {
        matches!(self, Self::Bearer { .. })
    }
}

#[derive(Debug, Clone)]
pub struct WebApp {
    config: AppConfig,
    config_path: Option<PathBuf>,
    auth: WebAuth,
}

impl WebApp {
    #[must_use]
    pub fn new(config: AppConfig, config_path: Option<PathBuf>, auth: WebAuth) -> Self {
        Self {
            config,
            config_path,
            auth,
        }
    }

    #[must_use]
    pub const fn auth(&self) -> &WebAuth {
        &self.auth
    }

    #[must_use]
    pub const fn config_path(&self) -> Option<&PathBuf> {
        self.config_path.as_ref()
    }

    pub fn state(&self, log_lines: usize) -> Result<WebUiState, WebError> {
        let repository = StateRepository::open(&self.config.paths.state_db)?;
        let roots = repository.list_sync_roots()?;
        let recent_runs = repository.list_recent_runs(20)?;
        let skipped = repository.list_recent_skipped_items(50)?;
        let failed = repository.list_recent_failed_items(50)?;
        let operations = repository.list_recent_operations(50)?;
        let log_lines = yds_service_logging_tail(&self.config.paths.logs_dir, log_lines);

        Ok(WebUiState {
            config: self.config.clone(),
            roots,
            recent_runs,
            skipped,
            failed,
            operations,
            log_lines,
            quota: QuotaView::Unavailable,
        })
    }

    pub fn render(&self, page: WebPage, state: &WebUiState, runtime: &RuntimeWebView) -> String {
        let title = page.title();
        let body = match page {
            WebPage::Dashboard => render_dashboard(state, runtime),
            WebPage::Roots => render_roots(state),
            WebPage::Exclusions => render_exclusions(state),
            WebPage::Schedule => render_schedule(state),
            WebPage::Config => render_config(state, self.config_path.as_ref()),
            WebPage::State => render_state(state),
            WebPage::Logs => render_logs(state),
            WebPage::FailedSkipped => render_failed_skipped(state),
            WebPage::Quota => render_quota(state),
        };
        render_shell(title, page, &body)
    }
}

#[derive(Debug, Clone)]
pub struct WebUiState {
    pub config: AppConfig,
    pub roots: Vec<SyncRootRecord>,
    pub recent_runs: Vec<SyncRunRecord>,
    pub skipped: Vec<RecentSkippedItemRecord>,
    pub failed: Vec<RecentFailedItemRecord>,
    pub operations: Vec<OperationJournalRecord>,
    pub log_lines: Vec<String>,
    pub quota: QuotaView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaView {
    Unavailable,
    Available {
        total_space: Option<u64>,
        used_space: Option<u64>,
        trash_size: Option<u64>,
        available_space: Option<u64>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeWebView {
    pub status: String,
    pub uptime_seconds: i64,
    pub current_run: Option<String>,
    pub last_successful_sync_at_utc: Option<String>,
    pub latest_run_status: Option<String>,
    pub metrics: MetricsWebView,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsWebView {
    pub scanned_files: i64,
    pub uploaded_files: i64,
    pub updated_files: i64,
    pub deleted_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
    pub bytes_uploaded: i64,
    pub last_duration_seconds: i64,
    pub average_upload_speed_bytes_per_second: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebPage {
    Dashboard,
    Roots,
    Exclusions,
    Schedule,
    Config,
    State,
    Logs,
    FailedSkipped,
    Quota,
}

impl WebPage {
    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Dashboard => "Dashboard",
            Self::Roots => "Sync roots",
            Self::Exclusions => "Exclusions",
            Self::Schedule => "Schedule",
            Self::Config => "Config",
            Self::State => "State",
            Self::Logs => "Logs",
            Self::FailedSkipped => "Failed / skipped",
            Self::Quota => "Remote quota",
        }
    }

    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::Dashboard => "/",
            Self::Roots => "/roots",
            Self::Exclusions => "/exclusions",
            Self::Schedule => "/schedule",
            Self::Config => "/config",
            Self::State => "/state",
            Self::Logs => "/logs",
            Self::FailedSkipped => "/failed-skipped",
            Self::Quota => "/quota",
        }
    }
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(COMPONENT_NAME, "web UI and API boundary is available")
}

fn yds_service_logging_tail(logs_dir: &str, lines: usize) -> Vec<String> {
    yds_service_logging_shim::tail_latest_log(logs_dir, lines).unwrap_or_default()
}

mod yds_service_logging_shim {
    pub fn tail_latest_log(logs_dir: &str, lines: usize) -> Result<Vec<String>, std::io::Error> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(logs_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                files.push(entry);
            }
        }
        files.sort_by_key(|entry| {
            entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });
        let Some(file) = files.pop() else {
            return Ok(Vec::new());
        };
        let content = std::fs::read_to_string(file.path())?;
        let all = content.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
        let start = all.len().saturating_sub(lines);
        Ok(all[start..].to_vec())
    }
}

fn render_shell(title: &str, active: WebPage, body: &str) -> String {
    let nav = [
        WebPage::Dashboard,
        WebPage::Roots,
        WebPage::Exclusions,
        WebPage::Schedule,
        WebPage::Config,
        WebPage::State,
        WebPage::Logs,
        WebPage::FailedSkipped,
        WebPage::Quota,
    ]
    .into_iter()
    .map(|page| {
        let class = if page == active {
            " class=\"active\""
        } else {
            ""
        };
        format!(
            "<a{class} href=\"{}\">{}</a>",
            page.path(),
            escape_html(page.title())
        )
    })
    .collect::<Vec<_>>()
    .join("");

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ya-disk-sync - {}</title>
<style>
:root {{ color-scheme: light; --bg:#f7f8fa; --panel:#ffffff; --text:#17202a; --muted:#697586; --line:#d9dee7; --accent:#1466c3; --danger:#b42318; --ok:#067647; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font:14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; color:var(--text); background:var(--bg); }}
header {{ display:flex; align-items:center; justify-content:space-between; padding:14px 24px; border-bottom:1px solid var(--line); background:#fff; }}
header h1 {{ margin:0; font-size:18px; font-weight:650; }}
nav {{ display:flex; gap:2px; padding:0 24px; border-bottom:1px solid var(--line); background:#fff; overflow:auto; }}
nav a {{ color:var(--muted); text-decoration:none; padding:10px 12px; border-bottom:2px solid transparent; white-space:nowrap; }}
nav a.active {{ color:var(--accent); border-color:var(--accent); }}
main {{ max-width:1180px; margin:0 auto; padding:20px 24px 40px; }}
.grid {{ display:grid; gap:12px; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); }}
.panel {{ background:var(--panel); border:1px solid var(--line); border-radius:6px; padding:14px; }}
.panel h2 {{ margin:0 0 10px; font-size:16px; }}
.metric {{ font-size:24px; font-weight:650; }}
.muted {{ color:var(--muted); }}
.ok {{ color:var(--ok); }}
.danger {{ color:var(--danger); }}
table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); }}
th,td {{ text-align:left; border-bottom:1px solid var(--line); padding:8px 10px; vertical-align:top; }}
th {{ color:var(--muted); font-weight:600; background:#fbfcfe; }}
pre, textarea {{ width:100%; overflow:auto; border:1px solid var(--line); border-radius:6px; background:#fff; padding:12px; font:12px/1.45 ui-monospace, SFMono-Regular, Consolas, monospace; }}
textarea {{ min-height:420px; }}
.actions {{ display:flex; gap:8px; flex-wrap:wrap; margin:0 0 14px; }}
button {{ border:1px solid var(--line); background:#fff; color:var(--text); border-radius:5px; padding:8px 10px; cursor:pointer; }}
button.primary {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
</style>
</head>
<body>
<header><h1>ya-disk-sync</h1><div>{}</div></header>
<nav>{nav}</nav>
<main>{body}</main>
</body>
</html>"#,
        escape_html(title),
        escape_html(title)
    )
}

fn render_dashboard(state: &WebUiState, runtime: &RuntimeWebView) -> String {
    format!(
        r#"<div class="actions">
<form method="post" action="/api/sync/run"><button class="primary" type="submit">Run now</button></form>
<form method="post" action="/api/sync/stop"><button type="submit">Stop</button></form>
</div>
<section class="grid">
<div class="panel"><h2>Status</h2><div class="metric">{}</div><div class="muted">uptime {} s</div></div>
<div class="panel"><h2>Last successful sync</h2><div>{}</div></div>
<div class="panel"><h2>Uploaded</h2><div class="metric">{}</div><div class="muted">{} bytes</div></div>
<div class="panel"><h2>Failed / skipped</h2><div class="metric">{} / {}</div></div>
</section>
<section class="panel"><h2>Latest run</h2>{}</section>
<section class="panel"><h2>Roots</h2><div class="metric">{}</div><div class="muted">configured roots</div></section>"#,
        escape_html(&runtime.status),
        runtime.uptime_seconds,
        runtime
            .last_successful_sync_at_utc
            .as_deref()
            .map(escape_html)
            .unwrap_or_else(|| "none".to_string()),
        runtime.metrics.uploaded_files,
        runtime.metrics.bytes_uploaded,
        runtime.metrics.failed_files,
        runtime.metrics.skipped_files,
        runtime
            .latest_run_status
            .as_deref()
            .map(escape_html)
            .unwrap_or_else(|| "none".to_string()),
        state.config.roots.len()
    )
}

fn render_roots(state: &WebUiState) -> String {
    let rows = if state.roots.is_empty() {
        state
            .config
            .roots
            .iter()
            .map(|root| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"muted\">not seen</td></tr>",
                    escape_html(&root.id),
                    escape_html(&root.local_path),
                    escape_html(root.remote_path_override.as_deref().unwrap_or("auto")),
                    root.enabled,
                )
            })
            .collect::<Vec<_>>()
            .join("")
    } else {
        state
            .roots
            .iter()
            .map(|root| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}{}</td></tr>",
                    escape_html(&root.id),
                    escape_html(&root.local_path),
                    escape_html(&root.canonical_remote_path),
                    root.enabled,
                    root.last_status
                        .as_deref()
                        .map(escape_html)
                        .unwrap_or_default(),
                    root.last_error
                        .as_deref()
                        .map(|error| format!(
                            "<br><span class=\"danger\">{}</span>",
                            escape_html(error)
                        ))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    format!(
        "<table><thead><tr><th>id</th><th>local path</th><th>remote path</th><th>enabled</th><th>status</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn render_exclusions(state: &WebUiState) -> String {
    format!(
        "<section class=\"grid\"><div class=\"panel\"><h2>Global excludes</h2><pre>{}</pre></div><div class=\"panel\"><h2>Absolute excludes</h2><pre>{}</pre></div></section>",
        escape_html(&state.config.global_excludes.join("\n")),
        escape_html(&state.config.absolute_excludes.join("\n"))
    )
}

fn render_schedule(state: &WebUiState) -> String {
    format!(
        "<section class=\"panel\"><h2>Schedule</h2><table><tbody><tr><th>enabled</th><td>{}</td></tr><tr><th>start UTC</th><td>{}</td></tr><tr><th>manual run</th><td>{}</td></tr></tbody></table></section>",
        state.config.schedule.enabled,
        escape_html(&state.config.schedule.start_time_utc),
        state.config.schedule.allow_manual_run
    )
}

fn render_config(state: &WebUiState, config_path: Option<&PathBuf>) -> String {
    let config_json = serde_json::to_string_pretty(&state.config).unwrap_or_default();
    format!(
        r#"<section class="panel"><h2>Config</h2><p class="muted">{} - changes require daemon restart.</p>
<form method="post" action="/api/config"><textarea name="config">{}</textarea><p><button class="primary" type="submit">Validate and save</button></p></form></section>"#,
        config_path
            .map(|path| escape_html(&path.display().to_string()))
            .unwrap_or_else(|| "config path unavailable".to_string()),
        escape_html(&config_json)
    )
}

fn render_state(state: &WebUiState) -> String {
    let rows = state
        .recent_runs
        .iter()
        .map(|run| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                run.id,
                escape_html(run.trigger.as_str()),
                escape_html(run.status.as_str()),
                run.summary.scanned_files,
                run.summary.failed_files
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(
        "<table><thead><tr><th>run</th><th>trigger</th><th>status</th><th>scanned</th><th>failed</th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

fn render_logs(state: &WebUiState) -> String {
    format!("<pre>{}</pre>", escape_html(&state.log_lines.join("\n")))
}

fn render_failed_skipped(state: &WebUiState) -> String {
    let failed = state
        .failed
        .iter()
        .map(|record| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&record.item.root_id),
                escape_html(&record.item.operation),
                escape_html(&record.item.error_kind),
                escape_html(&record.item.error_message)
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let skipped = state
        .skipped
        .iter()
        .map(|record| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&record.item.root_id),
                escape_html(&record.item.local_path),
                escape_html(&record.item.reason_code),
                record.item.size_bytes.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(
        "<section class=\"panel\"><h2>Failed</h2><table><tbody>{failed}</tbody></table></section><section class=\"panel\"><h2>Skipped</h2><table><tbody>{skipped}</tbody></table></section>"
    )
}

fn render_quota(state: &WebUiState) -> String {
    match &state.quota {
        QuotaView::Unavailable => {
            "<section class=\"panel\"><h2>Remote quota</h2><p class=\"muted\">unavailable</p></section>"
                .to_string()
        }
        QuotaView::Available {
            total_space,
            used_space,
            trash_size,
            available_space,
        } => format!(
            "<section class=\"panel\"><h2>Remote quota</h2><table><tbody><tr><th>total</th><td>{}</td></tr><tr><th>used</th><td>{}</td></tr><tr><th>trash</th><td>{}</td></tr><tr><th>available</th><td>{}</td></tr></tbody></table></section>",
            quota_value(*total_space),
            quota_value(*used_space),
            quota_value(*trash_size),
            quota_value(*available_space)
        ),
    }
}

fn quota_value(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

fn is_loopback_bind(bind_address: &str) -> bool {
    matches!(bind_address, "127.0.0.1" | "localhost" | "::1")
}

fn map_keyring_error(error: keyring_core::Error) -> WebError {
    WebError::Keyring(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use yds_core::config::default_config;
    use yds_core::ComponentHealth;

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), ComponentHealth::Ok);
    }

    #[test]
    fn bearer_auth_does_not_leak_token_in_debug() {
        let auth = WebAuth::required_static("secret-token");

        assert!(auth.authorize_header(Some("Bearer secret-token")));
        assert!(!auth.authorize_header(Some("Bearer wrong")));
        assert!(!format!("{auth:?}").contains("secret-token"));
    }

    #[test]
    fn loopback_auth_is_disabled() {
        let store = MemoryWebTokenStore::new("stored");
        let auth = auth_from_bind_for_tests("127.0.0.1", &store).unwrap();

        assert!(!auth.requires_auth());
        assert!(auth.authorize_header(None));
    }

    #[test]
    fn non_loopback_auth_requires_token() {
        let store = MemoryWebTokenStore::new("stored");
        let auth = auth_from_bind_for_tests("0.0.0.0", &store).unwrap();

        assert!(auth.requires_auth());
        assert!(auth.authorize_header(Some("Bearer stored")));
    }

    #[test]
    fn dashboard_html_contains_operational_controls() {
        let config = default_config();
        let app = WebApp::new(config.clone(), None, WebAuth::disabled());
        let state = WebUiState {
            config,
            roots: Vec::new(),
            recent_runs: Vec::new(),
            skipped: Vec::new(),
            failed: Vec::new(),
            operations: Vec::new(),
            log_lines: vec!["line".to_string()],
            quota: QuotaView::Unavailable,
        };
        let runtime = RuntimeWebView {
            status: "idle".to_string(),
            ..RuntimeWebView::default()
        };

        let html = app.render(WebPage::Dashboard, &state, &runtime);

        assert!(html.contains("Run now"));
        assert!(html.contains("Stop"));
        assert!(html.contains("Dashboard"));
    }

    #[test]
    fn quota_page_renders_available_values() {
        let config = default_config();
        let app = WebApp::new(config.clone(), None, WebAuth::disabled());
        let state = WebUiState {
            config,
            roots: Vec::new(),
            recent_runs: Vec::new(),
            skipped: Vec::new(),
            failed: Vec::new(),
            operations: Vec::new(),
            log_lines: Vec::new(),
            quota: QuotaView::Available {
                total_space: Some(100),
                used_space: Some(40),
                trash_size: Some(5),
                available_space: Some(60),
            },
        };

        let html = app.render(WebPage::Quota, &state, &RuntimeWebView::default());

        assert!(html.contains("Remote quota"));
        assert!(html.contains("<td>60</td>"));
    }

    struct MemoryWebTokenStore {
        token: Option<String>,
    }

    impl MemoryWebTokenStore {
        fn new(token: &str) -> Self {
            Self {
                token: Some(token.to_string()),
            }
        }
    }

    fn auth_from_bind_for_tests(
        bind_address: &str,
        store: &MemoryWebTokenStore,
    ) -> Result<WebAuth, WebError> {
        if is_loopback_bind(bind_address) {
            return Ok(WebAuth::Disabled);
        }
        Ok(WebAuth::Bearer {
            token: store
                .token
                .clone()
                .ok_or_else(|| WebError::Keyring("missing token".to_string()))?,
        })
    }
}
