pub mod logging;

use std::{
    future::Future, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc,
    time::Duration as StdDuration,
};

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime, Time};
use tokio::{
    net::TcpListener,
    sync::{broadcast, Mutex},
};
use tracing::{error, info, warn};
use yds_core::{
    config::{load_config, save_config, validate_config, AppConfig},
    ComponentStatus, APP_VERSION,
};
use yds_state::{
    models::{SyncRunRecord, SyncRunStatus, SyncRunSummary, SyncRunTrigger},
    StateError, StateRepository,
};
use yds_sync::{CancellationToken, SyncEngine, SyncError, SyncRunOptions, SyncRunReport};
use yds_web::{
    MetricsWebView, QuotaView, RuntimeWebView, WebApp, WebAuth, WebError, WebPage, WebTokenStore,
};
use yds_yandex_disk::{
    auth::{KeyringTokenStore, TokenStore},
    error::YandexDiskError,
    models::DiskInfo,
    HttpYandexDiskClient, RetryPolicy, YandexDiskClient,
};

pub const COMPONENT_NAME: &str = "service";

type SyncTaskFuture = Pin<Box<dyn Future<Output = Result<SyncRunReport, ServiceError>> + Send>>;

pub trait RuntimeSyncTask: Send + Sync {
    fn run(&self, trigger: SyncRunTrigger, cancellation: CancellationToken) -> SyncTaskFuture;
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("config I/O error: {0}")]
    ConfigIo(#[from] yds_core::config::ConfigIoError),
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("sync error: {0}")]
    Sync(#[from] SyncError),
    #[error("Yandex Disk error: {0}")]
    YandexDisk(#[from] YandexDiskError),
    #[error("web error: {0}")]
    Web(#[from] WebError),
    #[error("logging error: {0}")]
    Logging(#[from] logging::LoggingError),
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("invalid scheduler time: {0}")]
    InvalidScheduleTime(String),
    #[error("invalid bind address: {0}")]
    InvalidBindAddress(String),
    #[error("config validation failed: {0}")]
    ConfigValidation(String),
}

pub struct RuntimeHostOptions {
    pub config_path: PathBuf,
}

impl RuntimeHostOptions {
    #[must_use]
    pub fn new(config_path: impl Into<PathBuf>) -> Self {
        Self {
            config_path: config_path.into(),
        }
    }
}

pub struct RuntimeHost {
    runtime: ServiceRuntime,
    local_addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
    server_task: tokio::task::JoinHandle<Result<(), ServiceError>>,
    scheduler_task: tokio::task::JoinHandle<Result<(), ServiceError>>,
    _log_guard: logging::FileLogGuard,
}

impl RuntimeHost {
    pub async fn start(options: RuntimeHostOptions) -> Result<Self, ServiceError> {
        let config = load_config(&options.config_path)?;
        let report = validate_config(&config);
        if !report.is_valid() {
            let errors = report
                .errors()
                .iter()
                .map(|error| format!("{}: {}", error.field, error.message))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ServiceError::ConfigValidation(errors));
        }
        logging::cleanup_old_logs(&config.paths.logs_dir, config.logging.retention_days)?;
        let log_guard = logging::init_file_logging(&config.paths.logs_dir, &config.logging.level)?;

        info!(
            config_path = %options.config_path.display(),
            "runtime host starting"
        );
        let runtime = ServiceRuntime::with_engine(config.clone())?;
        let server = ControlServer::new(
            runtime.clone(),
            config.web_ui.bind_address.clone(),
            config.web_ui.port,
        )
        .with_config_path(options.config_path)
        .bind()
        .await?;
        let local_addr = server.local_addr();

        let (shutdown_tx, _) = broadcast::channel::<()>(4);
        let mut server_shutdown = shutdown_tx.subscribe();
        let server_task = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async move {
                    let _ = server_shutdown.recv().await;
                }))
                .await
        });
        let mut scheduler_shutdown = shutdown_tx.subscribe();
        let scheduler_runtime = runtime.clone();
        let scheduler_task = tokio::spawn(async move {
            scheduler_runtime
                .run_scheduler_until(Box::pin(async move {
                    let _ = scheduler_shutdown.recv().await;
                }))
                .await
        });

        Ok(Self {
            runtime,
            local_addr,
            shutdown_tx,
            server_task,
            scheduler_task,
            _log_guard: log_guard,
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub const fn runtime(&self) -> &ServiceRuntime {
        &self.runtime
    }

    pub async fn shutdown(self) -> Result<(), ServiceError> {
        info!("runtime host shutdown requested");
        let _ = self.runtime.request_cancel().await;
        let _ = self.shutdown_tx.send(());
        self.server_task.await??;
        self.scheduler_task.await??;
        info!("runtime host stopped");
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Idle,
    Running,
    Cancelling,
    Failed,
    Degraded,
}

impl RuntimeStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Failed => "failed",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentRunSnapshot {
    pub trigger: String,
    pub started_at_utc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub id: i64,
    pub started_at_utc: String,
    pub finished_at_utc: Option<String>,
    pub trigger: String,
    pub status: String,
    pub summary: SyncRunSummaryJson,
}

impl From<SyncRunRecord> for RunSnapshot {
    fn from(value: SyncRunRecord) -> Self {
        Self {
            id: value.id,
            started_at_utc: value.started_at_utc,
            finished_at_utc: value.finished_at_utc,
            trigger: value.trigger.as_str().to_string(),
            status: value.status.as_str().to_string(),
            summary: value.summary.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRunSummaryJson {
    pub scanned_files: i64,
    pub uploaded_files: i64,
    pub updated_files: i64,
    pub deleted_files: i64,
    pub skipped_files: i64,
    pub failed_files: i64,
    pub bytes_uploaded: i64,
    pub error_summary: Option<String>,
}

impl From<SyncRunSummary> for SyncRunSummaryJson {
    fn from(value: SyncRunSummary) -> Self {
        Self {
            scanned_files: value.scanned_files,
            uploaded_files: value.uploaded_files,
            updated_files: value.updated_files,
            deleted_files: value.deleted_files,
            skipped_files: value.skipped_files,
            failed_files: value.failed_files,
            bytes_uploaded: value.bytes_uploaded,
            error_summary: value.error_summary,
        }
    }
}

impl From<&SyncRunSummary> for SyncRunSummaryJson {
    fn from(value: &SyncRunSummary) -> Self {
        value.clone().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub status: RuntimeStatus,
    pub uptime_seconds: i64,
    pub current_run: Option<CurrentRunSnapshot>,
    pub latest_run: Option<RunSnapshot>,
    pub latest_error: Option<String>,
    pub scheduled_skips: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeMetrics {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: i64,
    pub current_run: Option<CurrentRunSnapshot>,
    pub last_successful_sync_at_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCommandResponse {
    pub accepted: bool,
    pub status: RuntimeStatus,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct DailyUtcSchedule {
    start_time: Time,
}

impl DailyUtcSchedule {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        let Some((hour, minute)) = value.split_once(':') else {
            return Err(ServiceError::InvalidScheduleTime(value.to_string()));
        };
        let hour = hour
            .parse::<u8>()
            .map_err(|_| ServiceError::InvalidScheduleTime(value.to_string()))?;
        let minute = minute
            .parse::<u8>()
            .map_err(|_| ServiceError::InvalidScheduleTime(value.to_string()))?;
        let start_time = Time::from_hms(hour, minute, 0)
            .map_err(|_| ServiceError::InvalidScheduleTime(value.to_string()))?;
        Ok(Self { start_time })
    }

    #[must_use]
    pub fn next_after(&self, now: OffsetDateTime) -> OffsetDateTime {
        let candidate = now.date().with_time(self.start_time).assume_utc();
        if candidate > now {
            candidate
        } else {
            candidate + Duration::days(1)
        }
    }
}

#[derive(Clone)]
pub struct ServiceRuntime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    config: AppConfig,
    started_at_utc: OffsetDateTime,
    task: Arc<dyn RuntimeSyncTask>,
    state: Mutex<RuntimeState>,
}

#[derive(Debug, Clone)]
struct RuntimeState {
    status: RuntimeStatus,
    current_run: Option<CurrentRunSnapshot>,
    latest_run: Option<RunSnapshot>,
    latest_error: Option<String>,
    metrics: RuntimeMetrics,
    cancellation: Option<CancellationToken>,
    scheduled_skips: i64,
}

impl ServiceRuntime {
    pub fn new(config: AppConfig, task: Arc<dyn RuntimeSyncTask>) -> Result<Self, ServiceError> {
        let latest_run = latest_run_from_state(&config).ok().flatten();
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                config,
                started_at_utc: OffsetDateTime::now_utc(),
                task,
                state: Mutex::new(RuntimeState {
                    status: RuntimeStatus::Idle,
                    current_run: None,
                    latest_run,
                    latest_error: None,
                    metrics: RuntimeMetrics::default(),
                    cancellation: None,
                    scheduled_skips: 0,
                }),
            }),
        })
    }

    pub fn with_engine(config: AppConfig) -> Result<Self, ServiceError> {
        Self::new(config.clone(), Arc::new(EngineSyncTask { config }))
    }

    pub async fn request_run(&self, trigger: SyncRunTrigger) -> RuntimeCommandResponse {
        let cancellation = CancellationToken::new();
        {
            let mut state = self.inner.state.lock().await;
            if matches!(
                state.status,
                RuntimeStatus::Running | RuntimeStatus::Cancelling
            ) {
                if trigger == SyncRunTrigger::Scheduled {
                    state.scheduled_skips += 1;
                }
                warn!(
                    trigger = trigger.as_str(),
                    "sync run request skipped: already running"
                );
                return RuntimeCommandResponse {
                    accepted: false,
                    status: state.status,
                    message: "already_running".to_string(),
                };
            }
            state.status = RuntimeStatus::Running;
            state.current_run = Some(CurrentRunSnapshot {
                trigger: trigger.as_str().to_string(),
                started_at_utc: utc_text(OffsetDateTime::now_utc()),
            });
            state.latest_error = None;
            state.cancellation = Some(cancellation.clone());
        }

        let runtime = self.clone();
        let task = Arc::clone(&self.inner.task);
        tokio::spawn(async move {
            let started = OffsetDateTime::now_utc();
            info!(trigger = trigger.as_str(), "sync run started");
            let result = task.run(trigger, cancellation).await;
            runtime.finish_run(trigger, started, result).await;
        });

        RuntimeCommandResponse {
            accepted: true,
            status: RuntimeStatus::Running,
            message: "started".to_string(),
        }
    }

    pub async fn request_cancel(&self) -> RuntimeCommandResponse {
        let mut state = self.inner.state.lock().await;
        let Some(cancellation) = &state.cancellation else {
            return RuntimeCommandResponse {
                accepted: false,
                status: state.status,
                message: "not_running".to_string(),
            };
        };

        cancellation.cancel();
        state.status = RuntimeStatus::Cancelling;
        info!("sync cancellation requested");
        RuntimeCommandResponse {
            accepted: true,
            status: RuntimeStatus::Cancelling,
            message: "cancelling".to_string(),
        }
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        let latest_from_state = latest_run_from_state(&self.inner.config).ok().flatten();
        let mut state = self.inner.state.lock().await;
        if latest_from_state.is_some() {
            state.latest_run = latest_from_state;
        }
        RuntimeSnapshot {
            status: state.status,
            uptime_seconds: uptime_seconds(self.inner.started_at_utc),
            current_run: state.current_run.clone(),
            latest_run: state.latest_run.clone(),
            latest_error: state.latest_error.clone(),
            scheduled_skips: state.scheduled_skips,
        }
    }

    pub async fn metrics(&self) -> RuntimeMetrics {
        self.inner.state.lock().await.metrics.clone()
    }

    pub async fn health(&self) -> HealthResponse {
        let snapshot = self.snapshot().await;
        let last_successful_sync_at_utc = latest_success_from_state(&self.inner.config)
            .ok()
            .flatten()
            .and_then(|record| record.finished_at_utc);
        HealthResponse {
            status: match snapshot.status {
                RuntimeStatus::Failed => "failed",
                RuntimeStatus::Degraded => "degraded",
                _ => "ok",
            }
            .to_string(),
            version: APP_VERSION.to_string(),
            uptime_seconds: snapshot.uptime_seconds,
            current_run: snapshot.current_run,
            last_successful_sync_at_utc,
        }
    }

    pub async fn run_scheduler_until(
        &self,
        mut shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Result<(), ServiceError> {
        if !self.inner.config.schedule.enabled {
            shutdown.as_mut().await;
            return Ok(());
        }

        let schedule = DailyUtcSchedule::parse(&self.inner.config.schedule.start_time_utc)?;
        loop {
            let now = OffsetDateTime::now_utc();
            let next = schedule.next_after(now);
            let sleep_duration = std_duration_until(now, next);
            tokio::select! {
                () = tokio::time::sleep(sleep_duration) => {
                    let response = self.request_run(SyncRunTrigger::Scheduled).await;
                    if !response.accepted {
                        warn!(message = response.message, "scheduled sync skipped");
                    }
                }
                () = shutdown.as_mut() => return Ok(()),
            }
        }
    }

    async fn finish_run(
        &self,
        trigger: SyncRunTrigger,
        started: OffsetDateTime,
        result: Result<SyncRunReport, ServiceError>,
    ) {
        let finished = OffsetDateTime::now_utc();
        let mut state = self.inner.state.lock().await;
        state.current_run = None;
        state.cancellation = None;

        match result {
            Ok(report) => {
                info!(
                    trigger = trigger.as_str(),
                    status = report.status.as_str(),
                    scanned_files = report.summary.scanned_files,
                    uploaded_files = report.summary.uploaded_files,
                    updated_files = report.summary.updated_files,
                    deleted_files = report.summary.deleted_files,
                    skipped_files = report.summary.skipped_files,
                    failed_files = report.summary.failed_files,
                    bytes_uploaded = report.summary.bytes_uploaded,
                    "sync run finished"
                );
                state.metrics = metrics_from_report(&report, started, finished);
                state.latest_run = Some(RunSnapshot {
                    id: report.run_id,
                    started_at_utc: utc_text(started),
                    finished_at_utc: Some(utc_text(finished)),
                    trigger: trigger.as_str().to_string(),
                    status: report.status.as_str().to_string(),
                    summary: (&report.summary).into(),
                });
                state.latest_error = report.summary.error_summary.clone();
                state.status = match report.status {
                    SyncRunStatus::Succeeded | SyncRunStatus::Cancelled => RuntimeStatus::Idle,
                    SyncRunStatus::PartialFailed => RuntimeStatus::Degraded,
                    SyncRunStatus::Failed => RuntimeStatus::Failed,
                    SyncRunStatus::Running => RuntimeStatus::Running,
                };
            }
            Err(error) => {
                error!(error = %error, "sync run failed");
                state.latest_error = Some(error.to_string());
                state.status = RuntimeStatus::Failed;
            }
        }
    }
}

#[derive(Clone)]
pub struct EngineSyncTask {
    config: AppConfig,
}

impl RuntimeSyncTask for EngineSyncTask {
    fn run(&self, trigger: SyncRunTrigger, cancellation: CancellationToken) -> SyncTaskFuture {
        let config = self.config.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(async move {
                    let token_store: Arc<dyn TokenStore> =
                        Arc::new(KeyringTokenStore::default_store()?);
                    let repository = StateRepository::open(&config.paths.state_db)?;
                    let client = HttpYandexDiskClient::new(
                        config.yandex_disk.account_alias.clone(),
                        token_store,
                        RetryPolicy::from_config(&config.sync),
                    )?;
                    let engine = SyncEngine::new(&config, &repository, Arc::new(client));
                    let options = SyncRunOptions {
                        trigger,
                        ..SyncRunOptions::default()
                    };
                    Ok(engine.run_once(options, &cancellation).await?)
                })
            })
            .await?
        })
    }
}

pub struct ControlServer {
    runtime: ServiceRuntime,
    bind_address: String,
    port: u16,
    config_path: Option<std::path::PathBuf>,
    auth_override: Option<WebAuth>,
}

impl ControlServer {
    #[must_use]
    pub fn new(runtime: ServiceRuntime, bind_address: impl Into<String>, port: u16) -> Self {
        Self {
            runtime,
            bind_address: bind_address.into(),
            port,
            config_path: None,
            auth_override: None,
        }
    }

    #[must_use]
    pub fn with_config_path(mut self, config_path: impl Into<std::path::PathBuf>) -> Self {
        self.config_path = Some(config_path.into());
        self
    }

    #[must_use]
    pub fn with_web_auth_for_tests(mut self, auth: WebAuth) -> Self {
        self.auth_override = Some(auth);
        self
    }

    pub async fn bind(self) -> Result<BoundControlServer, ServiceError> {
        let addr: SocketAddr = format!("{}:{}", self.bind_address, self.port)
            .parse()
            .map_err(|_| {
                ServiceError::InvalidBindAddress(format!("{}:{}", self.bind_address, self.port))
            })?;
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let auth = match self.auth_override {
            Some(auth) => auth,
            None => {
                WebAuth::from_bind_address(&self.bind_address, &WebTokenStore::default_store()?)?
            }
        };
        let web = WebApp::new(self.runtime.inner.config.clone(), self.config_path, auth);
        Ok(BoundControlServer {
            runtime: self.runtime,
            listener,
            local_addr,
            web,
        })
    }
}

pub struct BoundControlServer {
    runtime: ServiceRuntime,
    listener: TcpListener,
    local_addr: SocketAddr,
    web: WebApp,
}

impl BoundControlServer {
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn serve_until_shutdown(
        self,
        shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Result<(), ServiceError> {
        axum::serve(
            self.listener,
            control_router(Arc::new(ControlAppState {
                runtime: self.runtime,
                web: self.web,
            })),
        )
        .with_graceful_shutdown(shutdown)
        .await?;
        Ok(())
    }
}

#[derive(Clone)]
struct ControlAppState {
    runtime: ServiceRuntime,
    web: WebApp,
}

fn control_router(state: Arc<ControlAppState>) -> Router {
    Router::new()
        .route("/", get(page_dashboard))
        .route("/roots", get(page_roots))
        .route("/exclusions", get(page_exclusions))
        .route("/schedule", get(page_schedule))
        .route("/config", get(page_config))
        .route("/state", get(page_state))
        .route("/logs", get(page_logs))
        .route("/failed-skipped", get(page_failed_skipped))
        .route("/quota", get(page_quota))
        .route("/health", get(get_health))
        .route("/metrics", get(get_metrics))
        .route("/status", get(get_status))
        .route("/sync/run", post(post_sync_run))
        .route("/sync/stop", post(post_sync_stop))
        .route("/api/sync/run", post(post_sync_run))
        .route("/api/sync/stop", post(post_sync_stop))
        .route("/api/config", get(get_config).post(post_config))
        .route("/api/logs/tail", get(get_logs_tail))
        .route("/api/state/recent-runs", get(get_recent_runs))
        .route("/api/state/failed-skipped", get(get_failed_skipped))
        .route("/api/quota", get(get_quota))
        .with_state(state)
}

async fn get_health(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    Json(state.runtime.health().await).into_response()
}

async fn get_metrics(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    Json(state.runtime.metrics().await).into_response()
}

async fn get_status(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    Json(state.runtime.snapshot().await).into_response()
}

async fn post_sync_run(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    let response = state.runtime.request_run(SyncRunTrigger::Manual).await;
    let status = if response.accepted {
        StatusCode::ACCEPTED
    } else {
        StatusCode::CONFLICT
    };
    (status, Json(response)).into_response()
}

async fn post_sync_stop(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    let response = state.runtime.request_cancel().await;
    let status = if response.accepted {
        StatusCode::ACCEPTED
    } else {
        StatusCode::CONFLICT
    };
    (status, Json(response)).into_response()
}

async fn page_dashboard(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Dashboard).await
}

async fn page_roots(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Roots).await
}

async fn page_exclusions(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
) -> Response {
    render_page(state, headers, WebPage::Exclusions).await
}

async fn page_schedule(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Schedule).await
}

async fn page_config(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Config).await
}

async fn page_state(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::State).await
}

async fn page_logs(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Logs).await
}

async fn page_failed_skipped(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
) -> Response {
    render_page(state, headers, WebPage::FailedSkipped).await
}

async fn page_quota(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    render_page(state, headers, WebPage::Quota).await
}

async fn render_page(state: Arc<ControlAppState>, headers: HeaderMap, page: WebPage) -> Response {
    if let Some(response) = authorize(&state, &headers, false) {
        return response;
    }
    match web_state_and_runtime(&state, page).await {
        Ok((web_state, runtime)) => {
            Html(state.web.render(page, &web_state, &runtime)).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!(
                "<!doctype html><title>ya-disk-sync error</title><h1>error</h1><pre>{}</pre>",
                html_escape(&error.to_string())
            )),
        )
            .into_response(),
    }
}

async fn get_config(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    match state.web.state(0) {
        Ok(web_state) => Json(web_state.config).into_response(),
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn post_config(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    let Some(path) = state.web.config_path().cloned() else {
        return json_error(StatusCode::BAD_REQUEST, "config_path_unavailable");
    };
    let raw_json = extract_config_body(&body);
    let config = match serde_json::from_str::<AppConfig>(&raw_json) {
        Ok(config) => config,
        Err(error) => {
            return json_error(StatusCode::BAD_REQUEST, &format!("invalid_json: {error}"))
        }
    };
    let report = validate_config(&config);
    if !report.is_valid() {
        let messages = report
            .errors()
            .iter()
            .map(|error| format!("{}: {}", error.field, error.message))
            .collect::<Vec<_>>()
            .join("; ");
        return json_error(
            StatusCode::BAD_REQUEST,
            &format!("config_invalid: {messages}"),
        );
    }
    if let Err(error) = save_config(&path, &config) {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("config_save_failed: {error}"),
        );
    }
    Json(ConfigSaveResponse {
        saved: true,
        restart_required: true,
        path: path.display().to_string(),
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
struct TailQuery {
    lines: Option<usize>,
}

async fn get_logs_tail(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
    Query(query): Query<TailQuery>,
) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    let lines = query.lines.unwrap_or(50);
    match state.web.state(lines) {
        Ok(web_state) => Json(web_state.log_lines).into_response(),
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn get_recent_runs(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    match StateRepository::open(&state.runtime.inner.config.paths.state_db)
        .and_then(|repository| repository.list_recent_runs(20))
    {
        Ok(runs) => {
            Json(runs.into_iter().map(RunSnapshot::from).collect::<Vec<_>>()).into_response()
        }
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn get_failed_skipped(
    State(state): State<Arc<ControlAppState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    match state.web.state(0) {
        Ok(web_state) => Json(FailedSkippedResponse {
            failed_count: web_state.failed.len(),
            skipped_count: web_state.skipped.len(),
        })
        .into_response(),
        Err(error) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn get_quota(State(state): State<Arc<ControlAppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = authorize(&state, &headers, true) {
        return response;
    }
    match load_quota_view(&state.runtime.inner.config).await {
        Ok(quota) => Json(quota).into_response(),
        Err(_) => Json(QuotaView::Unavailable).into_response(),
    }
}

fn authorize(state: &ControlAppState, headers: &HeaderMap, json: bool) -> Option<Response> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if state.web.auth().authorize_header(authorization) {
        return None;
    }
    let status = StatusCode::UNAUTHORIZED;
    if json {
        Some(json_error(status, "unauthorized"))
    } else {
        Some((status, Html("unauthorized".to_string())).into_response())
    }
}

async fn web_state_and_runtime(
    state: &ControlAppState,
    page: WebPage,
) -> Result<(yds_web::WebUiState, RuntimeWebView), ServiceError> {
    let mut web_state = state.web.state(100)?;
    if page == WebPage::Quota {
        web_state.quota = load_quota_view(&state.runtime.inner.config)
            .await
            .unwrap_or(QuotaView::Unavailable);
    }
    let health = state.runtime.health().await;
    let snapshot = state.runtime.snapshot().await;
    let metrics = state.runtime.metrics().await;
    let runtime = RuntimeWebView {
        status: snapshot.status.as_str().to_string(),
        uptime_seconds: snapshot.uptime_seconds,
        current_run: snapshot.current_run.map(|run| run.trigger),
        last_successful_sync_at_utc: health.last_successful_sync_at_utc,
        latest_run_status: snapshot.latest_run.map(|run| run.status),
        metrics: MetricsWebView {
            scanned_files: metrics.scanned_files,
            uploaded_files: metrics.uploaded_files,
            updated_files: metrics.updated_files,
            deleted_files: metrics.deleted_files,
            skipped_files: metrics.skipped_files,
            failed_files: metrics.failed_files,
            bytes_uploaded: metrics.bytes_uploaded,
            last_duration_seconds: metrics.last_duration_seconds,
            average_upload_speed_bytes_per_second: metrics.average_upload_speed_bytes_per_second,
        },
    };
    Ok((web_state, runtime))
}

async fn load_quota_view(config: &AppConfig) -> Result<QuotaView, ServiceError> {
    let token_store: Arc<dyn TokenStore> = Arc::new(KeyringTokenStore::default_store()?);
    let client = HttpYandexDiskClient::new(
        config.yandex_disk.account_alias.clone(),
        token_store,
        RetryPolicy::from_config(&config.sync),
    )?;
    Ok(quota_view_from_disk_info(client.disk_info().await?))
}

fn quota_view_from_disk_info(info: DiskInfo) -> QuotaView {
    let available_space = info
        .quota
        .total_space
        .zip(info.quota.used_space)
        .map(|(total, used)| total.saturating_sub(used));
    QuotaView::Available {
        total_space: info.quota.total_space,
        used_space: info.quota.used_space,
        trash_size: info.quota.trash_size,
        available_space,
    }
}

fn extract_config_body(body: &str) -> String {
    if let Some(encoded) = body.strip_prefix("config=") {
        let normalized = encoded.replace('+', " ");
        return urlencoding::decode(&normalized)
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| normalized);
    }
    body.to_string()
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(ErrorResponse { error: message })).into_response()
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[derive(Debug, Serialize)]
struct ErrorResponse<'a> {
    error: &'a str,
}

#[derive(Debug, Serialize)]
struct ConfigSaveResponse {
    saved: bool,
    restart_required: bool,
    path: String,
}

#[derive(Debug, Serialize)]
struct FailedSkippedResponse {
    failed_count: usize,
    skipped_count: usize,
}

#[derive(Debug, Clone)]
pub struct ControlClient {
    base_url: String,
    http: reqwest::Client,
}

impl ControlClient {
    #[must_use]
    pub fn new(bind_address: &str, port: u16) -> Self {
        Self {
            base_url: format!("http://{}:{}", bind_address, port),
            http: reqwest::Client::new(),
        }
    }

    pub async fn health(&self) -> Result<HealthResponse, ServiceError> {
        Ok(self
            .http
            .get(format!("{}/health", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn metrics(&self) -> Result<RuntimeMetrics, ServiceError> {
        Ok(self
            .http
            .get(format!("{}/metrics", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn status(&self) -> Result<RuntimeSnapshot, ServiceError> {
        Ok(self
            .http
            .get(format!("{}/status", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn request_run(&self) -> Result<RuntimeCommandResponse, ServiceError> {
        Ok(self
            .http
            .post(format!("{}/sync/run", self.base_url))
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn request_stop(&self) -> Result<RuntimeCommandResponse, ServiceError> {
        Ok(self
            .http
            .post(format!("{}/sync/stop", self.base_url))
            .send()
            .await?
            .json()
            .await?)
    }
}

fn latest_run_from_state(config: &AppConfig) -> Result<Option<RunSnapshot>, ServiceError> {
    let repository = StateRepository::open(&config.paths.state_db)?;
    Ok(repository.get_latest_run()?.map(Into::into))
}

fn latest_success_from_state(config: &AppConfig) -> Result<Option<SyncRunRecord>, ServiceError> {
    let repository = StateRepository::open(&config.paths.state_db)?;
    Ok(repository.get_latest_successful_run()?)
}

fn metrics_from_report(
    report: &SyncRunReport,
    started: OffsetDateTime,
    finished: OffsetDateTime,
) -> RuntimeMetrics {
    let duration = (finished - started).whole_seconds().max(0);
    RuntimeMetrics {
        scanned_files: report.summary.scanned_files,
        uploaded_files: report.summary.uploaded_files,
        updated_files: report.summary.updated_files,
        deleted_files: report.summary.deleted_files,
        skipped_files: report.summary.skipped_files,
        failed_files: report.summary.failed_files,
        bytes_uploaded: report.summary.bytes_uploaded,
        last_duration_seconds: duration,
        average_upload_speed_bytes_per_second: if duration > 0 {
            report.summary.bytes_uploaded / duration
        } else {
            0
        },
    }
}

fn uptime_seconds(started_at_utc: OffsetDateTime) -> i64 {
    (OffsetDateTime::now_utc() - started_at_utc)
        .whole_seconds()
        .max(0)
}

fn std_duration_until(now: OffsetDateTime, next: OffsetDateTime) -> StdDuration {
    let seconds = (next - now).whole_seconds().max(0);
    StdDuration::from_secs(u64::try_from(seconds).unwrap_or(0))
}

fn utc_text(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(
        COMPONENT_NAME,
        "scheduler runtime and control API boundary is available",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;
    use yds_core::{config::default_config, ComponentHealth};
    use yds_yandex_disk::{auth::KeyringTokenStore, YandexDiskClient};

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), ComponentHealth::Ok);
    }

    #[test]
    fn daily_schedule_returns_next_future_utc_time() {
        let schedule = DailyUtcSchedule::parse("23:00").unwrap();
        let before = OffsetDateTime::parse("2026-06-14T22:59:59Z", &Rfc3339).unwrap();
        let exact = OffsetDateTime::parse("2026-06-14T23:00:00Z", &Rfc3339).unwrap();

        assert_eq!(
            schedule.next_after(before),
            OffsetDateTime::parse("2026-06-14T23:00:00Z", &Rfc3339).unwrap()
        );
        assert_eq!(
            schedule.next_after(exact),
            OffsetDateTime::parse("2026-06-15T23:00:00Z", &Rfc3339).unwrap()
        );
    }

    #[test]
    fn quota_view_computes_available_space() {
        let view = quota_view_from_disk_info(yds_yandex_disk::models::DiskInfo {
            quota: yds_yandex_disk::models::QuotaInfo {
                total_space: Some(100),
                used_space: Some(35),
                trash_size: Some(7),
            },
            system_folders: std::collections::BTreeMap::new(),
        });

        assert_eq!(
            view,
            QuotaView::Available {
                total_space: Some(100),
                used_space: Some(35),
                trash_size: Some(7),
                available_space: Some(65)
            }
        );
    }

    #[tokio::test]
    async fn manual_run_transitions_to_idle_and_records_metrics() {
        let fixture = RuntimeFixture::new(FakeTaskMode::ImmediateSuccess);
        let response = fixture.runtime.request_run(SyncRunTrigger::Manual).await;

        assert!(response.accepted);
        wait_until_idle(&fixture.runtime).await;
        let snapshot = fixture.runtime.snapshot().await;
        let metrics = fixture.runtime.metrics().await;

        assert_eq!(snapshot.status, RuntimeStatus::Idle);
        assert_eq!(metrics.scanned_files, 2);
        assert_eq!(metrics.uploaded_files, 1);
    }

    #[tokio::test]
    async fn overlap_is_rejected_and_scheduled_skip_is_counted() {
        let fixture = RuntimeFixture::new(FakeTaskMode::WaitUntilCancelled);

        assert!(
            fixture
                .runtime
                .request_run(SyncRunTrigger::Manual)
                .await
                .accepted
        );
        let second = fixture.runtime.request_run(SyncRunTrigger::Scheduled).await;

        assert!(!second.accepted);
        let snapshot = fixture.runtime.snapshot().await;
        assert_eq!(snapshot.status, RuntimeStatus::Running);
        assert_eq!(snapshot.scheduled_skips, 1);
        fixture.runtime.request_cancel().await;
        wait_until_idle(&fixture.runtime).await;
    }

    #[tokio::test]
    async fn cancel_transitions_through_cancelling() {
        let fixture = RuntimeFixture::new(FakeTaskMode::WaitUntilCancelled);
        fixture.runtime.request_run(SyncRunTrigger::Manual).await;

        let response = fixture.runtime.request_cancel().await;
        let snapshot = fixture.runtime.snapshot().await;

        assert!(response.accepted);
        assert_eq!(snapshot.status, RuntimeStatus::Cancelling);
        wait_until_idle(&fixture.runtime).await;
        assert_eq!(fixture.runtime.snapshot().await.status, RuntimeStatus::Idle);
    }

    #[tokio::test]
    async fn control_server_serves_health_metrics_status_and_commands() {
        let fixture = RuntimeFixture::new(FakeTaskMode::ImmediateSuccess);
        let server = ControlServer::new(fixture.runtime.clone(), "127.0.0.1", 0)
            .bind()
            .await
            .unwrap();
        let address = server.local_addr();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async {
                    let _ = rx.await;
                }))
                .await
                .unwrap();
        });
        let client = ControlClient::new("127.0.0.1", address.port());

        let health = client.health().await.unwrap();
        assert_eq!(health.status, "ok");
        let run = client.request_run().await.unwrap();
        assert!(run.accepted);
        wait_until_idle(&fixture.runtime).await;
        let metrics = client.metrics().await.unwrap();
        let status = client.status().await.unwrap();

        assert_eq!(metrics.uploaded_files, 1);
        assert_eq!(status.status, RuntimeStatus::Idle);
        tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn control_server_serves_dashboard_html() {
        let fixture = RuntimeFixture::new(FakeTaskMode::ImmediateSuccess);
        let server = ControlServer::new(fixture.runtime.clone(), "127.0.0.1", 0)
            .bind()
            .await
            .unwrap();
        let address = server.local_addr();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async {
                    let _ = rx.await;
                }))
                .await
                .unwrap();
        });

        let html = reqwest::get(format!("http://127.0.0.1:{}/", address.port()))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(html.contains("Run now"));
        assert!(html.contains("Sync roots"));
        tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn control_server_auth_rejects_missing_bearer_token() {
        let fixture = RuntimeFixture::new(FakeTaskMode::ImmediateSuccess);
        let server = ControlServer::new(fixture.runtime.clone(), "127.0.0.1", 0)
            .with_web_auth_for_tests(WebAuth::required_static("secret"))
            .bind()
            .await
            .unwrap();
        let address = server.local_addr();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async {
                    let _ = rx.await;
                }))
                .await
                .unwrap();
        });
        let http = reqwest::Client::new();

        let unauthorized = http
            .get(format!("http://127.0.0.1:{}/status", address.port()))
            .send()
            .await
            .unwrap();
        let authorized = http
            .get(format!("http://127.0.0.1:{}/status", address.port()))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap();

        assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(authorized.status(), reqwest::StatusCode::OK);
        tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn runtime_host_starts_control_server_and_shuts_down() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.json");
        let mut config = default_config();
        config.paths.state_db = temp.path().join("state.sqlite").display().to_string();
        config.paths.staging_dir = temp.path().join("staging").display().to_string();
        config.paths.logs_dir = temp.path().join("logs").display().to_string();
        config.schedule.enabled = false;
        config.web_ui.bind_address = "127.0.0.1".to_string();
        config.web_ui.port = 0;
        yds_core::config::save_config(&config_path, &config).unwrap();

        let host = RuntimeHost::start(RuntimeHostOptions::new(&config_path))
            .await
            .unwrap();
        let client = ControlClient::new("127.0.0.1", host.local_addr().port());

        assert_eq!(client.health().await.unwrap().status, "ok");
        assert_eq!(host.runtime().snapshot().await.status, RuntimeStatus::Idle);
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn config_save_rejects_invalid_json_without_changing_file() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.json");
        let mut config = default_config();
        config.paths.state_db = temp.path().join("state.sqlite").display().to_string();
        yds_core::config::save_config(&config_path, &config).unwrap();
        let before = std::fs::read_to_string(&config_path).unwrap();
        let runtime = ServiceRuntime::new(
            config,
            Arc::new(FakeSyncTask {
                mode: FakeTaskMode::ImmediateSuccess,
                run_count: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        let server = ControlServer::new(runtime, "127.0.0.1", 0)
            .with_config_path(config_path.clone())
            .bind()
            .await
            .unwrap();
        let address = server.local_addr();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async {
                    let _ = rx.await;
                }))
                .await
                .unwrap();
        });

        let response = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{}/api/config", address.port()))
            .body("{\"version\":999}")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), before);
        tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires YDS_LIVE_RUNTIME_TESTS=1 and local keyring sandbox"]
    async fn live_runtime_smoke_is_gated_by_env() {
        if std::env::var("YDS_LIVE_RUNTIME_TESTS").ok().as_deref() != Some("1") {
            return;
        }

        let account_alias =
            std::env::var("YDS_TEST_KEYRING_ACCOUNT_ALIAS").unwrap_or_else(|_| "default".into());
        let local_base = std::path::PathBuf::from(
            std::env::var("YDS_LIVE_LOCAL_ROOT")
                .unwrap_or_else(|_| r"C:\Data\YaDiskSyncSandbox".to_string()),
        );
        let remote_base = std::env::var("YDS_LIVE_REMOTE_ROOT")
            .unwrap_or_else(|_| "disk:/Backup/YaDiskSyncSandbox".to_string());
        assert_live_sandbox(&local_base, &remote_base);

        let unique = format!("yds-iteration8-live-{}", uuid_fragment());
        let local_root = local_base.join(&unique);
        let remote_root = format!("{}/{}", remote_base.trim_end_matches('/'), unique);
        cleanup_live_local_cases(&local_base);
        std::fs::create_dir_all(&local_root).unwrap();
        std::fs::write(local_root.join("runtime.txt"), b"runtime").unwrap();

        let token_store: Arc<dyn TokenStore> =
            Arc::new(KeyringTokenStore::default_store().unwrap());
        let cleanup_client = HttpYandexDiskClient::new(
            account_alias.clone(),
            Arc::clone(&token_store),
            RetryPolicy::new(3, StdDuration::from_secs(1)),
        )
        .unwrap();
        cleanup_live_remote_cases(&cleanup_client, &remote_base).await;
        let _ = cleanup_client.delete_permanently(&remote_root).await;

        let temp = tempfile::tempdir().unwrap();
        let mut config = default_config();
        config.paths.state_db = temp.path().join("state.sqlite").display().to_string();
        config.paths.staging_dir = temp.path().join("staging").display().to_string();
        config.paths.logs_dir = temp.path().join("logs").display().to_string();
        config.yandex_disk.account_alias = account_alias;
        config.schedule.enabled = false;
        config.web_ui.bind_address = "127.0.0.1".to_string();
        config.web_ui.port = 0;
        config.global_excludes = Vec::new();
        config.absolute_excludes = Vec::new();
        config.roots = vec![yds_core::config::SyncRootConfig {
            id: "live-runtime".to_string(),
            name: "Live Runtime".to_string(),
            enabled: true,
            local_path: local_root.display().to_string(),
            remote_path_override: Some(remote_root.clone()),
            legacy_remote_paths: Vec::new(),
            excludes: Vec::new(),
        }];

        let runtime = ServiceRuntime::with_engine(config).unwrap();
        let server = ControlServer::new(runtime.clone(), "127.0.0.1", 0)
            .bind()
            .await
            .unwrap();
        let port = server.local_addr().port();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_until_shutdown(Box::pin(async {
                    let _ = rx.await;
                }))
                .await
                .unwrap();
        });
        let client = ControlClient::new("127.0.0.1", port);

        assert_eq!(client.health().await.unwrap().status, "ok");
        let dashboard = reqwest::get(format!("http://127.0.0.1:{port}/"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(dashboard.contains("Run now"));
        assert!(dashboard.contains("Remote quota"));
        assert!(client.request_run().await.unwrap().accepted);
        wait_until_idle_for(&runtime, StdDuration::from_secs(120)).await;
        let first = client.status().await.unwrap();
        assert_eq!(first.status, RuntimeStatus::Idle);
        assert_eq!(first.latest_run.unwrap().summary.uploaded_files, 1);

        assert!(client.request_run().await.unwrap().accepted);
        wait_until_idle_for(&runtime, StdDuration::from_secs(120)).await;
        let second = client.status().await.unwrap();
        assert_eq!(second.latest_run.unwrap().summary.uploaded_files, 0);
        assert_eq!(client.metrics().await.unwrap().uploaded_files, 0);

        let stop = client.request_stop().await.unwrap();
        assert!(!stop.accepted);

        tx.send(()).unwrap();
        handle.await.unwrap();
        cleanup_client
            .delete_permanently(&remote_root)
            .await
            .unwrap();
        std::fs::remove_dir_all(&local_root).unwrap();
    }

    struct RuntimeFixture {
        runtime: ServiceRuntime,
    }

    impl RuntimeFixture {
        fn new(mode: FakeTaskMode) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mut config = default_config();
            config.paths.state_db = temp.path().join("state.sqlite").display().to_string();
            config.schedule.enabled = false;
            let task = Arc::new(FakeSyncTask {
                mode,
                run_count: AtomicUsize::new(0),
            });
            let runtime = ServiceRuntime::new(config, task).unwrap();
            Self { runtime }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FakeTaskMode {
        ImmediateSuccess,
        WaitUntilCancelled,
    }

    struct FakeSyncTask {
        mode: FakeTaskMode,
        run_count: AtomicUsize,
    }

    impl RuntimeSyncTask for FakeSyncTask {
        fn run(&self, _trigger: SyncRunTrigger, cancellation: CancellationToken) -> SyncTaskFuture {
            let mode = self.mode;
            let run_id = self.run_count.fetch_add(1, Ordering::SeqCst) as i64 + 1;
            Box::pin(async move {
                match mode {
                    FakeTaskMode::ImmediateSuccess => {}
                    FakeTaskMode::WaitUntilCancelled => {
                        while !cancellation.is_cancelled() {
                            tokio::time::sleep(StdDuration::from_millis(10)).await;
                        }
                    }
                }
                let status = if cancellation.is_cancelled() {
                    SyncRunStatus::Cancelled
                } else {
                    SyncRunStatus::Succeeded
                };
                Ok(SyncRunReport {
                    run_id,
                    status,
                    summary: SyncRunSummary {
                        scanned_files: 2,
                        uploaded_files: if status == SyncRunStatus::Succeeded {
                            1
                        } else {
                            0
                        },
                        bytes_uploaded: if status == SyncRunStatus::Succeeded {
                            5
                        } else {
                            0
                        },
                        error_summary: (status == SyncRunStatus::Cancelled)
                            .then(|| "cancelled".to_string()),
                        ..SyncRunSummary::default()
                    },
                    roots: Vec::new(),
                    plan: yds_sync::SyncPlan::new(),
                })
            })
        }
    }

    async fn wait_until_idle(runtime: &ServiceRuntime) {
        wait_until_idle_for(runtime, StdDuration::from_secs(2)).await;
    }

    async fn wait_until_idle_for(runtime: &ServiceRuntime, timeout: StdDuration) {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let status = runtime.snapshot().await.status;
            if matches!(
                status,
                RuntimeStatus::Idle | RuntimeStatus::Failed | RuntimeStatus::Degraded
            ) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        panic!("runtime did not become idle");
    }

    fn uuid_fragment() -> String {
        format!("{:?}", std::time::SystemTime::now())
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect()
    }

    fn assert_live_sandbox(local_base: &std::path::Path, remote_base: &str) {
        let local = local_base.to_string_lossy().replace('\\', "/");
        assert!(
            local == "C:/Data/YaDiskSyncSandbox" || local.starts_with("C:/Data/YaDiskSyncSandbox/"),
            "live runtime local root must stay under C:/Data/YaDiskSyncSandbox"
        );
        assert!(
            remote_base == "disk:/Backup/YaDiskSyncSandbox"
                || remote_base.starts_with("disk:/Backup/YaDiskSyncSandbox/"),
            "live runtime remote root must stay under disk:/Backup/YaDiskSyncSandbox"
        );
    }

    fn cleanup_live_local_cases(local_base: &std::path::Path) {
        let Ok(entries) = std::fs::read_dir(local_base) else {
            return;
        };
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with("yds-iteration8-live-")
            {
                let _ = std::fs::remove_dir_all(entry.path());
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
            if first_segment.starts_with("yds-iteration8-live-") {
                roots.insert(format!("{}{}", prefix, first_segment));
            }
        }
        for root in roots {
            let _ = client.delete_permanently(&root).await;
        }
    }
}
