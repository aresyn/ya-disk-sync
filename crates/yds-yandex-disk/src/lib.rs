pub mod auth;
pub mod error;
pub mod models;

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use auth::{OAuthFlow, StoredToken, TokenStore};
use error::YandexDiskError;
use error::YandexDiskErrorKind;
use models::{
    ApiDiskInfo, ApiOperationStatus, ApiResource, DiskInfo, LinkResponse, OperationStatus,
    ResourceMetadata,
};
use reqwest::{
    header::{AUTHORIZATION, CONTENT_LENGTH},
    StatusCode,
};
use secrecy::ExposeSecret;
use serde::de::DeserializeOwned;
use yds_core::{config::SyncConfig, ComponentStatus};

pub const COMPONENT_NAME: &str = "yandex-disk";
pub const DEFAULT_API_BASE_URL: &str = "https://cloud-api.yandex.net/v1/disk";
const DEFAULT_LIST_LIMIT: u64 = 1_000;
const DEFAULT_LIST_DIRECTORY_CONCURRENCY: usize = 8;
const LIST_DIRECTORY_PROGRESS_INTERVAL: usize = 1_000;

pub use models::ListRecursiveOptions;

pub type DiskFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, YandexDiskError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadSource {
    Bytes(Vec<u8>),
    File(PathBuf),
}

pub trait YandexDiskClient: Send + Sync {
    fn disk_info(&self) -> DiskFuture<'_, DiskInfo>;
    fn metadata<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ResourceMetadata>;
    fn list_recursive<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, Vec<ResourceMetadata>> {
        self.list_recursive_with_options(remote_path, ListRecursiveOptions::default())
    }
    fn list_recursive_with_options<'a>(
        &'a self,
        remote_path: &'a str,
        options: ListRecursiveOptions,
    ) -> DiskFuture<'a, Vec<ResourceMetadata>>;
    fn create_directory<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()>;
    fn upload_source<'a>(
        &'a self,
        remote_path: &'a str,
        source: UploadSource,
        overwrite: bool,
    ) -> DiskFuture<'a, ()>;
    fn upload<'a>(
        &'a self,
        remote_path: &'a str,
        content: Vec<u8>,
        overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        self.upload_source(remote_path, UploadSource::Bytes(content), overwrite)
    }
    fn download<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, Vec<u8>>;
    fn move_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        overwrite: bool,
    ) -> DiskFuture<'a, ()>;
    fn copy_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        overwrite: bool,
    ) -> DiskFuture<'a, ()>;
    fn delete_permanently<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()>;
    fn poll_operation<'a>(&'a self, href: &'a str) -> DiskFuture<'a, OperationStatus>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    attempts: u32,
    delay: Duration,
}

impl RetryPolicy {
    #[must_use]
    pub fn new(attempts: u32, delay: Duration) -> Self {
        Self {
            attempts: attempts.max(1),
            delay,
        }
    }

    #[must_use]
    pub fn from_config(config: &SyncConfig) -> Self {
        Self::new(
            config.retry_attempts,
            Duration::from_secs(config.retry_delay_seconds),
        )
    }

    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    #[must_use]
    pub fn should_retry(&self, error: &YandexDiskError, attempt: u32) -> bool {
        error.is_retryable() && attempt < self.attempts
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_secs(30))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpClientOptions {
    pub base_url: String,
    pub api_request_timeout: Duration,
    pub transfer_request_timeout: Duration,
    pub operation_poll_interval: Duration,
    pub operation_poll_timeout: Duration,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_API_BASE_URL.to_string(),
            api_request_timeout: Duration::from_secs(120),
            transfer_request_timeout: Duration::from_secs(6 * 60 * 60),
            operation_poll_interval: Duration::from_secs(2),
            operation_poll_timeout: Duration::from_secs(300),
        }
    }
}

async fn response_json_with_context<T>(
    response: reqwest::Response,
    context: &str,
) -> Result<T, YandexDiskError>
where
    T: DeserializeOwned,
{
    response
        .json()
        .await
        .map_err(YandexDiskError::from_reqwest)
        .map_err(|error| {
            YandexDiskError::new(
                error.kind(),
                format!(
                    "{} while decoding JSON response for {context}",
                    error.message()
                ),
            )
        })
}

#[derive(Clone)]
pub struct HttpYandexDiskClient {
    http: reqwest::Client,
    plain_http: reqwest::Client,
    token_store: Arc<dyn TokenStore>,
    account_alias: String,
    retry_policy: RetryPolicy,
    oauth_flow: Option<OAuthFlow>,
    options: HttpClientOptions,
}

impl std::fmt::Debug for HttpYandexDiskClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpYandexDiskClient")
            .field("account_alias", &self.account_alias)
            .field("retry_policy", &self.retry_policy)
            .field("oauth_flow", &self.oauth_flow)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl HttpYandexDiskClient {
    pub fn new(
        account_alias: impl Into<String>,
        token_store: Arc<dyn TokenStore>,
        retry_policy: RetryPolicy,
    ) -> Result<Self, YandexDiskError> {
        Self::with_options(
            account_alias,
            token_store,
            retry_policy,
            None,
            HttpClientOptions::default(),
        )
    }

    pub fn with_oauth_flow(
        account_alias: impl Into<String>,
        token_store: Arc<dyn TokenStore>,
        retry_policy: RetryPolicy,
        oauth_flow: OAuthFlow,
    ) -> Result<Self, YandexDiskError> {
        Self::with_options(
            account_alias,
            token_store,
            retry_policy,
            Some(oauth_flow),
            HttpClientOptions::default(),
        )
    }

    pub fn with_options(
        account_alias: impl Into<String>,
        token_store: Arc<dyn TokenStore>,
        retry_policy: RetryPolicy,
        oauth_flow: Option<OAuthFlow>,
        options: HttpClientOptions,
    ) -> Result<Self, YandexDiskError> {
        let http = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .http1_only()
            .timeout(options.api_request_timeout)
            .build()
            .map_err(YandexDiskError::from_reqwest)?;
        let plain_http = reqwest::ClientBuilder::new()
            .http1_only()
            .timeout(options.transfer_request_timeout)
            .build()
            .map_err(YandexDiskError::from_reqwest)?;

        Ok(Self {
            http,
            plain_http,
            token_store,
            account_alias: account_alias.into(),
            retry_policy,
            oauth_flow,
            options,
        })
    }

    async fn get_disk_info(&self) -> Result<DiskInfo, YandexDiskError> {
        let url = self.api_url("");
        let info: ApiDiskInfo = self
            .send_authorized_json("disk_info", |http, authorization| {
                http.get(&url).header(AUTHORIZATION, authorization)
            })
            .await?;
        Ok(info.into())
    }

    async fn get_metadata(&self, remote_path: &str) -> Result<ResourceMetadata, YandexDiskError> {
        let resource = self
            .get_resource_page(remote_path, 0, DEFAULT_LIST_LIMIT)
            .await?;
        Ok(resource.into())
    }

    async fn get_resource_page(
        &self,
        remote_path: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ApiResource, YandexDiskError> {
        let url = self.api_url("/resources");
        self.send_authorized_json(
            &format!("resource page path={remote_path} offset={offset}"),
            |http, authorization| {
                http.get(&url).header(AUTHORIZATION, authorization).query(&[
                    ("path", remote_path.to_string()),
                    ("limit", limit.to_string()),
                    ("offset", offset.to_string()),
                ])
            },
        )
        .await
    }

    async fn list_recursive_inner(
        &self,
        remote_path: &str,
        options: ListRecursiveOptions,
    ) -> Result<Vec<ResourceMetadata>, YandexDiskError> {
        let root = self
            .get_resource_page(remote_path, 0, DEFAULT_LIST_LIMIT)
            .await?;
        if !matches!(root.resource_type, models::ApiResourceType::Directory) {
            return Ok(vec![root.into()]);
        }

        let mut listed = self
            .list_recursive_by_directory_walk(remote_path.to_string(), &options)
            .await?;
        listed.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(listed)
    }

    async fn list_recursive_by_directory_walk(
        &self,
        root_path: String,
        options: &ListRecursiveOptions,
    ) -> Result<Vec<ResourceMetadata>, YandexDiskError> {
        let mut pending = vec![root_path];
        let mut listed = Vec::new();
        let started = Instant::now();
        let mut completed_directories = 0usize;
        let prune_prefixes = normalized_prune_prefixes(&options.prune_remote_prefixes);

        let mut active = 0usize;
        let mut tasks = tokio::task::JoinSet::new();
        tracing::info!(
            concurrency = DEFAULT_LIST_DIRECTORY_CONCURRENCY,
            timeout_seconds = self.list_directory_task_timeout().as_secs(),
            "Yandex Disk recursive listing walk start"
        );
        while active > 0 || !pending.is_empty() {
            while active < DEFAULT_LIST_DIRECTORY_CONCURRENCY {
                let Some(path) = pending.pop() else {
                    break;
                };
                if is_pruned_remote_path(&path, &prune_prefixes) {
                    continue;
                }
                let client = self.clone();
                let timeout = self.list_directory_task_timeout();
                let prune_prefixes = prune_prefixes.clone();
                tasks.spawn(async move {
                    let task_started = Instant::now();
                    let result = tokio::time::timeout(
                        timeout,
                        client.list_directory_children(&path, &prune_prefixes),
                    )
                    .await;
                    let elapsed = task_started.elapsed();
                    let result = match result {
                        Ok(result) => result,
                        Err(_) => Err(YandexDiskError::new(
                            YandexDiskErrorKind::Transient,
                            format!(
                                "recursive listing directory timed out after {} seconds: {path}",
                                timeout.as_secs()
                            ),
                        )),
                    };
                    (path, elapsed, result)
                });
                active += 1;
            }

            if active == 0 {
                break;
            }

            let result = tasks.join_next().await.ok_or_else(|| {
                YandexDiskError::new(
                    YandexDiskErrorKind::Critical,
                    "recursive listing task set ended unexpectedly",
                )
            })?;
            active -= 1;
            let (completed_path, directory_elapsed, result) = result.map_err(|error| {
                YandexDiskError::new(
                    YandexDiskErrorKind::Transient,
                    format!("recursive listing task failed: {error}"),
                )
            })?;
            let (mut child_directories, child_resources) = result?;
            completed_directories += 1;
            pending.append(&mut child_directories);
            listed.extend(child_resources);
            if completed_directories == 1
                || completed_directories.is_multiple_of(LIST_DIRECTORY_PROGRESS_INTERVAL)
            {
                tracing::info!(
                    completed_directories,
                    active,
                    pending_directories = pending.len(),
                    listed_resources = listed.len(),
                    last_path = completed_path.as_str(),
                    directory_elapsed_ms = directory_elapsed.as_millis(),
                    elapsed_ms = started.elapsed().as_millis(),
                    "Yandex Disk recursive listing walk progress"
                );
            }
        }

        listed.sort_by(|left, right| left.path.cmp(&right.path));
        tracing::info!(
            completed_directories,
            listed_resources = listed.len(),
            elapsed_ms = started.elapsed().as_millis(),
            "Yandex Disk recursive listing walk ok"
        );
        Ok(listed)
    }

    fn list_directory_task_timeout(&self) -> Duration {
        let request_time = self
            .options
            .api_request_timeout
            .saturating_mul(self.retry_policy.attempts);
        let retry_delay = self
            .retry_policy
            .delay
            .saturating_mul(self.retry_policy.attempts.saturating_sub(1));
        request_time.saturating_add(retry_delay)
    }

    async fn list_directory_children(
        &self,
        remote_path: &str,
        prune_prefixes: &[String],
    ) -> Result<(Vec<String>, Vec<ResourceMetadata>), YandexDiskError> {
        let mut pending = Vec::new();
        let mut listed = Vec::new();
        let mut offset = 0;
        loop {
            tracing::debug!(
                remote_path,
                offset,
                limit = DEFAULT_LIST_LIMIT,
                "Yandex Disk directory page request start"
            );
            let page = self
                .get_resource_page(remote_path, offset, DEFAULT_LIST_LIMIT)
                .await?;
            let total = page
                .embedded
                .as_ref()
                .and_then(|embedded| embedded.total)
                .unwrap_or(0);
            let returned = page
                .embedded
                .as_ref()
                .map(|embedded| embedded.items.len() as u64)
                .unwrap_or(0);

            self.collect_embedded_page(page, &mut pending, &mut listed, prune_prefixes);
            tracing::debug!(
                remote_path,
                offset,
                returned,
                total,
                accumulated = listed.len(),
                "Yandex Disk directory page request ok"
            );
            offset += returned;

            if returned == 0 || offset >= total {
                break;
            }
        }
        Ok((pending, listed))
    }

    fn collect_embedded_page(
        &self,
        page: ApiResource,
        pending: &mut Vec<String>,
        listed: &mut Vec<ResourceMetadata>,
        prune_prefixes: &[String],
    ) {
        let Some(embedded) = page.embedded else {
            return;
        };

        for item in embedded.items {
            if matches!(item.resource_type, models::ApiResourceType::Directory)
                && !is_pruned_remote_path(&item.path, prune_prefixes)
            {
                pending.push(item.path.clone());
            }
            listed.push(item.into());
        }
    }

    async fn create_directory_inner(&self, remote_path: &str) -> Result<(), YandexDiskError> {
        let url = self.api_url("/resources");
        let response = self
            .send_authorized(|http, authorization| {
                http.put(&url)
                    .header(AUTHORIZATION, authorization)
                    .query(&[("path", remote_path)])
            })
            .await;

        match response {
            Ok(_) => Ok(()),
            Err(error) if is_directory_already_exists(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn upload_inner(
        &self,
        remote_path: &str,
        source: UploadSource,
        overwrite: bool,
    ) -> Result<(), YandexDiskError> {
        let url = self.api_url("/resources/upload");
        let mut last_conflict = None;

        for attempt in 1..=self.retry_policy.attempts {
            let link: LinkResponse = self
                .send_authorized_json(
                    &format!("upload link path={remote_path}"),
                    |http, authorization| {
                        http.get(&url).header(AUTHORIZATION, authorization).query(&[
                            ("path", remote_path.to_string()),
                            ("overwrite", overwrite.to_string()),
                        ])
                    },
                )
                .await?;
            let upload_url = link.href;

            match self.send_upload_source(&upload_url, &source).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_retryable_upload_conflict(&error)
                        && attempt >= self.retry_policy.attempts =>
                {
                    return Err(persistent_upload_conflict_error(remote_path));
                }
                Err(error)
                    if should_retry_upload_conflict(
                        &error,
                        attempt,
                        self.retry_policy.attempts,
                    ) =>
                {
                    tracing::warn!(
                        remote_path,
                        attempt,
                        attempts = self.retry_policy.attempts,
                        error_kind = error.kind().as_str(),
                        error_message = error.message(),
                        "Yandex Disk upload conflict; retrying with a fresh upload link"
                    );
                    last_conflict = Some(error);
                    self.sleep_before_retry().await;
                }
                Err(error) => return Err(error),
            }
        }

        if last_conflict.is_some() {
            return Err(persistent_upload_conflict_error(remote_path));
        }

        Err(YandexDiskError::new(
            YandexDiskErrorKind::Transient,
            format!("upload retry attempts were exhausted: {remote_path}"),
        ))
    }

    async fn send_upload_source(
        &self,
        upload_url: &str,
        source: &UploadSource,
    ) -> Result<(), YandexDiskError> {
        for attempt in 1..=self.retry_policy.attempts {
            let response = self
                .upload_request(upload_url, source)
                .await?
                .send()
                .await
                .map_err(YandexDiskError::from_reqwest);

            match response {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let error = YandexDiskError::from_response(response).await;
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => {
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }

        Err(YandexDiskError::new(
            YandexDiskErrorKind::Transient,
            "upload retry attempts were exhausted",
        ))
    }

    async fn upload_request(
        &self,
        upload_url: &str,
        source: &UploadSource,
    ) -> Result<reqwest::RequestBuilder, YandexDiskError> {
        match source {
            UploadSource::Bytes(content) => {
                Ok(self.plain_http.put(upload_url).body(content.clone()))
            }
            UploadSource::File(path) => {
                let file = tokio::fs::File::open(path).await.map_err(|error| {
                    YandexDiskError::local_upload_source_unavailable(path.display(), error)
                })?;
                let size = file.metadata().await.ok().map(|metadata| metadata.len());
                let request = self
                    .plain_http
                    .put(upload_url)
                    .body(reqwest::Body::from(file));
                Ok(match size {
                    Some(size) => request.header(CONTENT_LENGTH, size),
                    None => request,
                })
            }
        }
    }

    async fn download_inner(&self, remote_path: &str) -> Result<Vec<u8>, YandexDiskError> {
        let url = self.api_url("/resources/download");
        let link: LinkResponse = self
            .send_authorized_json(
                &format!("download link path={remote_path}"),
                |http, authorization| {
                    http.get(&url)
                        .header(AUTHORIZATION, authorization)
                        .query(&[("path", remote_path)])
                },
            )
            .await?;
        let response = self.send_plain(|http| http.get(&link.href)).await?;

        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(YandexDiskError::from_reqwest)
    }

    async fn move_or_copy_inner(
        &self,
        endpoint: &str,
        from: &str,
        to: &str,
        overwrite: bool,
    ) -> Result<(), YandexDiskError> {
        let url = self.api_url(endpoint);
        let response = self
            .send_authorized(|http, authorization| {
                http.post(&url)
                    .header(AUTHORIZATION, authorization)
                    .query(&[
                        ("from", from.to_string()),
                        ("path", to.to_string()),
                        ("overwrite", overwrite.to_string()),
                    ])
            })
            .await?;

        if response.status() == StatusCode::ACCEPTED {
            let link: LinkResponse = response_json_with_context(
                response,
                &format!("operation link endpoint={endpoint} from={from} to={to}"),
            )
            .await?;
            self.poll_operation_inner(&link.href).await?;
            return Ok(());
        }

        Ok(())
    }

    async fn delete_permanently_inner(&self, remote_path: &str) -> Result<(), YandexDiskError> {
        let url = self.api_url("/resources");
        let response = self
            .send_authorized(|http, authorization| {
                http.delete(&url)
                    .header(AUTHORIZATION, authorization)
                    .query(&[
                        ("path", remote_path.to_string()),
                        ("permanently", "true".to_string()),
                    ])
            })
            .await?;

        if response.status() == StatusCode::ACCEPTED {
            let link: LinkResponse = response_json_with_context(
                response,
                &format!("operation link delete path={remote_path}"),
            )
            .await?;
            self.poll_operation_inner(&link.href).await?;
        }

        Ok(())
    }

    async fn poll_operation_inner(&self, href: &str) -> Result<OperationStatus, YandexDiskError> {
        let url = self.absolute_href(href)?;
        let started = Instant::now();

        loop {
            let payload: ApiOperationStatus = self
                .send_authorized_json(
                    &format!("operation poll href={href}"),
                    |http, authorization| http.get(&url).header(AUTHORIZATION, authorization),
                )
                .await?;
            let status = OperationStatus::from_api(&payload.status)?;

            match status {
                OperationStatus::Success => return Ok(status),
                OperationStatus::Failed => {
                    return Err(YandexDiskError::new(
                        YandexDiskErrorKind::Permanent,
                        "Yandex Disk async operation failed",
                    ));
                }
                OperationStatus::InProgress => {
                    if started.elapsed() >= self.options.operation_poll_timeout {
                        return Err(YandexDiskError::new(
                            YandexDiskErrorKind::Transient,
                            "Yandex Disk async operation polling timed out",
                        ));
                    }
                    tokio::time::sleep(self.options.operation_poll_interval).await;
                }
            }
        }
    }

    async fn send_authorized<F>(
        &self,
        mut make_request: F,
    ) -> Result<reqwest::Response, YandexDiskError>
    where
        F: FnMut(&reqwest::Client, String) -> reqwest::RequestBuilder,
    {
        let mut refreshed_after_unauthorized = false;

        for attempt in 1..=self.retry_policy.attempts {
            let token = self.load_required_token()?;
            let authorization = format!("OAuth {}", token.access_token.expose_secret());
            let response = make_request(&self.http, authorization)
                .send()
                .await
                .map_err(YandexDiskError::from_reqwest);

            match response {
                Ok(response)
                    if response.status() == StatusCode::UNAUTHORIZED
                        && !refreshed_after_unauthorized
                        && token.refresh_token.is_some()
                        && self.oauth_flow.is_some() =>
                {
                    self.refresh_stored_token(&token).await?;
                    refreshed_after_unauthorized = true;
                }
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let error = YandexDiskError::from_response(response).await;
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => {
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }

        Err(YandexDiskError::new(
            YandexDiskErrorKind::Transient,
            "request retry attempts were exhausted",
        ))
    }

    async fn send_authorized_json<T, F>(
        &self,
        context: &str,
        mut make_request: F,
    ) -> Result<T, YandexDiskError>
    where
        T: DeserializeOwned,
        F: FnMut(&reqwest::Client, String) -> reqwest::RequestBuilder,
    {
        let mut refreshed_after_unauthorized = false;

        for attempt in 1..=self.retry_policy.attempts {
            let token = self.load_required_token()?;
            let authorization = format!("OAuth {}", token.access_token.expose_secret());
            let response = make_request(&self.http, authorization)
                .send()
                .await
                .map_err(YandexDiskError::from_reqwest);

            let error = match response {
                Ok(response)
                    if response.status() == StatusCode::UNAUTHORIZED
                        && !refreshed_after_unauthorized
                        && token.refresh_token.is_some()
                        && self.oauth_flow.is_some() =>
                {
                    self.refresh_stored_token(&token).await?;
                    refreshed_after_unauthorized = true;
                    tracing::warn!(
                        context,
                        attempt,
                        "Yandex Disk JSON request refreshed token after unauthorized response"
                    );
                    continue;
                }
                Ok(response) if response.status().is_success() => {
                    match response_json_with_context(response, context).await {
                        Ok(payload) => return Ok(payload),
                        Err(error) => error,
                    }
                }
                Ok(response) => YandexDiskError::from_response(response).await,
                Err(error) => error,
            };

            tracing::warn!(
                context,
                attempt,
                attempts = self.retry_policy.attempts,
                error_kind = error.kind().as_str(),
                error_message = error.message(),
                "Yandex Disk JSON request failed"
            );
            if self.retry_policy.should_retry(&error, attempt) {
                self.sleep_before_retry().await;
                continue;
            }
            return Err(error);
        }

        Err(YandexDiskError::new(
            YandexDiskErrorKind::Transient,
            format!("JSON request retry attempts were exhausted: {context}"),
        ))
    }

    async fn send_plain<F>(&self, mut make_request: F) -> Result<reqwest::Response, YandexDiskError>
    where
        F: FnMut(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        for attempt in 1..=self.retry_policy.attempts {
            let response = make_request(&self.plain_http)
                .send()
                .await
                .map_err(YandexDiskError::from_reqwest);

            match response {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let error = YandexDiskError::from_response(response).await;
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => {
                    if self.retry_policy.should_retry(&error, attempt) {
                        self.sleep_before_retry().await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }

        Err(YandexDiskError::new(
            YandexDiskErrorKind::Transient,
            "request retry attempts were exhausted",
        ))
    }

    fn load_required_token(&self) -> Result<StoredToken, YandexDiskError> {
        self.token_store
            .load_token(&self.account_alias)?
            .ok_or_else(|| {
                YandexDiskError::auth_unavailable(format!(
                    "Yandex Disk account '{}' is not authenticated",
                    self.account_alias
                ))
            })
    }

    async fn refresh_stored_token(&self, current: &StoredToken) -> Result<(), YandexDiskError> {
        let flow = self.oauth_flow.as_ref().ok_or_else(|| {
            YandexDiskError::auth_unavailable("cannot refresh token without OAuth flow")
        })?;
        let refresh_token = current.refresh_token.as_ref().ok_or_else(|| {
            YandexDiskError::auth_unavailable("stored token does not contain refresh_token")
        })?;
        let refreshed = flow.refresh_access_token(refresh_token).await?;

        self.token_store.save_token(&self.account_alias, &refreshed)
    }

    async fn sleep_before_retry(&self) {
        if !self.retry_policy.delay.is_zero() {
            tokio::time::sleep(self.retry_policy.delay).await;
        }
    }

    fn api_url(&self, endpoint: &str) -> String {
        format!(
            "{}{}",
            self.options.base_url.trim_end_matches('/'),
            endpoint
        )
    }

    fn absolute_href(&self, href: &str) -> Result<String, YandexDiskError> {
        if href.starts_with("http://") || href.starts_with("https://") {
            return Ok(href.to_string());
        }

        if href.starts_with('/') {
            let base = reqwest::Url::parse(&self.options.base_url).map_err(|error| {
                YandexDiskError::permanent(format!("invalid Yandex Disk base URL: {error}"))
            })?;
            let origin = base.origin().unicode_serialization();
            return Ok(format!("{origin}{href}"));
        }

        Err(YandexDiskError::permanent(format!(
            "operation href is not absolute: {href}"
        )))
    }
}

impl YandexDiskClient for HttpYandexDiskClient {
    fn disk_info(&self) -> DiskFuture<'_, DiskInfo> {
        Box::pin(self.get_disk_info())
    }

    fn metadata<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ResourceMetadata> {
        Box::pin(self.get_metadata(remote_path))
    }

    fn list_recursive_with_options<'a>(
        &'a self,
        remote_path: &'a str,
        options: ListRecursiveOptions,
    ) -> DiskFuture<'a, Vec<ResourceMetadata>> {
        Box::pin(self.list_recursive_inner(remote_path, options))
    }

    fn create_directory<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()> {
        Box::pin(self.create_directory_inner(remote_path))
    }

    fn upload_source<'a>(
        &'a self,
        remote_path: &'a str,
        source: UploadSource,
        overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(self.upload_inner(remote_path, source, overwrite))
    }

    fn download<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, Vec<u8>> {
        Box::pin(self.download_inner(remote_path))
    }

    fn move_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(self.move_or_copy_inner("/resources/move", from, to, overwrite))
    }

    fn copy_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(self.move_or_copy_inner("/resources/copy", from, to, overwrite))
    }

    fn delete_permanently<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()> {
        Box::pin(self.delete_permanently_inner(remote_path))
    }

    fn poll_operation<'a>(&'a self, href: &'a str) -> DiskFuture<'a, OperationStatus> {
        Box::pin(self.poll_operation_inner(href))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MockYandexDiskClient {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MockOperationKind {
    DiskInfo,
    Metadata,
    ListRecursive,
    CreateDirectory,
    Upload,
    Download,
    Move,
    Copy,
    DeletePermanently,
    PollOperation,
}

impl MockOperationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DiskInfo => "disk_info",
            Self::Metadata => "metadata",
            Self::ListRecursive => "list_recursive",
            Self::CreateDirectory => "create_directory",
            Self::Upload => "upload",
            Self::Download => "download",
            Self::Move => "move",
            Self::Copy => "copy",
            Self::DeletePermanently => "delete_permanently",
            Self::PollOperation => "poll_operation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockOperation {
    pub kind: MockOperationKind,
    pub remote_path: String,
    pub secondary_path: Option<String>,
}

#[derive(Debug, Clone)]
struct MockState {
    disk_info: DiskInfo,
    resources: BTreeMap<String, ResourceMetadata>,
    contents: BTreeMap<String, Vec<u8>>,
    operation_log: Vec<MockOperation>,
    failures: BTreeMap<(MockOperationKind, String), VecDeque<YandexDiskError>>,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            disk_info: DiskInfo {
                quota: models::QuotaInfo {
                    total_space: Some(1_000_000_000),
                    used_space: Some(0),
                    trash_size: Some(0),
                },
                system_folders: BTreeMap::new(),
            },
            resources: BTreeMap::new(),
            contents: BTreeMap::new(),
            operation_log: Vec::new(),
            failures: BTreeMap::new(),
        }
    }
}

impl MockYandexDiskClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_resource(&self, resource: ResourceMetadata) -> Result<(), YandexDiskError> {
        let mut state = self.lock_state()?;
        for parent in parent_remote_paths(&resource.path) {
            state
                .resources
                .entry(parent.clone())
                .or_insert_with(|| ResourceMetadata::directory(parent));
        }
        state.resources.insert(resource.path.clone(), resource);
        Ok(())
    }

    pub fn insert_file(
        &self,
        remote_path: impl Into<String>,
        content: impl Into<Vec<u8>>,
    ) -> Result<(), YandexDiskError> {
        let remote_path = remote_path.into();
        let content = content.into();
        let mut state = self.lock_state()?;
        for parent in parent_remote_paths(&remote_path) {
            state
                .resources
                .entry(parent.clone())
                .or_insert_with(|| ResourceMetadata::directory(parent));
        }
        state.resources.insert(
            remote_path.clone(),
            ResourceMetadata::file(&remote_path, content.len() as u64),
        );
        state.contents.insert(remote_path, content);
        Ok(())
    }

    pub fn set_disk_info(&self, disk_info: DiskInfo) -> Result<(), YandexDiskError> {
        let mut state = self.lock_state()?;
        state.disk_info = disk_info;
        Ok(())
    }

    pub fn resources(&self) -> Result<Vec<ResourceMetadata>, YandexDiskError> {
        let state = self.lock_state()?;
        Ok(state.resources.values().cloned().collect())
    }

    pub fn operation_log(&self) -> Result<Vec<MockOperation>, YandexDiskError> {
        let state = self.lock_state()?;
        Ok(state.operation_log.clone())
    }

    pub fn clear_operation_log(&self) -> Result<(), YandexDiskError> {
        let mut state = self.lock_state()?;
        state.operation_log.clear();
        Ok(())
    }

    pub fn fail_next(
        &self,
        kind: MockOperationKind,
        remote_path: impl Into<String>,
        error: YandexDiskError,
    ) -> Result<(), YandexDiskError> {
        let mut state = self.lock_state()?;
        state
            .failures
            .entry((kind, remote_path.into()))
            .or_default()
            .push_back(error);
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, MockState>, YandexDiskError> {
        self.inner
            .lock()
            .map_err(|_| YandexDiskError::critical("mock Yandex Disk client mutex was poisoned"))
    }

    fn record_operation(
        state: &mut MockState,
        kind: MockOperationKind,
        remote_path: impl Into<String>,
        secondary_path: Option<String>,
    ) -> Result<(), YandexDiskError> {
        let remote_path = remote_path.into();
        state.operation_log.push(MockOperation {
            kind,
            remote_path: remote_path.clone(),
            secondary_path,
        });

        let key = (kind, remote_path);
        let mut failure = None;
        let mut remove_queue = false;
        if let Some(queue) = state.failures.get_mut(&key) {
            failure = queue.pop_front();
            remove_queue = queue.is_empty();
        }
        if remove_queue {
            state.failures.remove(&key);
        }
        if let Some(error) = failure {
            return Err(error);
        }

        Ok(())
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

fn normalized_prune_prefixes(prefixes: &[String]) -> Vec<String> {
    prefixes
        .iter()
        .map(|prefix| prefix.trim_end_matches('/').to_string())
        .filter(|prefix| !prefix.is_empty())
        .collect()
}

fn is_pruned_remote_path(path: &str, prune_prefixes: &[String]) -> bool {
    prune_prefixes.iter().any(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn mock_last_remote_segment(path: &str) -> Option<String> {
    path.rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
}

fn is_directory_already_exists(error: &YandexDiskError) -> bool {
    error.status() == Some(StatusCode::CONFLICT)
        && error
            .api_code()
            .map(|code| {
                let lower = code.to_ascii_lowercase();
                lower.contains("pathpointstoexistent") || lower.contains("alreadyexists")
            })
            .unwrap_or(false)
}

fn should_retry_upload_conflict(error: &YandexDiskError, attempt: u32, attempts: u32) -> bool {
    attempt < attempts && is_retryable_upload_conflict(error)
}

fn is_retryable_upload_conflict(error: &YandexDiskError) -> bool {
    if error.status() != Some(StatusCode::CONFLICT) {
        return false;
    }

    let code = error.api_code().unwrap_or_default().to_ascii_lowercase();
    !code.contains("doesntexist")
        && !code.contains("notfound")
        && !code.contains("pathpointstoexistentdirectory")
}

fn persistent_upload_conflict_error(remote_path: &str) -> YandexDiskError {
    YandexDiskError::persistent_uploader_conflict(remote_path)
}

impl YandexDiskClient for MockYandexDiskClient {
    fn disk_info(&self) -> DiskFuture<'_, DiskInfo> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(&mut state, MockOperationKind::DiskInfo, "", None)?;
            Ok(state.disk_info.clone())
        })
    }

    fn metadata<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ResourceMetadata> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(&mut state, MockOperationKind::Metadata, remote_path, None)?;
            state.resources.get(remote_path).cloned().ok_or_else(|| {
                YandexDiskError::with_status(
                    YandexDiskErrorKind::NotFound,
                    StatusCode::NOT_FOUND,
                    format!("mock resource not found: {remote_path}"),
                )
            })
        })
    }

    fn list_recursive_with_options<'a>(
        &'a self,
        remote_path: &'a str,
        options: ListRecursiveOptions,
    ) -> DiskFuture<'a, Vec<ResourceMetadata>> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(
                &mut state,
                MockOperationKind::ListRecursive,
                remote_path,
                None,
            )?;
            let prefix = format!("{}/", remote_path.trim_end_matches('/'));
            Ok(state
                .resources
                .values()
                .filter(|resource| resource.path.starts_with(&prefix))
                .filter(|resource| {
                    !options
                        .prune_remote_prefixes
                        .iter()
                        .map(|value| value.trim_end_matches('/'))
                        .any(|pruned| {
                            resource.path != pruned
                                && resource
                                    .path
                                    .strip_prefix(pruned)
                                    .is_some_and(|suffix| suffix.starts_with('/'))
                        })
                })
                .cloned()
                .collect())
        })
    }

    fn create_directory<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(
                &mut state,
                MockOperationKind::CreateDirectory,
                remote_path,
                None,
            )?;
            state.resources.insert(
                remote_path.to_string(),
                ResourceMetadata::directory(remote_path),
            );
            Ok(())
        })
    }

    fn upload_source<'a>(
        &'a self,
        remote_path: &'a str,
        source: UploadSource,
        _overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(async move {
            let content = match source {
                UploadSource::Bytes(content) => content,
                UploadSource::File(path) => tokio::fs::read(&path).await.map_err(|error| {
                    YandexDiskError::local_upload_source_unavailable(path.display(), error)
                })?,
            };
            let mut state = self.lock_state()?;
            Self::record_operation(&mut state, MockOperationKind::Upload, remote_path, None)?;
            state.resources.insert(
                remote_path.to_string(),
                ResourceMetadata::file(remote_path, content.len() as u64),
            );
            state.contents.insert(remote_path.to_string(), content);
            Ok(())
        })
    }

    fn download<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(&mut state, MockOperationKind::Download, remote_path, None)?;
            let resource = state.resources.get(remote_path).ok_or_else(|| {
                YandexDiskError::with_status(
                    YandexDiskErrorKind::NotFound,
                    StatusCode::NOT_FOUND,
                    format!("mock resource not found: {remote_path}"),
                )
            })?;
            Ok(state
                .contents
                .get(remote_path)
                .cloned()
                .unwrap_or_else(|| vec![0; resource.size_bytes.unwrap_or_default() as usize]))
        })
    }

    fn move_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        _overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(
                &mut state,
                MockOperationKind::Move,
                from,
                Some(to.to_string()),
            )?;
            let mut resource = state.resources.remove(from).ok_or_else(|| {
                YandexDiskError::with_status(
                    YandexDiskErrorKind::NotFound,
                    StatusCode::NOT_FOUND,
                    format!("mock resource not found: {from}"),
                )
            })?;
            let is_directory = resource.resource_type == models::ResourceType::Directory;
            resource.path = to.to_string();
            resource.name = mock_last_remote_segment(to);
            state.resources.insert(to.to_string(), resource);
            if let Some(content) = state.contents.remove(from) {
                state.contents.insert(to.to_string(), content);
            }

            if is_directory {
                let prefix = format!("{}/", from.trim_end_matches('/'));
                let moved_paths: Vec<_> = state
                    .resources
                    .keys()
                    .filter(|path| path.starts_with(&prefix))
                    .cloned()
                    .collect();
                for old_path in moved_paths {
                    if let Some(mut resource) = state.resources.remove(&old_path) {
                        let relative = old_path.trim_start_matches(&prefix);
                        let new_path = format!("{}/{}", to.trim_end_matches('/'), relative);
                        resource.path = new_path.clone();
                        resource.name = mock_last_remote_segment(&new_path);
                        state.resources.insert(new_path.clone(), resource);
                        if let Some(content) = state.contents.remove(&old_path) {
                            state.contents.insert(new_path, content);
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn copy_resource<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        _overwrite: bool,
    ) -> DiskFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(
                &mut state,
                MockOperationKind::Copy,
                from,
                Some(to.to_string()),
            )?;
            let mut resource = state.resources.get(from).cloned().ok_or_else(|| {
                YandexDiskError::with_status(
                    YandexDiskErrorKind::NotFound,
                    StatusCode::NOT_FOUND,
                    format!("mock resource not found: {from}"),
                )
            })?;
            let is_directory = resource.resource_type == models::ResourceType::Directory;
            resource.path = to.to_string();
            resource.name = mock_last_remote_segment(to);
            state.resources.insert(to.to_string(), resource);
            if let Some(content) = state.contents.get(from).cloned() {
                state.contents.insert(to.to_string(), content);
            }

            if is_directory {
                let prefix = format!("{}/", from.trim_end_matches('/'));
                let copied_paths: Vec<_> = state
                    .resources
                    .keys()
                    .filter(|path| path.starts_with(&prefix))
                    .cloned()
                    .collect();
                for old_path in copied_paths {
                    if let Some(mut resource) = state.resources.get(&old_path).cloned() {
                        let relative = old_path.trim_start_matches(&prefix);
                        let new_path = format!("{}/{}", to.trim_end_matches('/'), relative);
                        resource.path = new_path.clone();
                        resource.name = mock_last_remote_segment(&new_path);
                        state.resources.insert(new_path.clone(), resource);
                        if let Some(content) = state.contents.get(&old_path).cloned() {
                            state.contents.insert(new_path, content);
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn delete_permanently<'a>(&'a self, remote_path: &'a str) -> DiskFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(
                &mut state,
                MockOperationKind::DeletePermanently,
                remote_path,
                None,
            )?;
            if matches!(
                state
                    .resources
                    .get(remote_path)
                    .map(|resource| &resource.resource_type),
                Some(models::ResourceType::Directory)
            ) {
                let prefix = format!("{}/", remote_path.trim_end_matches('/'));
                let child_paths: Vec<_> = state
                    .resources
                    .keys()
                    .filter(|path| path.starts_with(&prefix))
                    .cloned()
                    .collect();
                for child_path in child_paths {
                    state.resources.remove(&child_path);
                    state.contents.remove(&child_path);
                }
            }
            state.resources.remove(remote_path);
            state.contents.remove(remote_path);
            Ok(())
        })
    }

    fn poll_operation<'a>(&'a self, href: &'a str) -> DiskFuture<'a, OperationStatus> {
        Box::pin(async move {
            let mut state = self.lock_state()?;
            Self::record_operation(&mut state, MockOperationKind::PollOperation, href, None)?;
            Ok(OperationStatus::Success)
        })
    }
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(
        COMPONENT_NAME,
        "Yandex Disk client, OAuth and mock boundaries are available",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{KeyringTokenStore, MemoryTokenStore, StoredToken, TokenStore};
    use secrecy::SecretString;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), yds_core::ComponentHealth::Ok);
    }

    #[test]
    fn retry_policy_retries_only_retryable_errors_within_attempts() {
        let policy = RetryPolicy::new(3, Duration::from_secs(0));
        let transient = YandexDiskError::new(YandexDiskErrorKind::Transient, "timeout");
        let permanent = YandexDiskError::new(YandexDiskErrorKind::Permanent, "bad path");

        assert!(policy.should_retry(&transient, 1));
        assert!(policy.should_retry(&transient, 2));
        assert!(!policy.should_retry(&transient, 3));
        assert!(!policy.should_retry(&permanent, 1));
    }

    #[tokio::test]
    async fn mock_client_supports_basic_resource_operations() {
        let client = MockYandexDiskClient::new();

        client.create_directory("disk:/root").await.unwrap();
        client
            .upload("disk:/root/file.txt", b"abc".to_vec(), true)
            .await
            .unwrap();
        let listed = client.list_recursive("disk:/root").await.unwrap();
        let metadata = client.metadata("disk:/root/file.txt").await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(metadata.size_bytes, Some(3));
    }

    #[tokio::test]
    async fn mock_client_prunes_recursive_listing_descendants_but_keeps_directory_entry() {
        let client = MockYandexDiskClient::new();
        client.create_directory("disk:/root").await.unwrap();
        client.create_directory("disk:/root/keep").await.unwrap();
        client.create_directory("disk:/root/prune").await.unwrap();
        client
            .create_directory("disk:/root/prune/nested")
            .await
            .unwrap();
        client
            .upload("disk:/root/keep/file.txt", b"keep".to_vec(), true)
            .await
            .unwrap();
        client
            .upload("disk:/root/prune/nested/file.txt", b"drop".to_vec(), true)
            .await
            .unwrap();

        let listed = client
            .list_recursive_with_options(
                "disk:/root",
                ListRecursiveOptions {
                    prune_remote_prefixes: vec!["disk:/root/prune".to_string()],
                },
            )
            .await
            .unwrap();
        let paths: Vec<_> = listed.into_iter().map(|resource| resource.path).collect();

        assert!(paths.contains(&"disk:/root/keep".to_string()));
        assert!(paths.contains(&"disk:/root/keep/file.txt".to_string()));
        assert!(paths.contains(&"disk:/root/prune".to_string()));
        assert!(!paths.contains(&"disk:/root/prune/nested".to_string()));
        assert!(!paths.contains(&"disk:/root/prune/nested/file.txt".to_string()));
    }

    #[tokio::test]
    async fn mock_client_upload_source_reads_file_source() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.txt");
        std::fs::write(&path, b"streamed").unwrap();
        let client = MockYandexDiskClient::new();

        client
            .upload_source("disk:/root/source.txt", UploadSource::File(path), true)
            .await
            .unwrap();

        let downloaded = client.download("disk:/root/source.txt").await.unwrap();
        assert_eq!(downloaded, b"streamed");
    }

    #[tokio::test]
    async fn http_upload_source_file_uses_streaming_put_request() {
        let server = TestServer::new_dynamic(|base_url| {
            vec![
                json_response(
                    200,
                    &format!(r#"{{"href":"{base_url}/upload","method":"PUT","templated":false}}"#),
                ),
                plain_response(201, ""),
            ]
        });
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.txt");
        std::fs::write(&path, b"streamed").unwrap();
        let client = test_client(&server, None).unwrap();

        client
            .upload_source("disk:/root/source.txt", UploadSource::File(path), true)
            .await
            .unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /v1/disk/resources/upload?"));
        assert!(requests[1].starts_with("PUT /upload "));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("content-length: 8"));
        assert!(requests[1].ends_with("streamed"));
    }

    #[tokio::test]
    async fn upload_retries_put_conflict_with_fresh_upload_link() {
        let server = TestServer::new_dynamic(|base_url| {
            vec![
                json_response(
                    200,
                    &format!(
                        r#"{{"href":"{base_url}/upload-1","method":"PUT","templated":false}}"#
                    ),
                ),
                plain_response(409, ""),
                json_response(
                    200,
                    &format!(
                        r#"{{"href":"{base_url}/upload-2","method":"PUT","templated":false}}"#
                    ),
                ),
                plain_response(201, ""),
            ]
        });
        let client = test_client(&server, None).unwrap();

        client
            .upload("disk:/root/conflict.txt", b"retry".to_vec(), true)
            .await
            .unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].starts_with("GET /v1/disk/resources/upload?"));
        assert!(requests[1].starts_with("PUT /upload-1 "));
        assert!(requests[2].starts_with("GET /v1/disk/resources/upload?"));
        assert!(requests[3].starts_with("PUT /upload-2 "));
        assert!(requests[3].ends_with("retry"));
    }

    #[tokio::test]
    async fn upload_exhausted_put_conflict_returns_persistent_conflict_code() {
        let server = TestServer::new_dynamic(|base_url| {
            vec![
                json_response(
                    200,
                    &format!(
                        r#"{{"href":"{base_url}/upload-1","method":"PUT","templated":false}}"#
                    ),
                ),
                plain_response(409, ""),
                json_response(
                    200,
                    &format!(
                        r#"{{"href":"{base_url}/upload-2","method":"PUT","templated":false}}"#
                    ),
                ),
                plain_response(409, ""),
                json_response(
                    200,
                    &format!(
                        r#"{{"href":"{base_url}/upload-3","method":"PUT","templated":false}}"#
                    ),
                ),
                plain_response(409, ""),
            ]
        });
        let client = test_client(&server, None).unwrap();

        let error = client
            .upload("disk:/root/conflict.txt", b"blocked".to_vec(), true)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), YandexDiskErrorKind::Permanent);
        assert_eq!(
            error.api_code(),
            Some(error::DISK_UPLOADER_PERSISTENT_CONFLICT)
        );
        assert!(error.is_persistent_uploader_conflict());
        assert!(error.message().contains("persistent HTTP 409 Conflict"));
        assert_eq!(server.requests().len(), 6);
    }

    #[tokio::test]
    async fn metadata_request_uses_expected_endpoint_and_authorization() {
        let server = TestServer::new(vec![json_response(
            200,
            r#"{"path":"disk:/root/file.txt","name":"file.txt","type":"file","size":3}"#,
        )]);
        let client = test_client(&server, None).unwrap();

        let metadata = client.metadata("disk:/root/file.txt").await.unwrap();

        assert_eq!(metadata.path, "disk:/root/file.txt");
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /v1/disk/resources?"));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("authorization: oauth access-secret"));
        assert!(!requests[0].contains("refresh-secret"));
    }

    #[tokio::test]
    async fn list_recursive_retries_json_decode_error_for_same_directory_page() {
        let server = TestServer::new(vec![
            json_response(
                200,
                r#"{"path":"disk:/root","name":"root","type":"dir","_embedded":{"items":[],"limit":1000,"offset":0,"total":0}}"#,
            ),
            json_response(200, "{"),
            json_response(
                200,
                r#"{"path":"disk:/root","name":"root","type":"dir","_embedded":{"items":[{"path":"disk:/root/a.txt","name":"a.txt","type":"file","size":1}],"limit":1000,"offset":0,"total":1}}"#,
            ),
        ]);
        let client = test_client(&server, None).unwrap();

        let listed = client.list_recursive("disk:/root").await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, "disk:/root/a.txt");
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1], requests[2]);
    }

    #[tokio::test]
    async fn json_decode_error_exhaustion_is_retryable_transient_without_secret_leak() {
        let server = TestServer::new(vec![
            json_response(200, "{"),
            json_response(200, "{"),
            json_response(200, "{"),
        ]);
        let client = test_client(&server, None).unwrap();

        let error = client.metadata("disk:/root").await.unwrap_err();

        assert_eq!(error.kind(), YandexDiskErrorKind::Transient);
        assert!(error.message().contains("decoding JSON response"));
        assert!(!error.message().contains("access-secret"));
        assert_eq!(server.requests().len(), 3);
    }

    #[tokio::test]
    async fn unauthorized_json_response_does_not_retry_without_refresh_flow() {
        let server = TestServer::new(vec![json_response(401, r#"{"error":"UnauthorizedError"}"#)]);
        let client = test_client(&server, None).unwrap();

        let error = client.metadata("disk:/root").await.unwrap_err();

        assert_eq!(error.kind(), YandexDiskErrorKind::AuthUnavailable);
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn refreshes_token_once_after_unauthorized_response() {
        let server = TestServer::new(vec![
            json_response(401, r#"{"error":"UnauthorizedError"}"#),
            json_response(
                200,
                r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600,"scope":"disk","token_type":"bearer"}"#,
            ),
            json_response(
                200,
                r#"{"path":"disk:/root/file.txt","name":"file.txt","type":"file","size":3}"#,
            ),
        ]);
        let flow = OAuthFlow::with_urls(
            "client-id",
            "disk",
            "ya-disk-sync",
            "https://oauth.yandex.com/authorize",
            server.url("/token"),
        )
        .unwrap();
        let client = test_client(&server, Some(flow)).unwrap();

        let metadata = client.metadata("disk:/root/file.txt").await.unwrap();

        assert_eq!(metadata.path, "disk:/root/file.txt");
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].starts_with("POST /token "));
        assert!(requests[2]
            .to_ascii_lowercase()
            .contains("authorization: oauth new-access"));
    }

    #[tokio::test]
    async fn operation_polling_handles_in_progress_then_success() {
        let server = TestServer::new(vec![
            json_response(200, r#"{"status":"in-progress"}"#),
            json_response(200, r#"{"status":"success"}"#),
        ]);
        let mut options = test_options(&server);
        options.operation_poll_interval = Duration::from_secs(0);
        let client = test_client_with_options(&server, None, options).unwrap();

        let status = client
            .poll_operation("/v1/disk/operations/42")
            .await
            .unwrap();

        assert_eq!(status, OperationStatus::Success);
        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn create_directory_ignores_only_already_exists_conflict() {
        let server = TestServer::new(vec![json_response(
            409,
            r#"{"error":"DiskPathPointsToExistentDirectoryError","description":"already exists"}"#,
        )]);
        let client = test_client(&server, None).unwrap();

        client.create_directory("disk:/root").await.unwrap();
    }

    #[tokio::test]
    async fn create_directory_reports_missing_parent_conflict() {
        let server = TestServer::new(vec![json_response(
            409,
            r#"{"error":"DiskPathDoesntExistsError","description":"parent is missing"}"#,
        )]);
        let client = test_client(&server, None).unwrap();

        let error = client
            .create_directory("disk:/missing/child")
            .await
            .unwrap_err();

        assert_eq!(error.kind(), YandexDiskErrorKind::Permanent);
        assert_eq!(error.api_code(), Some("DiskPathDoesntExistsError"));
    }

    #[tokio::test]
    #[ignore = "requires real Yandex Disk test account and YDS_INTEGRATION_TESTS=1"]
    async fn real_yandex_disk_smoke_test_is_gated_by_env() {
        if std::env::var("YDS_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
            return;
        }

        let remote_root = std::env::var("YDS_TEST_REMOTE_ROOT")
            .expect("YDS_TEST_REMOTE_ROOT must be set for real Yandex Disk integration test");
        let keyring_alias = std::env::var("YDS_TEST_KEYRING_ACCOUNT_ALIAS").ok();
        let access_token = std::env::var("YDS_TEST_ACCESS_TOKEN").ok();
        let (account_alias, store): (String, Arc<dyn TokenStore>) = if let Some(account_alias) =
            keyring_alias
        {
            (
                account_alias,
                Arc::new(KeyringTokenStore::default_store().unwrap()),
            )
        } else if let Some(access_token) = access_token {
            let store = MemoryTokenStore::shared();
            let token = StoredToken {
                access_token: SecretString::from(access_token),
                refresh_token: None,
                expires_at_unix: None,
                scope: Some("disk".to_string()),
                token_type: None,
                client_id: None,
            };
            store.save_token("integration", &token).unwrap();
            ("integration".to_string(), store)
        } else {
            panic!(
                    "YDS_TEST_ACCESS_TOKEN or YDS_TEST_KEYRING_ACCOUNT_ALIAS must be set for real Yandex Disk integration test"
                );
        };
        let client = HttpYandexDiskClient::new(
            account_alias,
            store,
            RetryPolicy::new(3, Duration::from_secs(1)),
        )
        .unwrap();
        let folder = format!(
            "{}/{}",
            remote_root.trim_end_matches('/'),
            uuid::Uuid::new_v4()
        );
        let file = format!("{folder}/smoke.txt");

        client.create_directory(&folder).await.unwrap();
        client.upload(&file, b"smoke".to_vec(), true).await.unwrap();
        let metadata = client.metadata(&file).await.unwrap();
        let downloaded = client.download(&file).await.unwrap();
        client.delete_permanently(&folder).await.unwrap();

        assert_eq!(metadata.path, file);
        assert_eq!(downloaded, b"smoke");
    }

    fn test_client(
        server: &TestServer,
        oauth_flow: Option<OAuthFlow>,
    ) -> Result<HttpYandexDiskClient, YandexDiskError> {
        test_client_with_options(server, oauth_flow, test_options(server))
    }

    fn test_client_with_options(
        _server: &TestServer,
        oauth_flow: Option<OAuthFlow>,
        options: HttpClientOptions,
    ) -> Result<HttpYandexDiskClient, YandexDiskError> {
        let store = MemoryTokenStore::shared();
        let token = StoredToken {
            access_token: SecretString::from("access-secret"),
            refresh_token: Some(SecretString::from("refresh-secret")),
            expires_at_unix: None,
            scope: Some("disk".to_string()),
            token_type: None,
            client_id: Some("client-id".to_string()),
        };
        store.save_token("default", &token)?;

        HttpYandexDiskClient::with_options(
            "default",
            store,
            RetryPolicy::new(3, Duration::from_secs(0)),
            oauth_flow,
            options,
        )
    }

    fn test_options(server: &TestServer) -> HttpClientOptions {
        HttpClientOptions {
            base_url: server.url("/v1/disk"),
            api_request_timeout: Duration::from_secs(30),
            transfer_request_timeout: Duration::from_secs(30),
            operation_poll_interval: Duration::from_secs(0),
            operation_poll_timeout: Duration::from_secs(1),
        }
    }

    fn json_response(status: u16, body: &str) -> String {
        format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn plain_response(status: u16, body: &str) -> String {
        format!(
            "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    struct TestServer {
        address: String,
        requests: Arc<Mutex<Vec<String>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl TestServer {
        fn new(responses: Vec<String>) -> Self {
            Self::new_dynamic(move |_| responses)
        }

        fn new_dynamic(build_responses: impl FnOnce(&str) -> Vec<String>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let responses = build_responses(&format!("http://{address}"));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let thread_requests = Arc::clone(&requests);
            let handle = thread::spawn(move || {
                for response in responses {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&mut stream);
                    thread_requests.lock().unwrap().push(request);
                    stream.write_all(response.as_bytes()).unwrap();
                }
            });

            Self {
                address,
                requests,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.address, path)
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        let read = stream.read(&mut buffer).unwrap();
        bytes.extend_from_slice(&buffer[..read]);

        if let Some(expected_len) = expected_body_len(&bytes) {
            while bytes.len() < expected_len {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buffer[..read]);
            }
        }

        String::from_utf8_lossy(&bytes).to_string()
    }

    fn expected_body_len(bytes: &[u8]) -> Option<usize> {
        let header_end = bytes.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })?;
        Some(header_end + content_length)
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                handle.join().unwrap();
            }
        }
    }
}
