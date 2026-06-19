use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

pub const DISK_UPLOADER_PERSISTENT_CONFLICT: &str = "DiskUploaderPersistentConflict";
pub const LOCAL_UPLOAD_SOURCE_UNAVAILABLE: &str = "LocalUploadSourceUnavailable";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YandexDiskErrorKind {
    Transient,
    Permanent,
    AuthUnavailable,
    QuotaExceeded,
    RateLimited,
    NotFound,
    Critical,
}

impl YandexDiskErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
            Self::AuthUnavailable => "auth_unavailable",
            Self::QuotaExceeded => "quota_exceeded",
            Self::RateLimited => "rate_limited",
            Self::NotFound => "not_found",
            Self::Critical => "critical",
        }
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Transient | Self::RateLimited)
    }
}

impl std::fmt::Display for YandexDiskErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Error)]
#[error("{kind}: {message}")]
pub struct YandexDiskError {
    kind: YandexDiskErrorKind,
    message: String,
    status: Option<StatusCode>,
    api_code: Option<String>,
}

impl YandexDiskError {
    #[must_use]
    pub fn new(kind: YandexDiskErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            status: None,
            api_code: None,
        }
    }

    #[must_use]
    pub fn with_status(
        kind: YandexDiskErrorKind,
        status: StatusCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            status: Some(status),
            api_code: None,
        }
    }

    #[must_use]
    pub fn with_api_code(
        kind: YandexDiskErrorKind,
        status: StatusCode,
        api_code: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            status: Some(status),
            api_code,
        }
    }

    #[must_use]
    pub fn auth_unavailable(message: impl Into<String>) -> Self {
        Self::new(YandexDiskErrorKind::AuthUnavailable, message)
    }

    #[must_use]
    pub fn permanent(message: impl Into<String>) -> Self {
        Self::new(YandexDiskErrorKind::Permanent, message)
    }

    #[must_use]
    pub fn persistent_uploader_conflict(remote_path: impl AsRef<str>) -> Self {
        let remote_path = remote_path.as_ref();
        Self::with_api_code(
            YandexDiskErrorKind::Permanent,
            StatusCode::CONFLICT,
            Some(DISK_UPLOADER_PERSISTENT_CONFLICT.to_string()),
            format!(
                "Yandex Disk uploader returned persistent HTTP 409 Conflict for {remote_path}; \
                 exact file content was rejected by the upload endpoint"
            ),
        )
    }

    #[must_use]
    pub fn local_upload_source_unavailable(
        path: impl std::fmt::Display,
        error: impl std::fmt::Display,
    ) -> Self {
        Self::with_api_code(
            YandexDiskErrorKind::Permanent,
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(LOCAL_UPLOAD_SOURCE_UNAVAILABLE.to_string()),
            format!("failed to open upload source {path}: {error}"),
        )
    }

    #[must_use]
    pub fn critical(message: impl Into<String>) -> Self {
        Self::new(YandexDiskErrorKind::Critical, message)
    }

    #[must_use]
    pub const fn kind(&self) -> YandexDiskErrorKind {
        self.kind
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn status(&self) -> Option<StatusCode> {
        self.status
    }

    #[must_use]
    pub fn api_code(&self) -> Option<&str> {
        self.api_code.as_deref()
    }

    #[must_use]
    pub fn is_persistent_uploader_conflict(&self) -> bool {
        self.api_code() == Some(DISK_UPLOADER_PERSISTENT_CONFLICT)
    }

    #[must_use]
    pub fn is_local_upload_source_unavailable(&self) -> bool {
        self.api_code() == Some(LOCAL_UPLOAD_SOURCE_UNAVAILABLE)
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }

    pub(crate) fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout()
            || error.is_connect()
            || error.is_request()
            || error.is_body()
            || error.is_decode()
        {
            return Self::new(YandexDiskErrorKind::Transient, error.to_string());
        }

        Self::new(YandexDiskErrorKind::Permanent, error.to_string())
    }

    pub(crate) async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status();
        let payload = response.json::<ApiErrorPayload>().await.ok();
        let api_code = payload.as_ref().and_then(|payload| payload.error.clone());
        let message = payload
            .as_ref()
            .and_then(ApiErrorPayload::best_message)
            .unwrap_or_else(|| format!("Yandex Disk API returned HTTP {status}"));
        let kind = classify_http_status(status, api_code.as_deref(), &message);

        Self::with_api_code(kind, status, api_code, message)
    }
}

#[derive(Debug, Deserialize)]
struct ApiErrorPayload {
    error: Option<String>,
    description: Option<String>,
    message: Option<String>,
}

impl ApiErrorPayload {
    fn best_message(&self) -> Option<String> {
        self.description
            .clone()
            .or_else(|| self.message.clone())
            .or_else(|| self.error.clone())
    }
}

#[must_use]
pub fn classify_http_status(
    status: StatusCode,
    api_code: Option<&str>,
    message: &str,
) -> YandexDiskErrorKind {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return YandexDiskErrorKind::AuthUnavailable;
    }
    if status == StatusCode::NOT_FOUND {
        return YandexDiskErrorKind::NotFound;
    }
    let lower_code = api_code.unwrap_or_default().to_ascii_lowercase();
    let lower_message = message.to_ascii_lowercase();
    if status.as_u16() == 507
        || lower_code.contains("quota")
        || lower_message.contains("quota")
        || lower_message.contains("not enough space")
    {
        return YandexDiskErrorKind::QuotaExceeded;
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return YandexDiskErrorKind::RateLimited;
    }
    if status.is_server_error() {
        return YandexDiskErrorKind::Transient;
    }

    YandexDiskErrorKind::Permanent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_transient_and_permanent_errors() {
        assert_eq!(
            classify_http_status(StatusCode::TOO_MANY_REQUESTS, None, ""),
            YandexDiskErrorKind::RateLimited
        );
        assert_eq!(
            classify_http_status(StatusCode::INTERNAL_SERVER_ERROR, None, ""),
            YandexDiskErrorKind::Transient
        );
        assert_eq!(
            classify_http_status(StatusCode::UNAUTHORIZED, None, ""),
            YandexDiskErrorKind::AuthUnavailable
        );
        assert_eq!(
            classify_http_status(StatusCode::PAYLOAD_TOO_LARGE, None, "too large"),
            YandexDiskErrorKind::Permanent
        );
        assert_eq!(
            classify_http_status(StatusCode::INSUFFICIENT_STORAGE, None, "quota exceeded"),
            YandexDiskErrorKind::QuotaExceeded
        );
    }
}
