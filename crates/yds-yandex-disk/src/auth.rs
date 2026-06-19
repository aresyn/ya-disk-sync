use std::{
    collections::HashMap,
    env, fmt, fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use keyring_core::Entry;
use oauth2::{
    basic::BasicClient, AuthUrl, ClientId, CsrfToken, PkceCodeChallenge, Scope, TokenUrl,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use yds_core::config::YandexDiskConfig;

use crate::error::{YandexDiskError, YandexDiskErrorKind};

pub const YDS_OAUTH_CLIENT_ID_ENV: &str = "YDS_OAUTH_CLIENT_ID";
pub const DEFAULT_KEYRING_SERVICE: &str = "ya-disk-sync.yandex-disk";
pub const YACLI_KEYRING_SERVICE: &str = "com.nextstat.yacli";
pub const YACLI_APPDATA_DIR: &str = "yacli";
pub const YACLI_ACCOUNTS_FILE: &str = "accounts.toml";
pub const YANDEX_AUTHORIZE_URL: &str = "https://oauth.yandex.com/authorize";
pub const YANDEX_TOKEN_URL: &str = "https://oauth.yandex.com/token";

pub trait TokenStore: Send + Sync {
    fn load_token(&self, account_alias: &str) -> Result<Option<StoredToken>, YandexDiskError>;
    fn save_token(&self, account_alias: &str, token: &StoredToken) -> Result<(), YandexDiskError>;
    fn delete_token(&self, account_alias: &str) -> Result<bool, YandexDiskError>;
}

#[derive(Clone)]
pub struct StoredToken {
    pub access_token: SecretString,
    pub refresh_token: Option<SecretString>,
    pub expires_at_unix: Option<i64>,
    pub scope: Option<String>,
    pub token_type: Option<String>,
    pub client_id: Option<String>,
}

impl fmt::Debug for StoredToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredToken")
            .field("access_token", &self.access_token)
            .field("refresh_token", &self.refresh_token)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("scope", &self.scope)
            .field("token_type", &self.token_type)
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl StoredToken {
    #[must_use]
    pub fn metadata(&self, account_alias: impl Into<String>) -> TokenStatus {
        TokenStatus {
            account_alias: account_alias.into(),
            authenticated: true,
            expires_at_unix: self.expires_at_unix,
            has_refresh_token: self.refresh_token.is_some(),
            scope: self.scope.clone(),
            client_id: self.client_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenStatus {
    pub account_alias: String,
    pub authenticated: bool,
    pub expires_at_unix: Option<i64>,
    pub has_refresh_token: bool,
    pub scope: Option<String>,
    pub client_id: Option<String>,
}

impl TokenStatus {
    #[must_use]
    pub fn unauthenticated(account_alias: impl Into<String>) -> Self {
        Self {
            account_alias: account_alias.into(),
            authenticated: false,
            expires_at_unix: None,
            has_refresh_token: false,
            scope: None,
            client_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyringTokenStore {
    service_name: String,
}

impl KeyringTokenStore {
    pub fn new(service_name: impl Into<String>) -> Result<Self, YandexDiskError> {
        keyring::use_native_store(false).map_err(map_keyring_error)?;

        Ok(Self {
            service_name: service_name.into(),
        })
    }

    pub fn default_store() -> Result<Self, YandexDiskError> {
        Self::new(DEFAULT_KEYRING_SERVICE)
    }

    fn entry(&self, account_alias: &str) -> Result<Entry, YandexDiskError> {
        Entry::new(&self.service_name, account_alias).map_err(map_keyring_error)
    }
}

impl TokenStore for KeyringTokenStore {
    fn load_token(&self, account_alias: &str) -> Result<Option<StoredToken>, YandexDiskError> {
        let entry = self.entry(account_alias)?;
        let secret = match entry.get_password() {
            Ok(secret) => secret,
            Err(keyring_core::Error::NoEntry) => return Ok(None),
            Err(error) => return Err(map_keyring_error(error)),
        };

        let parsed: StoredTokenJson = serde_json::from_str(&secret).map_err(|error| {
            YandexDiskError::new(
                YandexDiskErrorKind::AuthUnavailable,
                format!("stored Yandex Disk token is malformed: {error}"),
            )
        })?;

        Ok(Some(parsed.into()))
    }

    fn save_token(&self, account_alias: &str, token: &StoredToken) -> Result<(), YandexDiskError> {
        let entry = self.entry(account_alias)?;
        let secret = serde_json::to_string(&StoredTokenJson::from(token)).map_err(|error| {
            YandexDiskError::critical(format!("failed to serialize Yandex Disk token: {error}"))
        })?;

        entry.set_password(&secret).map_err(map_keyring_error)
    }

    fn delete_token(&self, account_alias: &str) -> Result<bool, YandexDiskError> {
        let entry = self.entry(account_alias)?;
        match entry.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring_core::Error::NoEntry) => Ok(false),
            Err(error) => Err(map_keyring_error(error)),
        }
    }
}

#[derive(Debug, Default)]
pub struct MemoryTokenStore {
    tokens: Mutex<HashMap<String, StoredToken>>,
}

impl MemoryTokenStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl TokenStore for MemoryTokenStore {
    fn load_token(&self, account_alias: &str) -> Result<Option<StoredToken>, YandexDiskError> {
        let tokens = self
            .tokens
            .lock()
            .map_err(|_| YandexDiskError::critical("in-memory token store mutex was poisoned"))?;
        Ok(tokens.get(account_alias).cloned())
    }

    fn save_token(&self, account_alias: &str, token: &StoredToken) -> Result<(), YandexDiskError> {
        let mut tokens = self
            .tokens
            .lock()
            .map_err(|_| YandexDiskError::critical("in-memory token store mutex was poisoned"))?;
        tokens.insert(account_alias.to_string(), token.clone());
        Ok(())
    }

    fn delete_token(&self, account_alias: &str) -> Result<bool, YandexDiskError> {
        let mut tokens = self
            .tokens
            .lock()
            .map_err(|_| YandexDiskError::critical("in-memory token store mutex was poisoned"))?;
        Ok(tokens.remove(account_alias).is_some())
    }
}

#[derive(Clone)]
pub struct OAuthFlow {
    client_id: String,
    scope: String,
    client_name: String,
    authorize_url: String,
    token_url: String,
    http: reqwest::Client,
}

impl fmt::Debug for OAuthFlow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthFlow")
            .field("client_id", &self.client_id)
            .field("scope", &self.scope)
            .field("client_name", &self.client_name)
            .field("authorize_url", &self.authorize_url)
            .field("token_url", &self.token_url)
            .finish_non_exhaustive()
    }
}

impl OAuthFlow {
    pub fn new(
        client_id: impl Into<String>,
        scope: impl Into<String>,
        client_name: impl Into<String>,
    ) -> Result<Self, YandexDiskError> {
        Self::with_urls(
            client_id,
            scope,
            client_name,
            YANDEX_AUTHORIZE_URL,
            YANDEX_TOKEN_URL,
        )
    }

    pub fn with_urls(
        client_id: impl Into<String>,
        scope: impl Into<String>,
        client_name: impl Into<String>,
        authorize_url: impl Into<String>,
        token_url: impl Into<String>,
    ) -> Result<Self, YandexDiskError> {
        let http = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(YandexDiskError::from_reqwest)?;

        Ok(Self {
            client_id: client_id.into(),
            scope: scope.into(),
            client_name: client_name.into(),
            authorize_url: authorize_url.into(),
            token_url: token_url.into(),
            http,
        })
    }

    pub fn authorization_request(&self) -> Result<OAuthAuthorizationRequest, YandexDiskError> {
        let auth_url = AuthUrl::new(self.authorize_url.clone()).map_err(|error| {
            YandexDiskError::permanent(format!("invalid Yandex OAuth authorize URL: {error}"))
        })?;
        let token_url = TokenUrl::new(self.token_url.clone()).map_err(|error| {
            YandexDiskError::permanent(format!("invalid Yandex OAuth token URL: {error}"))
        })?;
        let client = BasicClient::new(ClientId::new(self.client_id.clone()))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url);
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        let (authorization_url, csrf_state) = client
            .authorize_url(CsrfToken::new_random)
            .add_scope(Scope::new(self.scope.clone()))
            .set_pkce_challenge(pkce_challenge)
            .url();

        Ok(OAuthAuthorizationRequest {
            authorization_url: authorization_url.to_string(),
            csrf_state: csrf_state.secret().clone(),
            pkce_verifier: SecretString::from(pkce_verifier.secret().clone()),
        })
    }

    pub async fn exchange_code(
        &self,
        code: &str,
        pkce_verifier: &SecretString,
    ) -> Result<StoredToken, YandexDiskError> {
        let form = vec![
            ("grant_type", "authorization_code".to_string()),
            ("code", code.to_string()),
            ("client_id", self.client_id.clone()),
            ("code_verifier", pkce_verifier.expose_secret().to_string()),
        ];

        self.post_token_form(&form).await
    }

    pub async fn refresh_access_token(
        &self,
        refresh_token: &SecretString,
    ) -> Result<StoredToken, YandexDiskError> {
        let form = vec![
            ("grant_type", "refresh_token".to_string()),
            ("refresh_token", refresh_token.expose_secret().to_string()),
            ("client_id", self.client_id.clone()),
        ];

        self.post_token_form(&form).await
    }

    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    async fn post_token_form(
        &self,
        form: &[(&str, String)],
    ) -> Result<StoredToken, YandexDiskError> {
        let response = self
            .http
            .post(&self.token_url)
            .form(form)
            .send()
            .await
            .map_err(YandexDiskError::from_reqwest)?;

        if !response.status().is_success() {
            return Err(YandexDiskError::from_response(response).await);
        }

        let response: OAuthTokenResponse = response
            .json()
            .await
            .map_err(YandexDiskError::from_reqwest)?;
        Ok(response.into_stored_token(self.client_id.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct OAuthAuthorizationRequest {
    pub authorization_url: String,
    pub csrf_state: String,
    pub pkce_verifier: SecretString,
}

#[derive(Debug, Clone, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    token_type: Option<String>,
    expires_in: Option<i64>,
    refresh_token: Option<String>,
    scope: Option<String>,
}

impl OAuthTokenResponse {
    fn into_stored_token(self, client_id: String) -> StoredToken {
        let expires_at_unix = self.expires_in.and_then(|seconds| {
            OffsetDateTime::now_utc()
                .unix_timestamp()
                .checked_add(seconds)
        });

        StoredToken {
            access_token: SecretString::from(self.access_token),
            refresh_token: self.refresh_token.map(SecretString::from),
            expires_at_unix,
            scope: self.scope,
            token_type: self.token_type,
            client_id: Some(client_id),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredTokenJson {
    access_token: String,
    refresh_token: Option<String>,
    #[serde(default, alias = "expires_at_epoch_secs")]
    expires_at_unix: Option<i64>,
    scope: Option<String>,
    token_type: Option<String>,
    client_id: Option<String>,
}

impl From<&StoredToken> for StoredTokenJson {
    fn from(value: &StoredToken) -> Self {
        Self {
            access_token: value.access_token.expose_secret().to_string(),
            refresh_token: value
                .refresh_token
                .as_ref()
                .map(|token| token.expose_secret().to_string()),
            expires_at_unix: value.expires_at_unix,
            scope: value.scope.clone(),
            token_type: value.token_type.clone(),
            client_id: value.client_id.clone(),
        }
    }
}

impl From<StoredTokenJson> for StoredToken {
    fn from(value: StoredTokenJson) -> Self {
        Self {
            access_token: SecretString::from(value.access_token),
            refresh_token: value.refresh_token.map(SecretString::from),
            expires_at_unix: value.expires_at_unix,
            scope: value.scope,
            token_type: value.token_type,
            client_id: value.client_id,
        }
    }
}

#[must_use]
pub fn resolve_oauth_client_id(
    config: &YandexDiskConfig,
    cli_override: Option<&str>,
) -> Option<String> {
    cli_override
        .map(ToOwned::to_owned)
        .or_else(|| config.oauth_client_id.clone())
        .or_else(|| env::var(YDS_OAUTH_CLIENT_ID_ENV).ok())
        .filter(|value| !value.trim().is_empty())
}

pub fn token_status(
    store: &dyn TokenStore,
    account_alias: &str,
) -> Result<TokenStatus, YandexDiskError> {
    Ok(store
        .load_token(account_alias)?
        .map(|token| token.metadata(account_alias))
        .unwrap_or_else(|| TokenStatus::unauthenticated(account_alias)))
}

pub fn import_yacli_auth(
    store: &dyn TokenStore,
    account_alias: &str,
) -> Result<(), YandexDiskError> {
    keyring::use_native_store(false).map_err(map_keyring_error)?;

    let candidates = yacli_keyring_candidates(account_alias);
    let mut errors = Vec::new();

    for yacli_user in candidates {
        let entry = Entry::new(YACLI_KEYRING_SERVICE, &yacli_user).map_err(map_keyring_error)?;
        match entry.get_password() {
            Ok(secret) => {
                let token = parse_yacli_disk_token(&secret)?;
                store.save_token(account_alias, &token)?;
                return Ok(());
            }
            Err(keyring_core::Error::NoEntry) => {}
            Err(error) => errors.push(format!("{yacli_user}: {error}")),
        }
    }

    let detail = if errors.is_empty() {
        "no matching yacli keyring entry was found".to_string()
    } else {
        format!("keyring lookup errors: {}", errors.join("; "))
    };
    Err(YandexDiskError::auth_unavailable(format!(
        "safe yacli token import is unavailable: {detail}; run auth login"
    )))
}

fn map_keyring_error(error: keyring_core::Error) -> YandexDiskError {
    YandexDiskError::new(
        YandexDiskErrorKind::AuthUnavailable,
        format!("keyring access failed: {error}"),
    )
}

fn yacli_keyring_candidates(account_alias: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    if !account_alias.trim().is_empty() && account_alias != "default" {
        push_unique(&mut candidates, account_alias.to_string());
        push_unique(&mut candidates, format!("{account_alias}:disk"));
    }

    if let Some(accounts_path) = yacli_accounts_path() {
        if let Ok(content) = fs::read_to_string(accounts_path) {
            for candidate in yacli_candidates_from_accounts_toml(&content) {
                push_unique(&mut candidates, candidate);
            }
        }
    }

    candidates
}

fn yacli_accounts_path() -> Option<PathBuf> {
    if cfg!(windows) {
        return env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|path| path.join(YACLI_APPDATA_DIR).join(YACLI_ACCOUNTS_FILE));
    }

    env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|path| path.join(YACLI_APPDATA_DIR).join(YACLI_ACCOUNTS_FILE))
}

fn yacli_candidates_from_accounts_toml(content: &str) -> Vec<String> {
    let Ok(value) = toml::from_str::<toml::Value>(content) else {
        return Vec::new();
    };
    let Some(accounts) = value.get("accounts").and_then(toml::Value::as_table) else {
        return Vec::new();
    };

    let mut default_candidates = Vec::new();
    let mut other_candidates = Vec::new();
    for (account_name, account) in accounts {
        let Some(account_table) = account.as_table() else {
            continue;
        };
        let is_default = account_table
            .get("default")
            .and_then(toml::Value::as_bool)
            .unwrap_or(false);
        let Some(disk_table) = account_table.get("disk").and_then(toml::Value::as_table) else {
            continue;
        };
        if !disk_table
            .get("enabled")
            .and_then(toml::Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        let credential_ref = disk_table
            .get("credential_ref")
            .and_then(toml::Value::as_str)
            .unwrap_or("store:disk");
        let credential_name = credential_ref
            .strip_prefix("store:")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("disk");
        let candidate = format!("{account_name}:{credential_name}");
        if is_default {
            default_candidates.push(candidate);
        } else {
            other_candidates.push(candidate);
        }
    }

    default_candidates.extend(other_candidates);
    default_candidates
}

fn parse_yacli_disk_token(secret: &str) -> Result<StoredToken, YandexDiskError> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(secret) {
        if let Some(access_token) = value
            .get("access_token")
            .and_then(serde_json::Value::as_str)
        {
            if access_token.trim().is_empty() {
                return Err(YandexDiskError::auth_unavailable(
                    "yacli keyring JSON has an empty access_token",
                ));
            }
            return Ok(StoredToken {
                access_token: SecretString::from(access_token.to_string()),
                refresh_token: json_string(&value, "refresh_token").map(SecretString::from),
                expires_at_unix: json_i64(&value, "expires_at_unix")
                    .or_else(|| json_i64(&value, "expires_at_epoch_secs")),
                scope: json_string(&value, "scope"),
                token_type: json_string(&value, "token_type"),
                client_id: json_string(&value, "client_id"),
            });
        }
    }

    if let Ok(token) = serde_json::from_str::<StoredTokenJson>(secret) {
        return Ok(token.into());
    }

    let access_token = secret.trim();
    if access_token.is_empty() {
        return Err(YandexDiskError::auth_unavailable(
            "yacli keyring entry is empty",
        ));
    }

    Ok(StoredToken {
        access_token: SecretString::from(access_token.to_string()),
        refresh_token: None,
        expires_at_unix: None,
        scope: Some("disk".to_string()),
        token_type: Some("OAuth".to_string()),
        client_id: None,
    })
}

fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn json_i64(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
    })
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_url_contains_client_scope_and_pkce() {
        let flow = OAuthFlow::new("client-123", "disk", "ya-disk-sync").unwrap();
        let request = flow.authorization_request().unwrap();

        assert!(request.authorization_url.contains("client_id=client-123"));
        assert!(request.authorization_url.contains("scope=disk"));
        assert!(request.authorization_url.contains("code_challenge="));
        assert!(request
            .authorization_url
            .contains("code_challenge_method=S256"));
        assert!(!request.pkce_verifier.expose_secret().is_empty());
    }

    #[test]
    fn stored_token_debug_does_not_expose_secrets() {
        let token = StoredToken {
            access_token: SecretString::from("access-secret"),
            refresh_token: Some(SecretString::from("refresh-secret")),
            expires_at_unix: Some(42),
            scope: Some("disk".to_string()),
            token_type: Some("bearer".to_string()),
            client_id: Some("client".to_string()),
        };

        let debug = format!("{token:?}");

        assert!(!debug.contains("access-secret"));
        assert!(!debug.contains("refresh-secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn memory_token_store_round_trips_without_debug_leak() {
        let store = MemoryTokenStore::new();
        let token = StoredToken {
            access_token: SecretString::from("access-secret"),
            refresh_token: None,
            expires_at_unix: None,
            scope: Some("disk".to_string()),
            token_type: None,
            client_id: None,
        };

        store.save_token("default", &token).unwrap();
        let loaded = store.load_token("default").unwrap().unwrap();

        assert_eq!(loaded.access_token.expose_secret(), "access-secret");
        assert!(store.delete_token("default").unwrap());
        assert!(store.load_token("default").unwrap().is_none());
    }

    #[test]
    fn oauth_client_id_prefers_cli_then_config_then_env() {
        let mut config = yds_core::config::default_config().yandex_disk;
        config.oauth_client_id = Some("config-client".to_string());

        assert_eq!(
            resolve_oauth_client_id(&config, Some("cli-client")),
            Some("cli-client".to_string())
        );
        assert_eq!(
            resolve_oauth_client_id(&config, None),
            Some("config-client".to_string())
        );
    }

    #[test]
    fn yacli_candidates_prefer_default_disk_account() {
        let content = r#"
version = 1

[accounts.other]
email = "other@yandex.ru"

[accounts.other.disk]
enabled = true
auth_mode = "oauth"
credential_ref = "store:disk"

[accounts.main]
email = "main@yandex.ru"
default = true

[accounts.main.disk]
enabled = true
auth_mode = "oauth"
credential_ref = "store:disk"
"#;

        let candidates = yacli_candidates_from_accounts_toml(content);

        assert_eq!(candidates, ["main:disk", "other:disk"]);
    }

    #[test]
    fn raw_yacli_token_is_wrapped_without_debug_leak() {
        let token = parse_yacli_disk_token("raw-access-token").unwrap();

        assert_eq!(token.access_token.expose_secret(), "raw-access-token");
        assert_eq!(token.scope.as_deref(), Some("disk"));
        assert!(!format!("{token:?}").contains("raw-access-token"));
    }

    #[test]
    fn yacli_json_token_shape_is_imported_as_access_token() {
        let token = parse_yacli_disk_token(
            r#"{
                "kind": "oauth",
                "access_token": "json-access-token",
                "expires_at_epoch_secs": "12345",
                "scope": "disk",
                "token_type": "OAuth",
                "client_id": "client"
            }"#,
        )
        .unwrap();

        assert_eq!(token.access_token.expose_secret(), "json-access-token");
        assert_eq!(token.expires_at_unix, Some(12345));
        assert_eq!(token.scope.as_deref(), Some("disk"));
        assert_eq!(token.client_id.as_deref(), Some("client"));
        assert!(!format!("{token:?}").contains("json-access-token"));
    }
}
