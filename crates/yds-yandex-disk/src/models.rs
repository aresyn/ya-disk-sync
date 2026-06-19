use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{YandexDiskError, YandexDiskErrorKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaInfo {
    pub total_space: Option<u64>,
    pub used_space: Option<u64>,
    pub trash_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub quota: QuotaInfo,
    pub system_folders: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub path: String,
    pub name: Option<String>,
    pub resource_type: ResourceType,
    pub size_bytes: Option<u64>,
    pub md5: Option<String>,
    pub revision: Option<u64>,
    pub created: Option<String>,
    pub modified: Option<String>,
}

impl ResourceMetadata {
    #[must_use]
    pub fn file(path: impl Into<String>, size_bytes: u64) -> Self {
        let path = path.into();
        Self {
            name: last_remote_segment(&path),
            path,
            resource_type: ResourceType::File,
            size_bytes: Some(size_bytes),
            md5: None,
            revision: None,
            created: None,
            modified: None,
        }
    }

    #[must_use]
    pub fn directory(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            name: last_remote_segment(&path),
            path,
            resource_type: ResourceType::Directory,
            size_bytes: None,
            md5: None,
            revision: None,
            created: None,
            modified: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRecursiveOptions {
    #[serde(default)]
    pub prune_remote_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    InProgress,
    Success,
    Failed,
}

impl OperationStatus {
    pub(crate) fn from_api(value: &str) -> Result<Self, YandexDiskError> {
        match value {
            "in-progress" | "in_progress" => Ok(Self::InProgress),
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            other => Err(YandexDiskError::new(
                YandexDiskErrorKind::Permanent,
                format!("unknown Yandex Disk operation status: {other}"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ApiDiskInfo {
    pub total_space: Option<u64>,
    pub used_space: Option<u64>,
    pub trash_size: Option<u64>,
    #[serde(default)]
    pub system_folders: BTreeMap<String, String>,
}

impl From<ApiDiskInfo> for DiskInfo {
    fn from(value: ApiDiskInfo) -> Self {
        Self {
            quota: QuotaInfo {
                total_space: value.total_space,
                used_space: value.used_space,
                trash_size: value.trash_size,
            },
            system_folders: value.system_folders,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ApiResource {
    pub path: String,
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub resource_type: ApiResourceType,
    pub size: Option<u64>,
    pub md5: Option<String>,
    pub revision: Option<u64>,
    pub created: Option<String>,
    pub modified: Option<String>,
    #[serde(rename = "_embedded")]
    pub embedded: Option<ApiEmbeddedResources>,
}

impl From<ApiResource> for ResourceMetadata {
    fn from(value: ApiResource) -> Self {
        Self {
            path: value.path,
            name: value.name,
            resource_type: value.resource_type.into(),
            size_bytes: value.size,
            md5: value.md5,
            revision: value.revision,
            created: value.created,
            modified: value.modified,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ApiEmbeddedResources {
    #[serde(default)]
    pub items: Vec<ApiResource>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ApiResourceType {
    File,
    #[serde(rename = "dir")]
    Directory,
}

impl From<ApiResourceType> for ResourceType {
    fn from(value: ApiResourceType) -> Self {
        match value {
            ApiResourceType::File => Self::File,
            ApiResourceType::Directory => Self::Directory,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LinkResponse {
    pub href: String,
    pub method: Option<String>,
    pub templated: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct ApiOperationStatus {
    pub status: String,
}

fn last_remote_segment(path: &str) -> Option<String> {
    path.rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_status_reads_known_yandex_values_strictly() {
        assert_eq!(
            OperationStatus::from_api("in-progress").unwrap(),
            OperationStatus::InProgress
        );
        assert_eq!(
            OperationStatus::from_api("success").unwrap(),
            OperationStatus::Success
        );
        assert!(OperationStatus::from_api("done").is_err());
    }
}
