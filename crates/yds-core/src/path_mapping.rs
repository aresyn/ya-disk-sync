use thiserror::Error;

const REMOTE_PREFIX: &str = "disk:/";
const RESERVED_REMOTE_CHARS: &[char] = &['<', '>', ':', '"', '|', '?', '*', '\\'];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathMappingError {
    #[error("remote path must start with disk:/")]
    InvalidRemoteRoot,
    #[error("local path must be an absolute Windows drive path or an absolute Linux path")]
    InvalidLocalPath,
    #[error("UNC paths are not supported in config v1")]
    UnsupportedUncPath,
    #[error("drive-relative Windows paths are not supported in config v1")]
    UnsupportedDriveRelativePath,
    #[error("path segment became empty after sanitization: {0}")]
    EmptySanitizedSegment(String),
    #[error("relative path segments are not allowed: {0}")]
    RelativeSegment(String),
}

#[must_use]
pub fn is_valid_remote_root(path: &str) -> bool {
    sanitize_remote_path(path).is_ok()
}

pub fn canonical_remote_path(
    remote_root: &str,
    local_path: &str,
    remote_path_override: Option<&str>,
) -> Result<String, PathMappingError> {
    if let Some(override_path) = remote_path_override.filter(|value| !value.trim().is_empty()) {
        return sanitize_remote_path(override_path);
    }

    let mut remote_segments = remote_root_segments(remote_root)?;
    remote_segments.extend(local_path_segments(local_path)?);
    build_remote_path(remote_segments)
}

pub fn sanitize_remote_path(remote_path: &str) -> Result<String, PathMappingError> {
    build_remote_path(remote_root_segments(remote_path)?)
}

pub fn detect_remote_path_collision_key(remote_path: &str) -> Result<String, PathMappingError> {
    Ok(sanitize_remote_path(remote_path)?.to_lowercase())
}

fn remote_root_segments(remote_path: &str) -> Result<Vec<String>, PathMappingError> {
    let trimmed = remote_path.trim();
    if !trimmed.starts_with(REMOTE_PREFIX) {
        return Err(PathMappingError::InvalidRemoteRoot);
    }

    let body = trimmed[REMOTE_PREFIX.len()..].trim_matches('/');
    if body.is_empty() {
        return Err(PathMappingError::InvalidRemoteRoot);
    }

    body.split('/').map(sanitize_segment).collect()
}

fn local_path_segments(local_path: &str) -> Result<Vec<String>, PathMappingError> {
    let trimmed = local_path.trim();
    if trimmed.starts_with("\\\\") || trimmed.starts_with("//") {
        return Err(PathMappingError::UnsupportedUncPath);
    }

    if is_drive_relative_path(trimmed) {
        return Err(PathMappingError::UnsupportedDriveRelativePath);
    }

    if is_windows_drive_path(trimmed) {
        let drive = trimmed[..1].to_ascii_uppercase();
        let rest = &trimmed[3..];
        let mut segments = vec![drive];
        segments.extend(split_local_segments(rest)?);
        return Ok(segments);
    }

    if let Some(rest) = trimmed.strip_prefix('/') {
        return split_local_segments(rest);
    }

    Err(PathMappingError::InvalidLocalPath)
}

fn is_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
}

fn is_drive_relative_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && !is_windows_drive_path(path)
}

fn split_local_segments(path: &str) -> Result<Vec<String>, PathMappingError> {
    path.split(['\\', '/'])
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            if segment == "." || segment == ".." {
                return Err(PathMappingError::RelativeSegment(segment.to_string()));
            }
            sanitize_segment(segment)
        })
        .collect()
}

fn sanitize_segment(segment: &str) -> Result<String, PathMappingError> {
    let sanitized: String = segment
        .chars()
        .filter(|ch| !ch.is_control() && !RESERVED_REMOTE_CHARS.contains(ch) && *ch != '/')
        .collect();

    if sanitized.is_empty() {
        return Err(PathMappingError::EmptySanitizedSegment(segment.to_string()));
    }

    Ok(sanitized)
}

fn build_remote_path(segments: Vec<String>) -> Result<String, PathMappingError> {
    if segments.is_empty() {
        return Err(PathMappingError::InvalidRemoteRoot);
    }

    Ok(format!("{REMOTE_PREFIX}{}", segments.join("/")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_windows_drive_path_to_canonical_remote_path() {
        let mapped = canonical_remote_path("disk:/Backup", r"C:\Data\Projects", None).unwrap();

        assert_eq!(mapped, "disk:/Backup/C/Data/Projects");
    }

    #[test]
    fn maps_windows_path_with_forward_slashes() {
        let mapped = canonical_remote_path("disk:/Backup", "C:/Data/Music", None).unwrap();

        assert_eq!(mapped, "disk:/Backup/C/Data/Music");
    }

    #[test]
    fn maps_linux_absolute_path() {
        let mapped = canonical_remote_path("disk:/VPS-01", "/var/www/site", None).unwrap();

        assert_eq!(mapped, "disk:/VPS-01/var/www/site");
    }

    #[test]
    fn remote_path_override_wins() {
        let mapped =
            canonical_remote_path("disk:/VPS-01", "/var/www/site", Some("disk:/custom/site"))
                .unwrap();

        assert_eq!(mapped, "disk:/custom/site");
    }

    #[test]
    fn rejects_unc_and_drive_relative_paths() {
        assert_eq!(
            canonical_remote_path("disk:/root", r"\\server\share", None).unwrap_err(),
            PathMappingError::UnsupportedUncPath
        );
        assert_eq!(
            canonical_remote_path("disk:/root", "D:folder", None).unwrap_err(),
            PathMappingError::UnsupportedDriveRelativePath
        );
    }

    #[test]
    fn sanitizes_reserved_remote_chars() {
        let mapped = canonical_remote_path("disk:/root", r"C:\bad<name>\a?b.txt", None).unwrap();

        assert_eq!(mapped, "disk:/root/C/badname/ab.txt");
    }

    #[test]
    fn collision_key_uses_sanitized_path() {
        let first = detect_remote_path_collision_key("disk:/root/a?b.txt").unwrap();
        let second = detect_remote_path_collision_key("disk:/root/ab.txt").unwrap();

        assert_eq!(first, second);
    }
}
