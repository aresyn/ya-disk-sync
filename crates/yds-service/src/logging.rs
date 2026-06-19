use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{Duration as StdDuration, SystemTime},
};

use thiserror::Error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Error)]
pub enum LoggingError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to initialize tracing subscriber")]
    Init,
}

pub struct FileLogGuard {
    _guard: WorkerGuard,
}

pub fn init_file_logging(
    logs_dir: impl AsRef<Path>,
    level: &str,
) -> Result<FileLogGuard, LoggingError> {
    let logs_dir = logs_dir.as_ref();
    fs::create_dir_all(logs_dir).map_err(|source| LoggingError::Io {
        path: logs_dir.to_path_buf(),
        source,
    })?;
    let appender = tracing_appender::rolling::daily(logs_dir, "ya-disk-sync.log");
    let (writer, guard) = tracing_appender::non_blocking(appender);
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(false)
        .with_target(false)
        .try_init()
        .map_err(|_| LoggingError::Init)?;
    Ok(FileLogGuard { _guard: guard })
}

pub fn cleanup_old_logs(
    logs_dir: impl AsRef<Path>,
    retention_days: u32,
) -> Result<usize, LoggingError> {
    let logs_dir = logs_dir.as_ref();
    let cutoff = SystemTime::now()
        .checked_sub(StdDuration::from_secs(
            u64::from(retention_days) * 24 * 60 * 60,
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    let entries = match fs::read_dir(logs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(LoggingError::Io {
                path: logs_dir.to_path_buf(),
                source,
            });
        }
    };

    for entry in entries {
        let entry = entry.map_err(|source| LoggingError::Io {
            path: logs_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let metadata = entry.metadata().map_err(|source| LoggingError::Io {
            path: path.clone(),
            source,
        })?;
        let modified = metadata.modified().map_err(|source| LoggingError::Io {
            path: path.clone(),
            source,
        })?;
        if modified < cutoff {
            fs::remove_file(&path).map_err(|source| LoggingError::Io {
                path: path.clone(),
                source,
            })?;
            removed += 1;
        }
    }

    Ok(removed)
}

pub fn tail_latest_log(
    logs_dir: impl AsRef<Path>,
    lines: usize,
) -> Result<Vec<String>, LoggingError> {
    let Some(path) = latest_log_path(logs_dir.as_ref())? else {
        return Ok(Vec::new());
    };
    let content = fs::read_to_string(&path).map_err(|source| LoggingError::Io {
        path: path.clone(),
        source,
    })?;
    let mut output: Vec<_> = content.lines().map(ToOwned::to_owned).collect();
    if output.len() > lines {
        output = output.split_off(output.len() - lines);
    }
    Ok(output)
}

fn latest_log_path(logs_dir: &Path) -> Result<Option<PathBuf>, LoggingError> {
    let entries = match fs::read_dir(logs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(LoggingError::Io {
                path: logs_dir.to_path_buf(),
                source,
            });
        }
    };
    let mut latest: Option<(SystemTime, PathBuf)> = None;
    for entry in entries {
        let entry = entry.map_err(|source| LoggingError::Io {
            path: logs_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map_err(|source| LoggingError::Io {
                path: path.clone(),
                source,
            })?;
        if latest
            .as_ref()
            .map(|(latest_modified, _)| modified > *latest_modified)
            .unwrap_or(true)
        {
            latest = Some((modified, path));
        }
    }
    Ok(latest.map(|(_, path)| path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_latest_log_returns_last_lines() {
        let temp = tempfile::tempdir().unwrap();
        let log_path = temp.path().join("ya-disk-sync.log.2026-06-14");
        fs::write(&log_path, "one\ntwo\nthree\n").unwrap();

        let tail = tail_latest_log(temp.path(), 2).unwrap();

        assert_eq!(tail, ["two", "three"]);
    }

    #[test]
    fn cleanup_old_logs_removes_expired_files() {
        let temp = tempfile::tempdir().unwrap();
        let old = temp.path().join("old.log");
        let fresh = temp.path().join("fresh.log");
        fs::write(&old, "old").unwrap();
        fs::write(&fresh, "fresh").unwrap();
        let old_time = filetime::FileTime::from_system_time(
            SystemTime::now() - StdDuration::from_secs(10 * 24 * 60 * 60),
        );
        filetime::set_file_mtime(&old, old_time).unwrap();

        let removed = cleanup_old_logs(temp.path(), 7).unwrap();

        assert_eq!(removed, 1);
        assert!(!old.exists());
        assert!(fresh.exists());
    }
}
