use std::{path::PathBuf, process::Command};

use thiserror::Error;
use yds_core::ComponentStatus;

pub const COMPONENT_NAME: &str = "linux";
pub const SERVICE_NAME: &str = "ya-disk-sync";
pub const DEFAULT_UNIT_PATH: &str = "/etc/systemd/system/ya-disk-sync.service";
pub const DEFAULT_BINARY_PATH: &str = "/usr/local/bin/ya-disk-sync";
pub const DEFAULT_CONFIG_PATH: &str = "/etc/ya-disk-sync/config.json";

#[derive(Debug, Error)]
pub enum LinuxServiceError {
    #[error("systemd helpers are supported only on Linux")]
    UnsupportedPlatform,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("command `{command}` failed with status {status}")]
    CommandFailed { command: String, status: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemdUnit {
    pub binary_path: PathBuf,
    pub config_path: PathBuf,
    pub user: String,
    pub group: String,
}

impl Default for SystemdUnit {
    fn default() -> Self {
        Self {
            binary_path: PathBuf::from(DEFAULT_BINARY_PATH),
            config_path: PathBuf::from(DEFAULT_CONFIG_PATH),
            user: SERVICE_NAME.to_string(),
            group: SERVICE_NAME.to_string(),
        }
    }
}

impl SystemdUnit {
    #[must_use]
    pub fn render(&self) -> String {
        format!(
            "[Unit]\n\
Description=ya-disk-sync one-way Yandex Disk mirror\n\
After=network-online.target\n\
Wants=network-online.target\n\n\
[Service]\n\
Type=simple\n\
ExecStart={} daemon --config {}\n\
Restart=on-failure\n\
RestartSec=30\n\
User={}\n\
Group={}\n\
StateDirectory=ya-disk-sync\n\
LogsDirectory=ya-disk-sync\n\n\
[Install]\n\
WantedBy=multi-user.target\n",
            self.binary_path.display(),
            self.config_path.display(),
            self.user,
            self.group
        )
    }
}

#[derive(Debug, Clone)]
pub struct SystemdServiceManager {
    unit_path: PathBuf,
}

impl Default for SystemdServiceManager {
    fn default() -> Self {
        Self {
            unit_path: PathBuf::from(DEFAULT_UNIT_PATH),
        }
    }
}

impl SystemdServiceManager {
    #[must_use]
    pub fn new(unit_path: impl Into<PathBuf>) -> Self {
        Self {
            unit_path: unit_path.into(),
        }
    }

    pub fn install(&self, unit: &SystemdUnit, force: bool) -> Result<(), LinuxServiceError> {
        ensure_linux()?;
        if self.unit_path.exists() && !force {
            return Err(LinuxServiceError::CommandFailed {
                command: "install".to_string(),
                status: format!("{} already exists", self.unit_path.display()),
            });
        }
        if let Some(parent) = self.unit_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.unit_path, unit.render())?;
        run_systemctl(["daemon-reload"])?;
        run_systemctl(["enable", SERVICE_NAME])?;
        Ok(())
    }

    pub fn uninstall(&self) -> Result<(), LinuxServiceError> {
        ensure_linux()?;
        let _ = run_systemctl(["disable", SERVICE_NAME]);
        if self.unit_path.exists() {
            std::fs::remove_file(&self.unit_path)?;
        }
        run_systemctl(["daemon-reload"])?;
        Ok(())
    }

    pub fn start(&self) -> Result<(), LinuxServiceError> {
        ensure_linux()?;
        run_systemctl(["start", SERVICE_NAME])
    }

    pub fn stop(&self) -> Result<(), LinuxServiceError> {
        ensure_linux()?;
        run_systemctl(["stop", SERVICE_NAME])
    }

    pub fn restart(&self) -> Result<(), LinuxServiceError> {
        ensure_linux()?;
        run_systemctl(["restart", SERVICE_NAME])
    }

    pub fn status(&self) -> Result<String, LinuxServiceError> {
        ensure_linux()?;
        let output = Command::new("systemctl")
            .args(["is-active", SERVICE_NAME])
            .output()?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub fn update(&self, unit: &SystemdUnit, force: bool) -> Result<(), LinuxServiceError> {
        self.install(unit, force)?;
        self.restart()
    }
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(COMPONENT_NAME, "Linux systemd boundary is available")
}

fn ensure_linux() -> Result<(), LinuxServiceError> {
    if cfg!(target_os = "linux") {
        Ok(())
    } else {
        Err(LinuxServiceError::UnsupportedPlatform)
    }
}

fn run_systemctl<const N: usize>(args: [&str; N]) -> Result<(), LinuxServiceError> {
    let status = Command::new("systemctl").args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(LinuxServiceError::CommandFailed {
            command: "systemctl".to_string(),
            status: status.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yds_core::ComponentHealth;

    #[test]
    fn component_status_is_ok() {
        let status = component_status();

        assert_eq!(status.name(), COMPONENT_NAME);
        assert_eq!(status.health(), ComponentHealth::Ok);
    }

    #[test]
    fn systemd_unit_render_matches_documented_shape() {
        let unit = SystemdUnit::default();
        let rendered = unit.render();

        assert!(rendered.contains("Description=ya-disk-sync one-way Yandex Disk mirror"));
        assert!(rendered.contains(
            "ExecStart=/usr/local/bin/ya-disk-sync daemon --config /etc/ya-disk-sync/config.json"
        ));
        assert!(rendered.contains("Restart=on-failure"));
        assert!(rendered.contains("User=ya-disk-sync"));
        assert!(rendered.contains("StateDirectory=ya-disk-sync"));
        assert!(rendered.contains("LogsDirectory=ya-disk-sync"));
    }
}
