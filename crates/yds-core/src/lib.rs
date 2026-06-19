pub mod config;
pub mod exclusions;
pub mod path_mapping;

use serde::Serialize;
use thiserror::Error;

pub const APP_NAME: &str = "ya-disk-sync";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const COMPONENT_NAME: &str = "core";

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppError {
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentHealth {
    Ok,
}

impl ComponentHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComponentStatus {
    name: &'static str,
    health: ComponentHealth,
    details: &'static str,
}

impl ComponentStatus {
    #[must_use]
    pub const fn ok(name: &'static str, details: &'static str) -> Self {
        Self {
            name,
            health: ComponentHealth::Ok,
            details,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn health(&self) -> ComponentHealth {
        self.health
    }

    #[must_use]
    pub const fn details(&self) -> &'static str {
        self.details
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticReport {
    app_name: &'static str,
    version: &'static str,
    status: ComponentHealth,
    components: Vec<ComponentStatus>,
}

impl DiagnosticReport {
    #[must_use]
    pub fn new(components: Vec<ComponentStatus>) -> Self {
        Self {
            app_name: APP_NAME,
            version: APP_VERSION,
            status: ComponentHealth::Ok,
            components,
        }
    }

    #[must_use]
    pub const fn app_name(&self) -> &'static str {
        self.app_name
    }

    #[must_use]
    pub const fn version(&self) -> &'static str {
        self.version
    }

    #[must_use]
    pub const fn status(&self) -> ComponentHealth {
        self.status
    }

    #[must_use]
    pub fn components(&self) -> &[ComponentStatus] {
        &self.components
    }
}

#[must_use]
pub const fn component_status() -> ComponentStatus {
    ComponentStatus::ok(COMPONENT_NAME, "shared domain boundary is available")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_metadata_is_stable() {
        assert_eq!(APP_NAME, "ya-disk-sync");
        assert_eq!(APP_VERSION, "0.1.1");
    }

    #[test]
    fn diagnostic_report_preserves_component_order() {
        let report = DiagnosticReport::new(vec![
            ComponentStatus::ok("first", "available"),
            ComponentStatus::ok("second", "available"),
        ]);

        let names: Vec<_> = report
            .components()
            .iter()
            .map(ComponentStatus::name)
            .collect();

        assert_eq!(names, ["first", "second"]);
        assert_eq!(report.status(), ComponentHealth::Ok);
    }
}
