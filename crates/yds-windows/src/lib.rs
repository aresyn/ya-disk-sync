use std::{path::PathBuf, process::Command};

#[cfg(windows)]
use std::{ffi::OsString, sync::mpsc, time::Duration};

use thiserror::Error;
use yds_core::{ComponentStatus, APP_VERSION};
use yds_service::{ControlClient, RuntimeHost, RuntimeHostOptions, RuntimeStatus};

pub const COMPONENT_NAME: &str = "windows";
pub const SERVICE_NAME: &str = "ya-disk-sync";
pub const SERVICE_DISPLAY_NAME: &str = "ya-disk-sync";

#[derive(Debug, Error)]
pub enum WindowsServiceError {
    #[error("Windows service helpers are supported only on Windows")]
    UnsupportedPlatform,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("service command failed: {0}")]
    CommandFailed(String),
    #[error("Windows service API error: {0}")]
    ServiceApi(String),
    #[error("control API error: {0}")]
    Control(#[from] yds_service::ServiceError),
    #[error("async runtime error: {0}")]
    Runtime(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsServiceInstallConfig {
    pub executable_path: PathBuf,
    pub config_path: PathBuf,
}

impl WindowsServiceInstallConfig {
    #[must_use]
    pub fn service_run_arguments(&self) -> Vec<String> {
        vec![
            "service".to_string(),
            "run".to_string(),
            "--config".to_string(),
            self.config_path.display().to_string(),
        ]
    }

    #[must_use]
    pub fn bin_path(&self) -> String {
        format!(
            "\"{}\" service run --config \"{}\"",
            self.executable_path.display(),
            self.config_path.display()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowsServiceStatus {
    Running,
    Stopped,
    NotInstalled,
    Unknown(String),
}

impl WindowsServiceStatus {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::NotInstalled => "not_installed",
            Self::Unknown(_) => "unknown",
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct WindowsServiceManager;

impl WindowsServiceManager {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn install(
        &self,
        config: &WindowsServiceInstallConfig,
        force: bool,
    ) -> Result<(), WindowsServiceError> {
        install_service(config, force)
    }

    pub fn start(&self) -> Result<(), WindowsServiceError> {
        start_service()
    }

    pub fn stop(&self) -> Result<(), WindowsServiceError> {
        stop_service()
    }

    pub fn restart(&self) -> Result<(), WindowsServiceError> {
        let _ = self.stop();
        self.start()
    }

    pub fn uninstall(&self) -> Result<(), WindowsServiceError> {
        uninstall_service()
    }

    pub fn update(
        &self,
        config: &WindowsServiceInstallConfig,
        force: bool,
    ) -> Result<(), WindowsServiceError> {
        self.install(config, force)?;
        let _ = self.restart();
        Ok(())
    }

    pub fn status(&self) -> Result<WindowsServiceStatus, WindowsServiceError> {
        service_status()
    }
}

pub struct WindowsServiceRunner;

impl WindowsServiceRunner {
    pub fn run(config_path: PathBuf) -> Result<(), WindowsServiceError> {
        run_service_dispatcher(config_path)
    }

    pub async fn run_console(config_path: PathBuf) -> Result<(), WindowsServiceError> {
        let host = RuntimeHost::start(RuntimeHostOptions::new(config_path)).await?;
        let _ = tokio::signal::ctrl_c().await;
        host.shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayAction {
    Status,
    OpenWebUi,
    RunSync,
    StopSync,
    OpenLogs,
    Version,
    Quit,
}

#[derive(Debug, Clone)]
pub struct TrayApp {
    bind_address: String,
    port: u16,
    logs_dir: PathBuf,
}

impl TrayApp {
    #[must_use]
    pub fn new(bind_address: impl Into<String>, port: u16, logs_dir: impl Into<PathBuf>) -> Self {
        Self {
            bind_address: bind_address.into(),
            port,
            logs_dir: logs_dir.into(),
        }
    }

    #[must_use]
    pub fn menu_labels() -> Vec<&'static str> {
        vec![
            "Status",
            "Open Web UI",
            "Run sync",
            "Stop sync",
            "Open logs",
            "Version",
            "Quit",
        ]
    }

    pub async fn handle_action(&self, action: TrayAction) -> Result<String, WindowsServiceError> {
        match action {
            TrayAction::Status => {
                let status = self.client().status().await?;
                Ok(format!("status: {}", status.status.as_str()))
            }
            TrayAction::OpenWebUi => {
                open_target(&format!("http://{}:{}", self.bind_address, self.port))?;
                Ok("web UI opened".to_string())
            }
            TrayAction::RunSync => {
                let response = self.client().request_run().await?;
                Ok(format!("sync run: {}", response.message))
            }
            TrayAction::StopSync => {
                let response = self.client().request_stop().await?;
                Ok(format!("sync stop: {}", response.message))
            }
            TrayAction::OpenLogs => {
                open_target(&self.logs_dir.display().to_string())?;
                Ok("logs opened".to_string())
            }
            TrayAction::Version => Ok(format!("ya-disk-sync {APP_VERSION}")),
            TrayAction::Quit => Ok("tray quit".to_string()),
        }
    }

    fn client(&self) -> ControlClient {
        ControlClient::new(&self.bind_address, self.port)
    }
}

pub struct TrayRuntime {
    app: TrayApp,
}

impl TrayRuntime {
    #[must_use]
    pub fn new(app: TrayApp) -> Self {
        Self { app }
    }

    pub async fn run(self) -> Result<(), WindowsServiceError> {
        run_tray_runtime(self.app).await
    }
}

#[must_use]
pub fn component_status() -> ComponentStatus {
    ComponentStatus::ok(
        COMPONENT_NAME,
        "Windows service and tray boundary is available",
    )
}

#[cfg(windows)]
fn install_service(
    config: &WindowsServiceInstallConfig,
    force: bool,
) -> Result<(), WindowsServiceError> {
    use windows_service::{
        service::{ServiceAccess, ServiceErrorControl, ServiceInfo, ServiceStartType, ServiceType},
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    if force {
        let _ = uninstall_service();
    }
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )
    .map_err(service_api_error)?;
    let service_info = ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from(SERVICE_DISPLAY_NAME),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: config.executable_path.clone(),
        launch_arguments: config
            .service_run_arguments()
            .into_iter()
            .map(OsString::from)
            .collect(),
        dependencies: Vec::new(),
        account_name: None,
        account_password: None,
    };
    let service = manager
        .create_service(&service_info, ServiceAccess::QUERY_STATUS)
        .map_err(service_api_error)?;
    service
        .set_description("Local to Yandex Disk one-way mirror runtime")
        .map_err(service_api_error)?;
    Ok(())
}

#[cfg(not(windows))]
fn install_service(
    _config: &WindowsServiceInstallConfig,
    _force: bool,
) -> Result<(), WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
fn open_service(
    access: windows_service::service::ServiceAccess,
) -> Result<windows_service::service::Service, WindowsServiceError> {
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .map_err(service_api_error)?;
    manager.open_service(SERVICE_NAME, access).map_err(|error| {
        if looks_like_missing_service(&error) {
            WindowsServiceError::ServiceApi("service is not installed".to_string())
        } else {
            service_api_error(error)
        }
    })
}

#[cfg(windows)]
fn start_service() -> Result<(), WindowsServiceError> {
    use windows_service::service::ServiceAccess;

    let service = open_service(ServiceAccess::START)?;
    service.start(&[] as &[&str]).map_err(service_api_error)
}

#[cfg(not(windows))]
fn start_service() -> Result<(), WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
fn stop_service() -> Result<(), WindowsServiceError> {
    use windows_service::service::ServiceAccess;

    let service = open_service(ServiceAccess::STOP)?;
    service.stop().map_err(service_api_error)?;
    Ok(())
}

#[cfg(not(windows))]
fn stop_service() -> Result<(), WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
fn uninstall_service() -> Result<(), WindowsServiceError> {
    use windows_service::service::ServiceAccess;

    let service = open_service(ServiceAccess::DELETE)?;
    service.delete().map_err(service_api_error)
}

#[cfg(not(windows))]
fn uninstall_service() -> Result<(), WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
fn service_status() -> Result<WindowsServiceStatus, WindowsServiceError> {
    use windows_service::service::{ServiceAccess, ServiceState};

    let service = match open_service(ServiceAccess::QUERY_STATUS) {
        Ok(service) => service,
        Err(WindowsServiceError::ServiceApi(message)) if message.contains("not installed") => {
            return Ok(WindowsServiceStatus::NotInstalled);
        }
        Err(error) => return Err(error),
    };
    let status = service.query_status().map_err(service_api_error)?;
    Ok(match status.current_state {
        ServiceState::Running => WindowsServiceStatus::Running,
        ServiceState::Stopped => WindowsServiceStatus::Stopped,
        other => WindowsServiceStatus::Unknown(format!("{other:?}")),
    })
}

#[cfg(not(windows))]
fn service_status() -> Result<WindowsServiceStatus, WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
fn run_service_dispatcher(config_path: PathBuf) -> Result<(), WindowsServiceError> {
    SERVICE_CONFIG_PATH
        .set(config_path)
        .map_err(|_| WindowsServiceError::Runtime("service config path already set".to_string()))?;
    windows_service::service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .map_err(service_api_error)
}

#[cfg(not(windows))]
fn run_service_dispatcher(_config_path: PathBuf) -> Result<(), WindowsServiceError> {
    Err(WindowsServiceError::UnsupportedPlatform)
}

#[cfg(windows)]
static SERVICE_CONFIG_PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, service_main);

#[cfg(windows)]
fn service_main(_arguments: Vec<OsString>) {
    if let Err(error) = run_service_main() {
        eprintln!("ya-disk-sync service error: {error}");
    }
}

#[cfg(windows)]
fn run_service_main() -> Result<(), WindowsServiceError> {
    use windows_service::{
        service::{
            ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
            ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
    };

    let config_path = SERVICE_CONFIG_PATH.get().cloned().ok_or_else(|| {
        WindowsServiceError::Runtime("service config path is not set".to_string())
    })?;
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = stop_tx.send(());
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };
    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)
        .map_err(service_api_error)?;

    set_service_state(
        &status_handle,
        ServiceState::StartPending,
        ServiceControlAccept::empty(),
        1,
        Duration::from_secs(30),
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| WindowsServiceError::Runtime(error.to_string()))?;
    let result = runtime.block_on(async move {
        let host = RuntimeHost::start(RuntimeHostOptions::new(config_path)).await?;
        set_service_state(
            &status_handle,
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            0,
            Duration::from_secs(0),
        )?;
        let _ = tokio::task::spawn_blocking(move || stop_rx.recv()).await;
        set_service_state(
            &status_handle,
            ServiceState::StopPending,
            ServiceControlAccept::empty(),
            1,
            Duration::from_secs(30),
        )?;
        host.shutdown().await?;
        Ok::<(), WindowsServiceError>(())
    });

    let exit_code = if result.is_ok() {
        ServiceExitCode::NO_ERROR
    } else {
        ServiceExitCode::ServiceSpecific(1)
    };
    status_handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code,
            checkpoint: 0,
            wait_hint: Duration::from_secs(0),
            process_id: None,
        })
        .map_err(service_api_error)?;
    result
}

#[cfg(windows)]
fn set_service_state(
    status_handle: &windows_service::service_control_handler::ServiceStatusHandle,
    current_state: windows_service::service::ServiceState,
    controls_accepted: windows_service::service::ServiceControlAccept,
    checkpoint: u32,
    wait_hint: Duration,
) -> Result<(), WindowsServiceError> {
    use windows_service::service::{ServiceExitCode, ServiceStatus, ServiceType};

    status_handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state,
            controls_accepted,
            exit_code: ServiceExitCode::NO_ERROR,
            checkpoint,
            wait_hint,
            process_id: None,
        })
        .map_err(service_api_error)
}

#[cfg(windows)]
fn service_api_error(error: windows_service::Error) -> WindowsServiceError {
    WindowsServiceError::ServiceApi(error.to_string())
}

#[cfg(windows)]
fn looks_like_missing_service(error: &windows_service::Error) -> bool {
    matches!(
        error,
        windows_service::Error::Winapi(source)
            if source.raw_os_error() == Some(1060)
    ) || error.to_string().contains("1060")
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("does not exist")
}

#[cfg(not(all(windows, feature = "native-tray")))]
async fn run_tray_runtime(app: TrayApp) -> Result<(), WindowsServiceError> {
    if std::env::var("YDS_TRAY_TEST_MODE").ok().as_deref() == Some("1") {
        let _ = app.handle_action(TrayAction::Status).await;
        return Ok(());
    }
    if cfg!(windows) {
        Err(WindowsServiceError::Runtime(
            "native tray backend is not enabled; rebuild with feature native-tray".to_string(),
        ))
    } else {
        Err(WindowsServiceError::UnsupportedPlatform)
    }
}

#[cfg(all(windows, feature = "native-tray"))]
async fn run_tray_runtime(app: TrayApp) -> Result<(), WindowsServiceError> {
    if std::env::var("YDS_TRAY_TEST_MODE").ok().as_deref() == Some("1") {
        let _ = app.handle_action(TrayAction::Status).await;
        return Ok(());
    }
    tokio::task::spawn_blocking(move || run_windows_tray_loop(app))
        .await
        .map_err(|error| WindowsServiceError::Runtime(error.to_string()))?
}

#[cfg(all(windows, feature = "native-tray"))]
fn run_windows_tray_loop(app: TrayApp) -> Result<(), WindowsServiceError> {
    use tray_icon::{
        menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem},
        TrayIconBuilder,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        DispatchMessageW, PeekMessageW, TranslateMessage, MSG, PM_REMOVE, WM_QUIT,
    };

    let menu = Menu::new();
    let status = MenuItem::with_id(MenuId::new("status"), "Status", true, None);
    let open_web = MenuItem::with_id(MenuId::new("open_web"), "Open Web UI", true, None);
    let run_sync = MenuItem::with_id(MenuId::new("run_sync"), "Run sync", true, None);
    let stop_sync = MenuItem::with_id(MenuId::new("stop_sync"), "Stop sync", true, None);
    let open_logs = MenuItem::with_id(MenuId::new("open_logs"), "Open logs", true, None);
    let version = MenuItem::with_id(
        MenuId::new("version"),
        format!("ya-disk-sync {APP_VERSION}"),
        false,
        None,
    );
    let quit = MenuItem::with_id(MenuId::new("quit"), "Quit", true, None);
    menu.append(&status).map_err(menu_error)?;
    menu.append(&open_web).map_err(menu_error)?;
    menu.append(&run_sync).map_err(menu_error)?;
    menu.append(&stop_sync).map_err(menu_error)?;
    menu.append(&open_logs).map_err(menu_error)?;
    menu.append(&PredefinedMenuItem::separator())
        .map_err(menu_error)?;
    menu.append(&version).map_err(menu_error)?;
    menu.append(&quit).map_err(menu_error)?;

    let _tray_icon = TrayIconBuilder::new()
        .with_tooltip("ya-disk-sync")
        .with_menu(Box::new(menu))
        .with_icon(generated_icon()?)
        .build()
        .map_err(menu_error)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| WindowsServiceError::Runtime(error.to_string()))?;

    loop {
        unsafe {
            let mut message = std::mem::zeroed::<MSG>();
            while PeekMessageW(&mut message, std::ptr::null_mut(), 0, 0, PM_REMOVE) != 0 {
                if message.message == WM_QUIT {
                    return Ok(());
                }
                TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }
        while let Ok(event) = MenuEvent::receiver().try_recv() {
            let action = match event.id.0.as_str() {
                "status" => TrayAction::Status,
                "open_web" => TrayAction::OpenWebUi,
                "run_sync" => TrayAction::RunSync,
                "stop_sync" => TrayAction::StopSync,
                "open_logs" => TrayAction::OpenLogs,
                "quit" => TrayAction::Quit,
                _ => continue,
            };
            if action == TrayAction::Quit {
                return Ok(());
            }
            let _ = runtime.block_on(app.handle_action(action));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(all(windows, feature = "native-tray"))]
fn generated_icon() -> Result<tray_icon::Icon, WindowsServiceError> {
    let width = 32;
    let height = 32;
    let mut rgba = Vec::with_capacity(width * height * 4);
    for y in 0..height {
        for x in 0..width {
            let in_mark = (8..=23).contains(&x) && (8..=23).contains(&y);
            let diagonal = x >= y.saturating_sub(2) && x <= y + 2;
            let (r, g, b, a) = if in_mark && diagonal {
                (255, 255, 255, 255)
            } else if in_mark {
                (38, 112, 255, 255)
            } else {
                (0, 0, 0, 0)
            };
            rgba.extend([r, g, b, a]);
        }
    }
    tray_icon::Icon::from_rgba(rgba, width as u32, height as u32).map_err(menu_error)
}

#[cfg(all(windows, feature = "native-tray"))]
fn menu_error(error: impl std::fmt::Display) -> WindowsServiceError {
    WindowsServiceError::Runtime(error.to_string())
}

fn open_target(target: &str) -> Result<(), WindowsServiceError> {
    if std::env::var("YDS_NO_OPEN").ok().as_deref() == Some("1") {
        return Ok(());
    }
    if cfg!(windows) {
        Command::new("cmd")
            .args(["/C", "start", "", target])
            .spawn()?;
    } else if cfg!(target_os = "linux") {
        Command::new("xdg-open").arg(target).spawn()?;
    } else {
        return Err(WindowsServiceError::UnsupportedPlatform);
    }
    Ok(())
}

#[allow(dead_code)]
#[cfg(windows)]
fn windows_service_start_type_marker() -> windows_service::service::ServiceStartType {
    windows_service::service::ServiceStartType::AutoStart
}

#[allow(dead_code)]
fn _runtime_status_marker(status: RuntimeStatus) -> &'static str {
    status.as_str()
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
    fn install_config_builds_service_run_command() {
        let config = WindowsServiceInstallConfig {
            executable_path: PathBuf::from(r"C:\Program Files\ya-disk-sync\ya-disk-sync.exe"),
            config_path: PathBuf::from(r"C:\ProgramData\YaDiskSync\config\config.json"),
        };

        assert_eq!(
            config.service_run_arguments(),
            [
                "service",
                "run",
                "--config",
                r"C:\ProgramData\YaDiskSync\config\config.json"
            ]
        );
        assert!(config.bin_path().contains("service run --config"));
    }

    #[test]
    fn tray_menu_contains_required_actions() {
        assert_eq!(
            TrayApp::menu_labels(),
            [
                "Status",
                "Open Web UI",
                "Run sync",
                "Stop sync",
                "Open logs",
                "Version",
                "Quit"
            ]
        );
    }
}
