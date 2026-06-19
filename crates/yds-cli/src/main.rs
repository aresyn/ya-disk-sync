use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use serde_json::Value;
use yds_core::{
    config::{
        default_config, load_config, resolve_config_path, save_config, validate_config, AppConfig,
        ConfigValidationReport,
    },
    ComponentStatus, DiagnosticReport,
};
use yds_service::{logging, ControlClient, RuntimeHost, RuntimeHostOptions};
use yds_state::models::SyncRunRecord;
use yds_web::WebTokenStore;
use yds_windows::{TrayApp, TrayRuntime, WindowsServiceInstallConfig, WindowsServiceManager};
use yds_yandex_disk::auth::{
    import_yacli_auth, resolve_oauth_client_id, token_status, KeyringTokenStore, OAuthFlow,
    TokenStore, YDS_OAUTH_CLIENT_ID_ENV,
};
use yds_yandex_disk::{HttpYandexDiskClient, RetryPolicy};

#[derive(Debug, Parser)]
#[command(
    name = "ya-disk-sync",
    version,
    about = "One-way local filesystem to Yandex Disk mirror"
)]
struct Cli {
    /// Path to config JSON. Falls back to YDS_CONFIG_PATH and then the platform default.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check the compiled application component boundaries.
    Doctor,
    /// Manage JSON configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    /// Manage Yandex Disk authorization.
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
    /// Run one foreground sync.
    Sync {
        #[command(subcommand)]
        command: SyncCommands,
    },
    /// Run foreground daemon runtime.
    Daemon,
    /// Show daemon status or latest SQLite run when daemon is offline.
    Status,
    /// Inspect runtime logs.
    Logs {
        #[command(subcommand)]
        command: LogCommands,
    },
    /// Open or administer the local Web UI.
    Web {
        #[command(subcommand)]
        command: WebCommands,
    },
    /// Install, control or run the OS service wrapper.
    Service {
        #[command(subcommand)]
        command: ServiceCommands,
    },
    /// Run the Windows tray helper process.
    Tray,
    /// Run and inspect initial legacy -> canonical migration.
    Migration {
        #[command(subcommand)]
        command: MigrationCommands,
    },
    /// Developer test fixture utilities.
    TestFixtures {
        #[command(subcommand)]
        command: TestFixtureCommands,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommands {
    /// Create a default JSON configuration file.
    Init {
        /// Overwrite an existing config file.
        #[arg(long)]
        force: bool,
    },
    /// Validate the JSON configuration file.
    Validate,
    /// Print normalized pretty JSON configuration.
    Show,
    /// Set an existing JSON pointer to a JSON value.
    Set {
        /// JSON Pointer path, for example /logging/level.
        json_pointer: String,
        /// JSON value, for example "debug", true or 2.
        json_value: String,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommands {
    /// Start manual Yandex OAuth login flow.
    Login {
        /// OAuth application client id. Overrides config and YDS_OAUTH_CLIENT_ID.
        #[arg(long)]
        client_id: Option<String>,
    },
    /// Show authorization status without printing tokens.
    Status,
    /// Remove stored Yandex Disk credentials from the OS keyring.
    Logout,
    /// Try to import an existing yacli authorization if a compatible safe format is available.
    ImportYacli,
}

#[derive(Debug, Subcommand)]
enum SyncCommands {
    /// Execute one foreground local -> Yandex Disk sync run.
    Run {
        /// Ignore cached remote inventory state and force a fresh Yandex Disk listing.
        #[arg(long)]
        force_remote_rescan: bool,
    },
    /// Request cancellation of the daemon-managed sync run.
    Stop,
}

#[derive(Debug, Subcommand)]
enum LogCommands {
    /// Print last lines from the newest log file.
    Tail {
        /// Number of lines to print.
        #[arg(long, default_value_t = 50)]
        lines: usize,
    },
}

#[derive(Debug, Subcommand)]
enum WebCommands {
    /// Open the configured local Web UI URL.
    Open,
    /// Manage the non-loopback Web UI bearer token.
    Token {
        #[command(subcommand)]
        command: WebTokenCommands,
    },
}

#[derive(Debug, Subcommand)]
enum WebTokenCommands {
    /// Show whether a Web UI bearer token is configured.
    Status,
    /// Rotate the Web UI bearer token and print it once.
    Rotate,
}

#[derive(Debug, Subcommand)]
enum ServiceCommands {
    /// Install the OS service wrapper.
    Install {
        /// Install Linux systemd unit instead of native Windows service.
        #[arg(long)]
        systemd: bool,
        /// Replace existing service/unit where supported.
        #[arg(long)]
        force: bool,
    },
    /// Start the installed OS service.
    Start,
    /// Stop the installed OS service.
    Stop,
    /// Restart the installed OS service.
    Restart,
    /// Show installed OS service status.
    Status,
    /// Uninstall the OS service wrapper.
    Uninstall,
    /// Best-effort service update using the current binary/config path.
    Update {
        /// Update Linux systemd unit instead of native Windows service.
        #[arg(long)]
        systemd: bool,
        /// Replace existing service/unit where supported.
        #[arg(long)]
        force: bool,
    },
    /// Internal SCM entrypoint.
    #[command(hide = true)]
    Run,
}

#[derive(Debug, Subcommand)]
enum MigrationCommands {
    /// Execute one foreground initial migration/adoption run.
    Run {
        /// Ignore cached remote inventory state and force a fresh Yandex Disk listing.
        #[arg(long)]
        force_remote_rescan: bool,
    },
    /// Print stored migration map status from SQLite state.
    Status,
}

#[derive(Debug, Subcommand)]
enum TestFixtureCommands {
    /// Generate a deterministic synthetic file tree.
    GenerateTree {
        /// Number of files to create.
        #[arg(long)]
        files: usize,
        /// Maximum directory depth.
        #[arg(long)]
        max_depth: usize,
        /// Output directory. It must not be an existing non-empty directory.
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if !matches!(
        cli.command,
        Some(Commands::Daemon)
            | Some(Commands::Service {
                command: ServiceCommands::Run
            })
    ) {
        init_tracing();
    }
    match cli.command {
        Some(Commands::Doctor) => print_doctor_report()?,
        Some(Commands::Config { command }) => handle_config_command(command, cli.config)?,
        Some(Commands::Auth { command }) => handle_auth_command(command, cli.config).await?,
        Some(Commands::Sync { command }) => handle_sync_command(command, cli.config).await?,
        Some(Commands::Daemon) => daemon(cli.config).await?,
        Some(Commands::Status) => runtime_status(cli.config).await?,
        Some(Commands::Logs { command }) => handle_logs_command(command, cli.config)?,
        Some(Commands::Web { command }) => handle_web_command(command, cli.config).await?,
        Some(Commands::Service { command }) => handle_service_command(command, cli.config).await?,
        Some(Commands::Tray) => tray(cli.config).await?,
        Some(Commands::Migration { command }) => {
            handle_migration_command(command, cli.config).await?
        }
        Some(Commands::TestFixtures { command }) => handle_test_fixture_command(command)?,
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }

    Ok(())
}

fn handle_test_fixture_command(command: TestFixtureCommands) -> Result<()> {
    match command {
        TestFixtureCommands::GenerateTree {
            files,
            max_depth,
            output,
        } => {
            yds_scanner::generate_fixture_tree(&output, files, max_depth)?;
            println!("test-fixtures: generated");
            println!("files: {files}");
            println!("max_depth: {max_depth}");
            println!("output: {}", output.display());
            Ok(())
        }
    }
}

async fn handle_sync_command(command: SyncCommands, config_path: Option<PathBuf>) -> Result<()> {
    match command {
        SyncCommands::Run {
            force_remote_rescan,
        } => sync_run(config_path, force_remote_rescan).await,
        SyncCommands::Stop => sync_stop(config_path).await,
    }
}

fn handle_logs_command(command: LogCommands, config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;

    match command {
        LogCommands::Tail { lines } => {
            let lines = logging::tail_latest_log(&config.paths.logs_dir, lines)?;
            for line in lines {
                println!("{line}");
            }
            Ok(())
        }
    }
}

async fn handle_web_command(command: WebCommands, config_path: Option<PathBuf>) -> Result<()> {
    match command {
        WebCommands::Open => {
            let config = load_valid_config(config_path)?;
            let url = format!(
                "http://{}:{}",
                config.web_ui.bind_address, config.web_ui.port
            );
            println!("web: {url}");
            open_target(&url)?;
            Ok(())
        }
        WebCommands::Token { command } => handle_web_token_command(command),
    }
}

fn handle_web_token_command(command: WebTokenCommands) -> Result<()> {
    let store = WebTokenStore::default_store()?;
    match command {
        WebTokenCommands::Status => {
            let status = store.token_status()?;
            println!(
                "web_token: {}",
                if status.configured {
                    "configured"
                } else {
                    "not_configured"
                }
            );
            Ok(())
        }
        WebTokenCommands::Rotate => {
            let token = store.rotate_token()?;
            println!("web_token: rotated");
            println!("bearer_token: {token}");
            Ok(())
        }
    }
}

async fn handle_service_command(
    command: ServiceCommands,
    config_path: Option<PathBuf>,
) -> Result<()> {
    match command {
        ServiceCommands::Install { systemd, force } => {
            if systemd {
                let manager = yds_linux::SystemdServiceManager::default();
                let unit = yds_linux::SystemdUnit::default();
                manager.install(&unit, force)?;
                println!("service: installed systemd unit");
                return Ok(());
            }
            let config_path = resolve_config_path(config_path);
            let executable_path = std::env::current_exe()?;
            let manager = WindowsServiceManager::new();
            manager.install(
                &WindowsServiceInstallConfig {
                    executable_path,
                    config_path,
                },
                force,
            )?;
            println!("service: installed");
            Ok(())
        }
        ServiceCommands::Start => {
            if cfg!(target_os = "linux") {
                yds_linux::SystemdServiceManager::default().start()?;
            } else {
                WindowsServiceManager::new().start()?;
            }
            println!("service: started");
            Ok(())
        }
        ServiceCommands::Stop => {
            if cfg!(target_os = "linux") {
                yds_linux::SystemdServiceManager::default().stop()?;
            } else {
                WindowsServiceManager::new().stop()?;
            }
            println!("service: stopped");
            Ok(())
        }
        ServiceCommands::Restart => {
            if cfg!(target_os = "linux") {
                yds_linux::SystemdServiceManager::default().restart()?;
            } else {
                WindowsServiceManager::new().restart()?;
            }
            println!("service: restarted");
            Ok(())
        }
        ServiceCommands::Status => {
            if cfg!(target_os = "linux") {
                let status = yds_linux::SystemdServiceManager::default().status()?;
                println!("service: {status}");
            } else {
                let status = WindowsServiceManager::new().status()?;
                println!("service: {}", status.as_str());
            }
            Ok(())
        }
        ServiceCommands::Uninstall => {
            if cfg!(target_os = "linux") {
                yds_linux::SystemdServiceManager::default().uninstall()?;
            } else {
                WindowsServiceManager::new().uninstall()?;
            }
            println!("service: uninstalled");
            Ok(())
        }
        ServiceCommands::Update { systemd, force } => {
            if systemd || cfg!(target_os = "linux") {
                let manager = yds_linux::SystemdServiceManager::default();
                let unit = yds_linux::SystemdUnit::default();
                manager.update(&unit, force)?;
                println!("service: updated systemd unit");
                return Ok(());
            }
            let config_path = resolve_config_path(config_path);
            let executable_path = std::env::current_exe()?;
            WindowsServiceManager::new().update(
                &WindowsServiceInstallConfig {
                    executable_path,
                    config_path,
                },
                force,
            )?;
            println!("service: updated");
            Ok(())
        }
        ServiceCommands::Run => service_run(config_path).await,
    }
}

async fn tray(config_path: Option<PathBuf>) -> Result<()> {
    let config = load_valid_config(config_path)?;
    let app = TrayApp::new(
        config.web_ui.bind_address.clone(),
        config.web_ui.port,
        config.paths.logs_dir.clone(),
    );

    println!("tray: running");
    println!("menu: {}", TrayApp::menu_labels().join(", "));
    TrayRuntime::new(app).run().await?;
    Ok(())
}

async fn daemon(config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);
    let host = RuntimeHost::start(RuntimeHostOptions::new(path.clone()))
        .await
        .with_context(|| {
            format!(
                "failed to start daemon from config {}; run config init first",
                path.display()
            )
        })?;
    let local_addr = host.local_addr();
    println!("daemon: running");
    println!("control: http://{local_addr}");

    tokio::signal::ctrl_c().await?;
    host.shutdown().await?;
    println!("daemon: stopped");
    Ok(())
}

async fn service_run(config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);
    if cfg!(windows) && std::env::var("YDS_WINDOWS_SERVICE_CONSOLE").ok().as_deref() != Some("1") {
        tokio::task::spawn_blocking(move || yds_windows::WindowsServiceRunner::run(path)).await??;
        return Ok(());
    }
    daemon(Some(path)).await
}

async fn handle_migration_command(
    command: MigrationCommands,
    config_path: Option<PathBuf>,
) -> Result<()> {
    match command {
        MigrationCommands::Run {
            force_remote_rescan,
        } => migration_run(config_path, force_remote_rescan).await,
        MigrationCommands::Status => migration_status(config_path),
    }
}

async fn sync_stop(config_path: Option<PathBuf>) -> Result<()> {
    let config = load_valid_config(config_path)?;
    let client = control_client(&config);
    match client.request_stop().await {
        Ok(response) => {
            println!("sync stop: {}", response.message);
            println!("status: {}", response.status.as_str());
            if response.accepted {
                Ok(())
            } else {
                bail!("daemon has no running sync")
            }
        }
        Err(error) => {
            bail!("daemon not running or control API unavailable: {error}")
        }
    }
}

async fn runtime_status(config_path: Option<PathBuf>) -> Result<()> {
    let config = load_valid_config(config_path)?;
    let client = control_client(&config);
    match client.status().await {
        Ok(snapshot) => {
            println!("daemon: online");
            println!("status: {}", snapshot.status.as_str());
            println!("uptime_seconds: {}", snapshot.uptime_seconds);
            if let Some(current) = snapshot.current_run {
                println!("current_run: {}", current.trigger);
                println!("current_started_at_utc: {}", current.started_at_utc);
            }
            if let Some(latest) = snapshot.latest_run {
                print_run_record(&latest);
            }
            if let Some(error) = snapshot.latest_error {
                println!("latest_error: {error}");
            }
            Ok(())
        }
        Err(error) => {
            println!("daemon: offline");
            println!("control_error: {error}");
            let repository = yds_state::StateRepository::open(&config.paths.state_db)?;
            if let Some(run) = repository.get_latest_run()? {
                print_state_run_record(&run);
            } else {
                println!("latest_run: none");
            }
            Ok(())
        }
    }
}

async fn sync_run(config_path: Option<PathBuf>, force_remote_rescan: bool) -> Result<()> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;

    let token_store: Arc<dyn TokenStore> = Arc::new(KeyringTokenStore::default_store()?);
    let status = token_status(token_store.as_ref(), &config.yandex_disk.account_alias)?;
    if !status.authenticated {
        bail!(
            "auth: unauthenticated for account_alias {}; run auth login first",
            config.yandex_disk.account_alias
        );
    }

    let repository = yds_state::StateRepository::open(&config.paths.state_db)?;
    let client = HttpYandexDiskClient::new(
        config.yandex_disk.account_alias.clone(),
        token_store,
        RetryPolicy::from_config(&config.sync),
    )?;
    let engine = yds_sync::SyncEngine::new(&config, &repository, Arc::new(client));
    let report = engine
        .run_once(
            yds_sync::SyncRunOptions {
                force_remote_rescan,
                ..yds_sync::SyncRunOptions::default()
            },
            &yds_sync::CancellationToken::new(),
        )
        .await?;

    println!("sync: {}", report.status.as_str());
    println!("run_id: {}", report.run_id);
    println!("scanned_files: {}", report.summary.scanned_files);
    println!("uploaded_files: {}", report.summary.uploaded_files);
    println!("updated_files: {}", report.summary.updated_files);
    println!("deleted_files: {}", report.summary.deleted_files);
    println!("skipped_files: {}", report.summary.skipped_files);
    println!("failed_files: {}", report.summary.failed_files);
    println!("bytes_uploaded: {}", report.summary.bytes_uploaded);
    if let Some(error_summary) = &report.summary.error_summary {
        println!("error_summary: {error_summary}");
    }

    if !report.is_successful() {
        bail!("sync run finished with status {}", report.status.as_str());
    }

    Ok(())
}

fn load_valid_config(config_path: Option<PathBuf>) -> Result<AppConfig> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;
    Ok(config)
}

fn control_client(config: &AppConfig) -> ControlClient {
    ControlClient::new(&config.web_ui.bind_address, config.web_ui.port)
}

fn open_target(target: &str) -> Result<()> {
    if std::env::var("YDS_NO_OPEN").ok().as_deref() == Some("1") {
        return Ok(());
    }
    if cfg!(windows) {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", target])
            .spawn()
            .with_context(|| format!("failed to open {target}"))?;
    } else if cfg!(target_os = "linux") {
        std::process::Command::new("xdg-open")
            .arg(target)
            .spawn()
            .with_context(|| format!("failed to open {target}"))?;
    } else {
        bail!("opening targets is unsupported on this platform");
    }
    Ok(())
}

fn print_run_record(run: &yds_service::RunSnapshot) {
    println!("latest_run_id: {}", run.id);
    println!("latest_run_status: {}", run.status);
    println!("latest_run_trigger: {}", run.trigger);
    println!("latest_run_started_at_utc: {}", run.started_at_utc);
    if let Some(finished) = &run.finished_at_utc {
        println!("latest_run_finished_at_utc: {finished}");
    }
    println!("scanned_files: {}", run.summary.scanned_files);
    println!("uploaded_files: {}", run.summary.uploaded_files);
    println!("updated_files: {}", run.summary.updated_files);
    println!("deleted_files: {}", run.summary.deleted_files);
    println!("skipped_files: {}", run.summary.skipped_files);
    println!("failed_files: {}", run.summary.failed_files);
    println!("bytes_uploaded: {}", run.summary.bytes_uploaded);
}

fn print_state_run_record(run: &SyncRunRecord) {
    println!("latest_run_id: {}", run.id);
    println!("latest_run_status: {}", run.status.as_str());
    println!("latest_run_trigger: {}", run.trigger.as_str());
    println!("latest_run_started_at_utc: {}", run.started_at_utc);
    if let Some(finished) = &run.finished_at_utc {
        println!("latest_run_finished_at_utc: {finished}");
    }
    println!("scanned_files: {}", run.summary.scanned_files);
    println!("uploaded_files: {}", run.summary.uploaded_files);
    println!("updated_files: {}", run.summary.updated_files);
    println!("deleted_files: {}", run.summary.deleted_files);
    println!("skipped_files: {}", run.summary.skipped_files);
    println!("failed_files: {}", run.summary.failed_files);
    println!("bytes_uploaded: {}", run.summary.bytes_uploaded);
}

async fn migration_run(config_path: Option<PathBuf>, force_remote_rescan: bool) -> Result<()> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;

    let token_store: Arc<dyn TokenStore> = Arc::new(KeyringTokenStore::default_store()?);
    let status = token_status(token_store.as_ref(), &config.yandex_disk.account_alias)?;
    if !status.authenticated {
        bail!(
            "auth: unauthenticated for account_alias {}; run auth login first",
            config.yandex_disk.account_alias
        );
    }

    let repository = yds_state::StateRepository::open(&config.paths.state_db)?;
    let client = HttpYandexDiskClient::new(
        config.yandex_disk.account_alias.clone(),
        token_store,
        RetryPolicy::from_config(&config.sync),
    )?;
    let engine = yds_sync::migration::MigrationEngine::new(&config, &repository, Arc::new(client));
    let report = engine
        .run_once(
            yds_sync::migration::MigrationRunOptions {
                force_remote_rescan,
                ..yds_sync::migration::MigrationRunOptions::default()
            },
            &yds_sync::CancellationToken::new(),
        )
        .await?;

    println!("migration: {}", report.status.as_str());
    println!("run_id: {}", report.run_id);
    println!("scanned_files: {}", report.summary.scanned_files);
    println!("uploaded_files: {}", report.summary.uploaded_files);
    println!("updated_files: {}", report.summary.updated_files);
    println!("deleted_files: {}", report.summary.deleted_files);
    println!("skipped_files: {}", report.summary.skipped_files);
    println!("failed_files: {}", report.summary.failed_files);
    println!("bytes_uploaded: {}", report.summary.bytes_uploaded);
    let adopted_files: i64 = report.roots.iter().map(|root| root.adopted_files).sum();
    let moved_resources: i64 = report.roots.iter().map(|root| root.moved_resources).sum();
    println!("adopted_files: {adopted_files}");
    println!("moved_resources: {moved_resources}");
    if let Some(error_summary) = &report.summary.error_summary {
        println!("error_summary: {error_summary}");
    }

    if !report.is_successful() {
        bail!(
            "migration run finished with status {}",
            report.status.as_str()
        );
    }

    Ok(())
}

fn migration_status(config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;
    let repository = yds_state::StateRepository::open(&config.paths.state_db)?;
    let records = repository.list_migration_map(None)?;

    if records.is_empty() {
        println!("migration: no records");
        println!("state_db: {}", config.paths.state_db);
        return Ok(());
    }

    println!("migration: records");
    println!("state_db: {}", config.paths.state_db);
    for record in records {
        println!(
            "{}\t{}\t{}\t{}{}",
            record.root_id,
            record.status.as_str(),
            record.legacy_remote_path,
            record.canonical_remote_path,
            record
                .last_error
                .as_ref()
                .map(|error| format!("\t{error}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

async fn handle_auth_command(command: AuthCommands, config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);
    let config = load_config(&path).with_context(|| {
        format!(
            "failed to load config {}; run config init first",
            path.display()
        )
    })?;
    ensure_valid_config(&config)?;

    match command {
        AuthCommands::Login { client_id } => auth_login(&config, client_id.as_deref()).await,
        AuthCommands::Status => auth_status(&config),
        AuthCommands::Logout => auth_logout(&config),
        AuthCommands::ImportYacli => auth_import_yacli(&config),
    }
}

async fn auth_login(config: &AppConfig, client_id: Option<&str>) -> Result<()> {
    let client_id = resolve_oauth_client_id(&config.yandex_disk, client_id).ok_or_else(|| {
        anyhow!(
            "missing OAuth client id; pass --client-id, set yandex_disk.oauth_client_id, or set {YDS_OAUTH_CLIENT_ID_ENV}"
        )
    })?;
    let flow = OAuthFlow::new(
        client_id,
        config.yandex_disk.oauth_scope.clone(),
        config.yandex_disk.client_name.clone(),
    )?;
    let request = flow.authorization_request()?;

    println!("Open this URL and grant Yandex Disk access:");
    println!("{}", request.authorization_url);
    print!("Paste confirmation code: ");
    io::stdout().flush()?;

    let mut code = String::new();
    io::stdin().read_line(&mut code)?;
    let code = code.trim();
    if code.is_empty() {
        bail!("confirmation code must not be empty");
    }

    let token = flow.exchange_code(code, &request.pkce_verifier).await?;
    let store = KeyringTokenStore::default_store()?;
    store.save_token(&config.yandex_disk.account_alias, &token)?;

    println!("auth: logged in");
    println!("account_alias: {}", config.yandex_disk.account_alias);
    Ok(())
}

fn auth_status(config: &AppConfig) -> Result<()> {
    let store = KeyringTokenStore::default_store()?;
    let status = token_status(&store, &config.yandex_disk.account_alias)?;

    println!(
        "auth: {}",
        if status.authenticated {
            "authenticated"
        } else {
            "unauthenticated"
        }
    );
    println!("account_alias: {}", status.account_alias);
    if let Some(expires_at_unix) = status.expires_at_unix {
        println!("expires_at_unix: {expires_at_unix}");
    }
    if let Some(scope) = status.scope {
        println!("scope: {scope}");
    }
    println!("has_refresh_token: {}", status.has_refresh_token);
    Ok(())
}

fn auth_logout(config: &AppConfig) -> Result<()> {
    let store = KeyringTokenStore::default_store()?;
    let deleted = store.delete_token(&config.yandex_disk.account_alias)?;

    println!(
        "auth: {}",
        if deleted {
            "logged out"
        } else {
            "not authenticated"
        }
    );
    println!("account_alias: {}", config.yandex_disk.account_alias);
    Ok(())
}

fn auth_import_yacli(config: &AppConfig) -> Result<()> {
    let store = KeyringTokenStore::default_store()?;
    import_yacli_auth(&store, &config.yandex_disk.account_alias)?;
    println!("auth: imported from yacli");
    println!("account_alias: {}", config.yandex_disk.account_alias);
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .try_init();
}

fn print_doctor_report() -> Result<()> {
    let report = DiagnosticReport::new(component_statuses());
    tracing::debug!(status = report.status().as_str(), "doctor completed");

    let _json_report = serde_json::to_string(&report)?;

    println!("{} {}", report.app_name(), report.version());
    println!("status: {}", report.status().as_str());
    println!("components:");

    for component in report.components() {
        println!(
            "- {}: {} ({})",
            component.name(),
            component.health().as_str(),
            component.details()
        );
    }

    Ok(())
}

fn handle_config_command(command: ConfigCommands, config_path: Option<PathBuf>) -> Result<()> {
    let path = resolve_config_path(config_path);

    match command {
        ConfigCommands::Init { force } => init_config(path, force),
        ConfigCommands::Validate => {
            let config = load_config(&path)?;
            ensure_valid_config(&config)?;
            println!("config: ok");
            println!("path: {}", path.display());
            Ok(())
        }
        ConfigCommands::Show => {
            let config = load_config(&path)?;
            ensure_valid_config(&config)?;
            println!("{}", serde_json::to_string_pretty(&config)?);
            Ok(())
        }
        ConfigCommands::Set {
            json_pointer,
            json_value,
        } => set_config_value(path, &json_pointer, &json_value),
    }
}

fn init_config(path: PathBuf, force: bool) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "config already exists at {}; pass --force to overwrite",
            path.display()
        );
    }

    let config = default_config();
    ensure_valid_config(&config)?;
    save_config(&path, &config)?;
    println!("config: initialized");
    println!("path: {}", path.display());
    Ok(())
}

fn set_config_value(path: PathBuf, json_pointer: &str, json_value: &str) -> Result<()> {
    let content = fs::read_to_string(&path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let mut value: Value = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    let replacement: Value = serde_json::from_str(json_value)
        .with_context(|| format!("failed to parse JSON value {json_value}"))?;

    let target = value
        .pointer_mut(json_pointer)
        .ok_or_else(|| anyhow!("json pointer not found: {json_pointer}"))?;
    *target = replacement;

    let config: AppConfig =
        serde_json::from_value(value).context("updated JSON does not match config schema")?;
    ensure_valid_config(&config)?;
    save_config(&path, &config)?;

    println!("config: updated");
    println!("path: {}", path.display());
    println!("set: {json_pointer}");
    Ok(())
}

fn ensure_valid_config(config: &AppConfig) -> Result<()> {
    let report = validate_config(config);
    if report.is_valid() {
        return Ok(());
    }

    print_validation_errors(&report);
    bail!("config validation failed")
}

fn print_validation_errors(report: &ConfigValidationReport) {
    println!("config: invalid");
    for error in report.errors() {
        let mut location = error.field.clone();
        if let Some(root_index) = error.root_index {
            location.push_str(&format!(" root_index={root_index}"));
        }
        if let Some(rule_index) = error.rule_index {
            location.push_str(&format!(" rule_index={rule_index}"));
        }
        println!("- {location}: {}", error.message);
    }
}

fn component_statuses() -> Vec<ComponentStatus> {
    vec![
        yds_core::component_status(),
        yds_state::component_status(),
        yds_yandex_disk::component_status(),
        yds_scanner::component_status(),
        yds_sync::component_status(),
        yds_service::component_status(),
        yds_web::component_status(),
        yds_windows::component_status(),
        yds_linux::component_status(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_component_order_is_deterministic() {
        let names: Vec<_> = component_statuses()
            .iter()
            .map(ComponentStatus::name)
            .collect();

        assert_eq!(
            names,
            [
                "core",
                "state",
                "yandex-disk",
                "scanner",
                "sync",
                "service",
                "web",
                "windows",
                "linux",
            ]
        );
    }
}
