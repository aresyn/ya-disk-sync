$ErrorActionPreference = "Stop"

if ($env:YDS_WINDOWS_SERVICE_TESTS -ne "1") {
    throw "Set YDS_WINDOWS_SERVICE_TESTS=1 to run the admin-gated Windows service smoke test."
}

if (-not $IsWindows) {
    throw "Windows service smoke test can run only on Windows."
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw "Windows service smoke test requires an elevated PowerShell session."
}

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
Set-Location -LiteralPath $repoRoot

$programData = Join-Path $env:ProgramData "YaDiskSync\service-smoke"
$configPath = Join-Path $programData "config.json"
$stateDb = Join-Path $programData "state\state.sqlite"
$logsDir = Join-Path $programData "logs"
$stagingDir = Join-Path $programData "staging"
[System.IO.Directory]::CreateDirectory($programData) | Out-Null

cargo build -p yds-cli

$existingStatusOutput = cargo run -p yds-cli -- service status 2>&1 | Out-String
if (($existingStatusOutput -notmatch "not_installed") -and ($env:YDS_WINDOWS_SERVICE_SMOKE_ALLOW_OVERWRITE -ne "1")) {
    throw "Service is already installed. Set YDS_WINDOWS_SERVICE_SMOKE_ALLOW_OVERWRITE=1 only for a disposable test installation."
}

cargo run -p yds-cli -- --config $configPath config init --force
cargo run -p yds-cli -- --config $configPath config set /schedule/enabled false
cargo run -p yds-cli -- --config $configPath config set /paths/state_db (ConvertTo-Json $stateDb)
cargo run -p yds-cli -- --config $configPath config set /paths/logs_dir (ConvertTo-Json $logsDir)
cargo run -p yds-cli -- --config $configPath config set /paths/staging_dir (ConvertTo-Json $stagingDir)
cargo run -p yds-cli -- --config $configPath config set /web_ui/port 0
cargo run -p yds-cli -- --config $configPath config validate

try {
    cargo run -p yds-cli -- --config $configPath service install --force
    cargo run -p yds-cli -- service start
    Start-Sleep -Seconds 5
    cargo run -p yds-cli -- service status
    cargo run -p yds-cli -- service stop
}
finally {
    try {
        cargo run -p yds-cli -- service uninstall
    }
    catch {
        Write-Warning "Failed to uninstall ya-disk-sync smoke service: $_"
    }
}

Write-Host "windows-service-smoke: ok"
