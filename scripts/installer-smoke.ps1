param(
    [string]$SetupPath,
    [string]$InstallDirectory,
    [string]$DataDirectory,
    [switch]$AllowServiceMutation,
    [switch]$ForceExistingService
)

$ErrorActionPreference = "Stop"

if (-not ($IsWindows -or $env:OS -eq "Windows_NT")) {
    Write-Host "installer-smoke: skipped"
    Write-Host "reason: Windows-only test"
    exit 0
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdministrator)) {
    Write-Host "installer-smoke: skipped"
    Write-Host "reason: administrator privileges are required"
    exit 0
}

if (-not $AllowServiceMutation) {
    Write-Host "installer-smoke: skipped"
    Write-Host "reason: pass -AllowServiceMutation to install/uninstall the ya-disk-sync service"
    exit 0
}

$version = (Get-Content -LiteralPath "Cargo.toml" -Raw -Encoding UTF8 |
    Select-String -Pattern 'version = "([^"]+)"').Matches[0].Groups[1].Value

if ([string]::IsNullOrWhiteSpace($SetupPath)) {
    $SetupPath = "dist\ya-disk-sync-$version-windows-x86_64-setup.exe"
}

if (-not (Test-Path -LiteralPath $SetupPath)) {
    throw "setup.exe not found: $SetupPath"
}

$testRoot = Join-Path $env:TEMP "YaDiskSyncInstallerSmoke"
if ([string]::IsNullOrWhiteSpace($InstallDirectory)) {
    $InstallDirectory = Join-Path $testRoot "ProgramFiles\YaDiskSync"
}
if ([string]::IsNullOrWhiteSpace($DataDirectory)) {
    $DataDirectory = Join-Path $testRoot "ProgramData\YaDiskSync"
}

function Assert-UnderTestRoot {
    param([string]$Path)
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($testRoot)
    if (-not $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to modify path outside test root: $fullPath"
    }
}

function Remove-TestPath {
    param([string]$Path)
    Assert-UnderTestRoot -Path $Path
    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
}

$existingService = Get-Service -Name "ya-disk-sync" -ErrorAction SilentlyContinue
if ($existingService -and -not $ForceExistingService) {
    Write-Host "installer-smoke: skipped"
    Write-Host "reason: service ya-disk-sync already exists; pass -ForceExistingService only on a disposable test machine"
    exit 0
}

Remove-TestPath -Path $InstallDirectory
Remove-TestPath -Path $DataDirectory
[System.IO.Directory]::CreateDirectory($testRoot) | Out-Null

$installLog = Join-Path $testRoot "install.log"
$setupArgs = @(
    "/VERYSILENT",
    "/SUPPRESSMSGBOXES",
    "/NORESTART",
    "/DIR=$InstallDirectory",
    "/YDSDATA=$DataDirectory",
    "/TASKS=",
    "/LOG=$installLog"
)

& $SetupPath @setupArgs
if ($LASTEXITCODE -ne 0) {
    throw "installer exited with code $LASTEXITCODE"
}

$installedExe = Join-Path $InstallDirectory "ya-disk-sync.exe"
if (-not (Test-Path -LiteralPath $installedExe)) {
    throw "installed exe not found: $installedExe"
}

$installedVersion = (& $installedExe --version).Trim()
if (-not $installedVersion.Contains($version)) {
    throw "unexpected installed version: $installedVersion"
}

$configPath = Join-Path $DataDirectory "config\config.json"
if (-not (Test-Path -LiteralPath $configPath)) {
    throw "config was not created: $configPath"
}

$service = Get-CimInstance Win32_Service -Filter "Name='ya-disk-sync'" -ErrorAction Stop
if (-not $service.PathName.Contains($InstallDirectory)) {
    throw "service does not use test install dir: $($service.PathName)"
}

$uninstaller = Join-Path $InstallDirectory "unins000.exe"
if (-not (Test-Path -LiteralPath $uninstaller)) {
    throw "uninstaller not found: $uninstaller"
}

& $uninstaller "/VERYSILENT" "/SUPPRESSMSGBOXES" "/NORESTART"
if ($LASTEXITCODE -ne 0) {
    throw "uninstaller exited with code $LASTEXITCODE"
}

$serviceAfterUninstall = Get-Service -Name "ya-disk-sync" -ErrorAction SilentlyContinue
if ($serviceAfterUninstall) {
    throw "service still exists after uninstall"
}

if (-not (Test-Path -LiteralPath $configPath)) {
    throw "ProgramData config was not preserved after uninstall"
}

Remove-TestPath -Path $testRoot

Write-Host "installer-smoke: ok"
Write-Host "setup: $SetupPath"
Write-Host "version: $installedVersion"
