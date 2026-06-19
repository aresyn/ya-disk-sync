param(
    [string]$InstallDirectory = "C:\Program Files\YaDiskSync",
    [string]$ProgramDataDirectory = "C:\ProgramData\YaDiskSync",
    [switch]$Force,
    [switch]$SkipConfigInit,
    [switch]$StartService
)

$ErrorActionPreference = "Stop"

function Require-Admin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script from an elevated PowerShell session."
    }
}

Require-Admin

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$sourceExe = Join-Path $scriptRoot "..\ya-disk-sync.exe"
if (-not (Test-Path -LiteralPath $sourceExe)) {
    $sourceExe = Join-Path $scriptRoot "ya-disk-sync.exe"
}
if (-not (Test-Path -LiteralPath $sourceExe)) {
    throw "ya-disk-sync.exe was not found near the installer script."
}

$configDirectory = Join-Path $ProgramDataDirectory "config"
$stateDirectory = Join-Path $ProgramDataDirectory "state"
$logsDirectory = Join-Path $ProgramDataDirectory "logs"
$stagingDirectory = Join-Path $ProgramDataDirectory "staging"
$configPath = Join-Path $configDirectory "config.json"
$targetExe = Join-Path $InstallDirectory "ya-disk-sync.exe"

[System.IO.Directory]::CreateDirectory($InstallDirectory) | Out-Null
[System.IO.Directory]::CreateDirectory($configDirectory) | Out-Null
[System.IO.Directory]::CreateDirectory($stateDirectory) | Out-Null
[System.IO.Directory]::CreateDirectory($logsDirectory) | Out-Null
[System.IO.Directory]::CreateDirectory($stagingDirectory) | Out-Null

Copy-Item -LiteralPath $sourceExe -Destination $targetExe -Force

if (-not $SkipConfigInit) {
    if (-not (Test-Path -LiteralPath $configPath)) {
        & $targetExe --config $configPath config init
    } elseif ($Force) {
        & $targetExe --config $configPath config init --force
    } else {
        Write-Host "config exists: $configPath"
    }
}

& $targetExe --config $configPath config validate
& $targetExe --config $configPath service install --force

if ($StartService) {
    & $targetExe service start
}

Write-Host "ya-disk-sync installed"
Write-Host "binary: $targetExe"
Write-Host "config: $configPath"
Write-Host "next: edit config roots, run auth login, then start service"
