param(
    [string]$OutputDirectory = "dist\local-smoke"
)

$ErrorActionPreference = "Stop"

cargo build --release -p yds-cli

$version = (cargo run -q -p yds-cli -- --version).Trim()
$doctor = cargo run -q -p yds-cli -- doctor
if ($LASTEXITCODE -ne 0) {
    throw "doctor command failed"
}
if (-not ($doctor -join "`n").Contains("status: ok")) {
    throw "doctor output did not report status: ok"
}

if (Test-Path -LiteralPath $OutputDirectory) {
    Remove-Item -LiteralPath $OutputDirectory -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

if ($IsWindows -or $env:OS -eq "Windows_NT") {
    Copy-Item -LiteralPath "target\release\ya-disk-sync.exe" -Destination "$OutputDirectory\ya-disk-sync.exe"
} else {
    Copy-Item -LiteralPath "target/release/ya-disk-sync" -Destination "$OutputDirectory/ya-disk-sync"
}
Copy-Item -LiteralPath "README.md" -Destination $OutputDirectory
Copy-Item -LiteralPath "LICENSE" -Destination $OutputDirectory
Copy-Item -LiteralPath "docs" -Destination "$OutputDirectory\docs" -Recurse
Copy-Item -LiteralPath "assets" -Destination "$OutputDirectory\assets" -Recurse
New-Item -ItemType Directory -Force -Path "$OutputDirectory\scripts" | Out-Null
Copy-Item -LiteralPath "scripts\install-windows-service.ps1" -Destination "$OutputDirectory\scripts\install-windows-service.ps1"

Write-Host "release-smoke: ok"
Write-Host "version: $version"
Write-Host "output: $OutputDirectory"
