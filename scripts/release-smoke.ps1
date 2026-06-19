param(
    [string]$OutputDirectory = "dist\local-smoke",
    [switch]$BuildInstaller
)

$ErrorActionPreference = "Stop"

cargo build --release -p yds-cli

$version = (cargo run -q -p yds-cli -- --version).Trim()
$packageVersion = (Get-Content -LiteralPath "Cargo.toml" -Raw -Encoding UTF8 |
    Select-String -Pattern 'version = "([^"]+)"').Matches[0].Groups[1].Value
$doctor = cargo run -q -p yds-cli -- doctor
if ($LASTEXITCODE -ne 0) {
    throw "doctor command failed"
}
if (-not ($doctor -join "`n").Contains("status: ok")) {
    throw "doctor output did not report status: ok"
}

$distDirectory = Split-Path -Path $OutputDirectory -Parent
if ([string]::IsNullOrWhiteSpace($distDirectory)) {
    $distDirectory = "."
}
[System.IO.Directory]::CreateDirectory($distDirectory) | Out-Null

if (Test-Path -LiteralPath $OutputDirectory) {
    Remove-Item -LiteralPath $OutputDirectory -Recurse -Force
}
[System.IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null

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

$portableArchive = $null
$setupPath = $null
if ($IsWindows -or $env:OS -eq "Windows_NT") {
    $artifact = "ya-disk-sync-$packageVersion-windows-x86_64"
    $portableArchive = Join-Path $distDirectory "$artifact-portable.zip"
    if (Test-Path -LiteralPath $portableArchive) {
        Remove-Item -LiteralPath $portableArchive -Force
    }
    Compress-Archive -Path "$OutputDirectory\*" -DestinationPath $portableArchive -Force

    $iscc = Join-Path ${env:ProgramFiles(x86)} "Inno Setup 6\ISCC.exe"
    if (-not (Test-Path -LiteralPath $iscc)) {
        $command = Get-Command ISCC.exe -ErrorAction SilentlyContinue
        if ($command) {
            $iscc = $command.Source
        }
    }

    if (Test-Path -LiteralPath $iscc) {
        $setupPath = Join-Path $distDirectory "$artifact-setup.exe"
        if (Test-Path -LiteralPath $setupPath) {
            Remove-Item -LiteralPath $setupPath -Force
        }
        $payloadPath = (Resolve-Path -LiteralPath $OutputDirectory).Path
        $distPath = (Resolve-Path -LiteralPath $distDirectory).Path
        $generatedIss = Join-Path $distDirectory "ya-disk-sync-$packageVersion.generated.iss"
        $issTemplate = Get-Content -LiteralPath "packaging\windows\ya-disk-sync.iss" -Raw -Encoding UTF8
        $issTemplate.Replace('#define AppVersion "0.1.1"', "#define AppVersion `"$packageVersion`"") |
            Set-Content -LiteralPath $generatedIss -Encoding UTF8
        & $iscc $generatedIss "/DPayloadDir=$payloadPath" "/DOutputDir=$distPath"
        if ($LASTEXITCODE -ne 0) {
            throw "Inno Setup build failed"
        }
    } elseif ($BuildInstaller) {
        throw "Inno Setup 6 ISCC.exe was not found"
    } else {
        Write-Warning "Inno Setup 6 was not found; setup.exe build skipped"
    }
}

Write-Host "release-smoke: ok"
Write-Host "version: $version"
Write-Host "output: $OutputDirectory"
if ($portableArchive) {
    Write-Host "portable: $portableArchive"
}
if ($setupPath) {
    Write-Host "setup: $setupPath"
}
