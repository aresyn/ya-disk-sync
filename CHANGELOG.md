# Changelog

## 0.1.1 - 2026-06-19

- Added a Windows Inno Setup installer as the primary release artifact.
- Updated the public README to focus on tested real-world sync volumes and the one-click Windows install path.
- Kept the Windows portable zip as an advanced/manual option.

## 0.1.0 - 2026-06-19

- Scaffolded Rust workspace and CLI.
- Added JSON configuration, path mapping and exclusion engine.
- Added SQLite state repository, migrations and single-run lock.
- Added Yandex Disk client, OAuth/keyring auth and mock client.
- Added scanner, fingerprinting, SQLite backup staging and fixture generator.
- Added one-way sync engine and initial migration/adoption mode.
- Added foreground daemon runtime, logs, control API and server-rendered Web UI.
- Added Windows service helpers, tray boundary and Linux systemd helpers.
- Added streaming file upload path, live quota UI/API, release workflow, acceptance docs and live/release smoke scripts.
