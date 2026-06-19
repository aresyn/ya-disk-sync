# Разработка

Workspace состоит из crate-ов:

- `yds-cli` — CLI и application boundary;
- `yds-core` — конфиг, path mapping, diagnostics, exclusions;
- `yds-state` — SQLite repository;
- `yds-yandex-disk` — OAuth и REST-клиент;
- `yds-scanner` — local inventory и fingerprinting;
- `yds-sync` — planner и sync/migration engine;
- `yds-service` — daemon/runtime/control API;
- `yds-web` — server-rendered Web UI;
- `yds-windows` — Windows service/tray helpers;
- `yds-linux` — systemd helpers.

Перед изменениями полезно прочитать `specifications.md`, `PLAN.MD` и историю итераций. Эти файлы относятся к внутренней разработке и не обязательны для публичного релиза.

Проверки перед PR/релизом:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
.\scripts\release-smoke.ps1
```

Секреты, реальные configs, state DB и logs нельзя добавлять в git.
