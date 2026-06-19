# Релизы

Релизы публикуются через GitHub Actions по тегу `v*`.

```powershell
git tag v0.1.0
git push origin v0.1.0
```

Workflow собирает:

- Windows zip с `ya-disk-sync.exe`, `README.md`, `LICENSE`, `docs/`, `assets/`, `scripts/install-windows-service.ps1`;
- Linux tar.gz с бинарником, `README.md`, `LICENSE`, `docs/`, `assets/`;
- `SHA256SUMS`.

В архивы не должны попадать config, state, logs, keyring dumps, `.env`, приватные ключи и локальные production-заметки.

Локальная проверка упаковки:

```powershell
.\scripts\release-smoke.ps1
```
