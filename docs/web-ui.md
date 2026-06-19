# Web UI

Web UI встроен в бинарник. Node, SPA-сборка и отдельный web stack не нужны.

Открыть интерфейс:

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json web open
```

Страницы:

- Dashboard;
- Sync roots;
- Exclusions;
- Schedule;
- Config editor;
- State/last runs;
- Logs;
- Failed/Skipped;
- Remote quota.

Run now и Stop вызывают тот же daemon API, что и CLI. Config editor сохраняет только валидный pretty JSON и показывает, что для применения изменений нужен restart.

## Auth boundary

`127.0.0.1` доступен без токена. Если bind не loopback, нужен Bearer token. Токен хранится в keyring и не записывается в JSON.

```powershell
ya-disk-sync web token status
ya-disk-sync web token rotate
```
