# Runtime и observability

`yds-service` запускает foreground daemon и тот же runtime, который используется Windows-службой и systemd.

## Возможности runtime

- ежедневный UTC scheduler;
- ручной запуск sync;
- cancel активного запуска;
- защита от overlap;
- статус текущей фазы;
- HTTP endpoints для health/status/metrics;
- текстовые логи с retention.

## Команды

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json daemon
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json status
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json sync stop
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json logs tail --lines 100
```

## HTTP API

По умолчанию API слушает `127.0.0.1:17691`.

- `GET /health`
- `GET /metrics`
- `GET /status`
- `POST /sync/run`
- `POST /sync/stop`

Loopback-bind работает без bearer token. Non-loopback bind требует web token из keyring.

## Логи

Логи пишутся в `paths.logs_dir`. В них есть start/end run, trigger, root summaries, counters, ошибки и cancellation. OAuth-токены, refresh tokens и authorization codes в логи не выводятся.
