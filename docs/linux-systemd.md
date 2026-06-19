# Linux systemd

Linux-режим использует тот же бинарник и тот же daemon runtime.

Пример установки:

```bash
sudo install -m 0755 ya-disk-sync /usr/local/bin/ya-disk-sync
sudo ya-disk-sync --config /etc/ya-disk-sync/config.json config init
sudo ya-disk-sync --config /etc/ya-disk-sync/config.json service install --systemd --force
sudo ya-disk-sync service start
```

Generated unit запускает:

```text
/usr/local/bin/ya-disk-sync daemon --config /etc/ya-disk-sync/config.json
```

Unit использует `Restart=on-failure`, `StateDirectory` и `LogsDirectory`. Для production рекомендуется отдельный пользователь `ya-disk-sync` и права только на нужные локальные roots.

Команды:

```bash
sudo ya-disk-sync service status
sudo ya-disk-sync service restart
sudo ya-disk-sync service stop
sudo ya-disk-sync service uninstall
```
