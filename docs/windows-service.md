# Windows-служба и tray

Windows-служба запускает тот же runtime, что и `daemon`, но под Service Control Manager.

## Установка через релизный архив

Откройте PowerShell от имени администратора в распакованном архиве:

```powershell
.\scripts\install-windows-service.ps1
```

Скрипт копирует бинарник в `C:\Program Files\YaDiskSync`, создаёт каталоги в `C:\ProgramData\YaDiskSync`, и вызывает `service install --force`.

## Ручные команды

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json service install --force
ya-disk-sync service start
ya-disk-sync service status
ya-disk-sync service stop
ya-disk-sync service restart
ya-disk-sync service uninstall
```

Служба устанавливается с automatic start и работает под `LocalSystem`. Если OAuth был выполнен под обычным пользователем, токен нужно отдельно завести в keyring контекста службы.

## Tray

Tray-процесс запускается в пользовательской сессии и общается со службой через local control API. Сама служба tray не запускает.

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json tray
```

Tray нужен только для удобства: открыть Web UI, увидеть статус, запустить sync, остановить sync, открыть логи.
