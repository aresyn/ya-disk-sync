# Windows-служба и tray

Windows-служба запускает тот же runtime, что и `daemon`, но под Service Control Manager.

## Установка через EXE-инсталлятор

Основной Windows-сценарий: скачайте `ya-disk-sync-0.1.1-windows-x86_64-setup.exe` из GitHub Releases и запустите его от имени администратора.

Инсталлятор:

- копирует `ya-disk-sync.exe` в `C:\Program Files\YaDiskSync`;
- создаёт `C:\ProgramData\YaDiskSync\config`, `state`, `logs`, `staging`;
- создаёт `config.json`, только если его ещё нет;
- проверяет конфиг через `config validate`;
- регистрирует службу через установленный exe;
- по умолчанию запускает службу после установки;
- создаёт пункты меню «Пуск» для Web UI, статуса, логов, папки конфигурации и удаления.

Удаление через стандартный uninstaller останавливает и удаляет службу, но не удаляет `C:\ProgramData\YaDiskSync`. Конфиг, state и логи остаются на месте для повторной установки или обновления.

Silent-установка использует стандартные ключи Inno Setup:

```powershell
.\ya-disk-sync-0.1.1-windows-x86_64-setup.exe /SILENT /NORESTART
```

Для полностью тихой установки:

```powershell
.\ya-disk-sync-0.1.1-windows-x86_64-setup.exe /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /LOG=install.log
```

Путь установки можно изменить через `/DIR=...`. Существующие `config.json`, SQLite-state и логи не перезаписываются.

## Portable-вариант

Если нужен ручной сценарий, скачайте `ya-disk-sync-0.1.1-windows-x86_64-portable.zip`, распакуйте архив и откройте PowerShell от имени администратора:

```powershell
.\scripts\install-windows-service.ps1
```

Скрипт копирует бинарник в `C:\Program Files\YaDiskSync`, создаёт каталоги в `C:\ProgramData\YaDiskSync` и вызывает `service install --force`.

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
