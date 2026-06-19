# Конфигурация

`ya-disk-sync` использует один JSON-файл. Секреты OAuth в него не записываются: токены хранятся в системном keyring.

Путь к конфигу выбирается в таком порядке:

1. `--config <path>`
2. переменная окружения `YDS_CONFIG_PATH`
3. путь по умолчанию:
   - Windows: `C:\ProgramData\YaDiskSync\config\config.json`
   - Linux: `/etc/ya-disk-sync/config.json`

## Команды

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json config init
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json config validate
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json config show
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json config set /logging/level '"debug"'
```

`config init` создаёт полный pretty JSON и не перезаписывает существующий файл без `--force`. `config set` принимает JSON Pointer и JSON-значение, затем валидирует весь конфиг. Если итоговый файл невалиден, запись не выполняется.

## Основные разделы

- `version` — версия схемы, сейчас `1`.
- `instance` — имя инстанса и общий удалённый корень.
- `paths` — SQLite-state, логи и staging-каталог.
- `schedule` — ежедневное расписание UTC.
- `sync` — лимиты размеров, параллелизм, retry и политика удаления.
- `yandex_disk` — account alias и не секретный OAuth client id.
- `web_ui` — локальный bind, порт и auth boundary.
- `logging` — уровень логов и retention.
- `global_excludes` — правила исключений для всех roots.
- `absolute_excludes` — абсолютные пути, которые всегда исключаются.
- `roots` — каталоги синхронизации.

## Root

Пример:

```json
{
  "id": "projects",
  "name": "Рабочие проекты",
  "enabled": true,
  "local_path": "C:\\Data\\Projects",
  "remote_path_override": "disk:/Backup/Projects",
  "legacy_remote_paths": [],
  "excludes": ["**/target/**", "**/.venv/**"]
}
```

`id` должен быть стабильным: по нему state связывает локальные файлы, удалённый inventory и историю операций. `remote_path_override` лучше задавать явно, если удалённый каталог уже существует.

## Sync-настройки

Значения по умолчанию:

- `max_file_size_bytes`: `53687091200` (50 GiB)
- `full_hash_max_bytes`: `10485760` (10 MiB)
- `large_file_name_size_only_min_bytes`: `104857600` (100 MiB)
- `scan_concurrency`: `0` (авто)
- `upload_concurrency`: `2`
- `create_directory_concurrency`: `8`
- `retry_attempts`: `3`
- `retry_delay_seconds`: `30`
- `force_remote_rescan`: `false`
- `remote_inventory_cache_ttl_hours`: `168`

Крупные файлы `>= 100 MiB` дополнительно ограничены одним одновременным upload, чтобы они не забивали всю очередь.

## Исключения

Правила похожи на `.gitignore`:

```text
**/target/**
**/node_modules/**
**/.venv/**
!important/node_modules/keep.txt
```

Порядок такой:

1. сначала `global_excludes`;
2. затем `roots[].excludes`;
3. последнее совпадение побеждает;
4. `absolute_excludes` всегда имеют максимальный приоритет.

По умолчанию исключаются временные файлы, SQLite WAL/SHM и системный мусор вроде `Thumbs.db`. `.git`, `.env`, `node_modules`, `target` и ключи не исключаются молча: это зеркало, поэтому такие решения должны быть явными.

## Path mapping

Windows:

```text
C:\Data\Projects -> disk:/Backup/C/Data/Projects
```

Linux:

```text
/var/www/site -> disk:/Backup/var/www/site
```

UNC-пути и drive-relative пути вроде `D:folder` в схеме v1 невалидны.
