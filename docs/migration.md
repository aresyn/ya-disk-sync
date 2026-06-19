# Первичная миграция

Migration/adoption нужен, когда каталог уже есть и локально, и в Яндекс Диске. Вместо слепой повторной загрузки инструмент сравнивает стороны, принимает совпадающие файлы и записывает baseline в SQLite.

Команда:

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json migration run --force-remote-rescan
```

`--force-remote-rescan` полезен для первого запуска root: так state получает полный свежий remote inventory.

## Что делает migration

1. Сканирует local root с учётом исключений.
2. Получает remote inventory canonical path и legacy paths.
3. Переносит legacy layout в canonical layout, если это безопасно.
4. Сравнивает локальные и удалённые файлы.
5. Совпадающие файлы принимает без upload.
6. Отличающиеся или отсутствующие в облаке файлы загружает.
7. Лишнее в управляемом remote root удаляет в финальной фазе.
8. Записывает file/directory state, remote inventory и журнал операций.

## Идемпотентность

Повторный `migration run` после успешной миграции должен стать no-op или обычным небольшим incremental run. Следующий `sync run` использует тот же state.

## Безопасность удаления

Удаления выполняются только после полного remote inventory. Если listing root завершился transient-ошибкой, root помечается degraded/partial, а delete phase для него запрещается.
