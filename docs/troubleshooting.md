# Troubleshooting

## Проверить конфиг

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json config validate
```

## Проверить авторизацию

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json auth status
```

Если служба работает под `LocalSystem`, токен должен быть доступен именно в keyring этого контекста.

## Посмотреть статус и логи

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json status
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json logs tail --lines 100
```

## Run partial_failed

`partial_failed` не всегда означает провал всего зеркала. Обычно это root или файл, который не удалось обработать из-за transient-сети, занятости файла, quota/auth ошибки или некорректного remote response. Смотрите failed/skipped items и operation journal.

## Неполный remote listing

Если listing root не завершился полностью, delete phase для root не запускается. Это защита от удаления по неполным данным. Повторный запуск обычно продолжает работу с сохранённым state.

## Занятые или исчезнувшие файлы

Если файл исчез или стал недоступен во время run, он записывается как skipped/failed item, а run продолжает остальные файлы. Если исчез целый подкаталог внутри root, планируется delete subtree вместо тысяч ошибок по descendants.
