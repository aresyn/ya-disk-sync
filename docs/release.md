# Релизы

Релизы публикуются через GitHub Actions по тегу `v*`.

```powershell
git tag v0.1.1
git push origin v0.1.1
```

Workflow собирает:

- Windows `setup.exe` через Inno Setup 6. Это основной артефакт для пользователей;
- Windows portable zip с `ya-disk-sync.exe`, `README.md`, `LICENSE`, `docs/`, `assets/`, `scripts/install-windows-service.ps1` для ручной установки и автоматизации;
- Linux tar.gz с бинарником, `README.md`, `LICENSE`, `docs/`, `assets/`;
- `SHA256SUMS`.

В архивы не должны попадать config, state, logs, keyring dumps, `.env`, приватные ключи и локальные production-заметки.

Локальная проверка упаковки и portable-архива:

```powershell
.\scripts\release-smoke.ps1
```

Если на машине установлен Inno Setup 6, `release-smoke.ps1` также соберёт `setup.exe`. Чтобы считать отсутствие Inno ошибкой, используйте:

```powershell
.\scripts\release-smoke.ps1 -BuildInstaller
```

Smoke-тест самого инсталлятора регистрирует и удаляет службу `ya-disk-sync`, поэтому запускайте его только от администратора на чистой тестовой машине или CI-runner:

```powershell
.\scripts\installer-smoke.ps1 -AllowServiceMutation
```

Инсталлятор должен сохранять пользовательские данные: при установке и удалении не удаляются `C:\ProgramData\YaDiskSync\config`, `state`, `logs` и `staging`.
