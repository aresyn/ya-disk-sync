# Клиент Яндекс Диска и OAuth

Проект использует собственный Rust-клиент Яндекс Диска. Рабочие операции не вызывают внешний CLI.

## OAuth

Поддерживается authorization-code flow с PKCE S256. CLI печатает URL авторизации, пользователь вставляет code вручную.

```powershell
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json auth login --client-id "<client-id>"
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json auth status
ya-disk-sync --config C:\ProgramData\YaDiskSync\config\config.json auth logout
```

`oauth_client_id` не является секретом. Access/refresh tokens являются секретами и хранятся в OS keyring под `account_alias`.

## Операции API

Клиент поддерживает:

- disk info и quota;
- metadata;
- recursive listing с paging;
- mkdir;
- upload через upload link;
- download через download link;
- move/copy;
- permanent delete;
- polling async-операций.

## Retry

Retry применяется к timeout/reset, HTTP `429` и `5xx`, а также transient body/json decode ошибкам. `401/403` классифицируются как auth error, quota errors останавливают run. Ответы и токены не пишутся в логи.
