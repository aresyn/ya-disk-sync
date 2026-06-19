# Тестирование

Обычные проверки:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release -p yds-cli
```

## Fixture generator

```powershell
ya-disk-sync test-fixtures generate-tree --files 10000 --max-depth 20 --output C:\Temp\yds-tree
```

Generator создаёт детерминированную структуру и не пишет в существующий непустой каталог.

## Live tests

Реальные тесты Яндекс Диска должны быть gated переменными окружения и запускаться только в отдельном sandbox-каталоге. Production roots не должны использоваться в тестах.

## Fault injection

В тестах есть сценарии для transient-сетевых ошибок, занятых файлов, исчезновения файла во время чтения, partial remote inventory, idempotent delete `NotFound` и cancellation.
