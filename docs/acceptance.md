# Acceptance checklist

Перед публичным релизом проверьте:

- `cargo fmt --all -- --check` проходит;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` проходит;
- `cargo test --workspace --all-features` проходит;
- `cargo build --release -p yds-cli` собирает бинарник;
- `scripts/release-smoke.ps1` создаёт архив без config/state/logs/secrets;
- README рендерит изображения из `assets/readme/`;
- `config init` создаёт обезличенный пример, а не production-конфиг;
- `rg` не находит персональные пути, токены и реальные аккаунты в публичном наборе файлов.

Для live-проверок используйте только отдельный sandbox root и отдельный remote sandbox. Production-каталоги не должны участвовать в тестах.
