# SO3

SO3 — прототип распределённого объектного хранилища, реализованный в рамках магистерской диссертации.
В качестве протокола репликации используется Accord-подобный безлидерный консенсус.

Цель работы — показать, что безлидерный протокол обеспечивает предсказуемую (линейную)
деградацию при конфликтах и отказах, сопоставимую с лидерными системами (Raft), но без
единой точки отказа.

## Реализованные возможности

- Подмножество S3 API: `PUT`, `GET`, `HEAD`, `DELETE` по маршруту `/{bucket}/{*key}`.
- Приватный gRPC API для консенсуса и передачи blob-данных между узлами.
- Полная репликация: каждый узел хранит копию всех данных.
- Хранилище: метаданные и журнал консенсуса — в SQLite, содержимое объектов — в локальной файловой системе.
- Maelstrom-адаптер для проверки линеаризуемости через workload `lin-kv` и `g-set`.

## Структура workspace

| Пакет           | Назначение                                        |
|-----------------|---------------------------------------------------|
| `so3-core`      | Доменные типы, репозитории, S3/RPC API, консенсус |
| `so3`           | Бинарный файл узла                                |
| `so3-maelstrom` | Адаптер для Jepsen Maelstrom                      |

## Конфигурация

`so3` собирает конфигурацию из значений по умолчанию, TOML-файла и переменных окружения:

1. Значение по умолчанию, если не задано иное.
2. TOML-файл из `SO3_CONFIG` или `./so3.toml`.
3. Переменные окружения переопределяют TOML.

Пример (`so3.toml`):

```toml
node_id = "123e4567-e89b-12d3-a456-426614174000"
object_api_addr = "127.0.0.1:3000"
rpc_api_addr = "127.0.0.1:4000"
object_request_timeout_secs = 30
network_skew_ms = 50
consensus_rpc_deadline_ms = 3000
data_dir = "./var/so3"

[cluster]
peers = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa@127.0.0.1:4001",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb@127.0.0.1:4002",
]
```

| Переменная                        | TOML-поле                     | По умолчанию          |
|-----------------------------------|-------------------------------|-----------------------|
| `SO3_CONFIG`                      | путь к TOML-файлу             | `./so3.toml`          |
| `SO3_NODE_ID`                     | `node_id`                     | автогенерация         |
| `SO3_OBJECT_ADDR`                 | `object_api_addr`             | `127.0.0.1:3000`      |
| `SO3_RPC_ADDR`                    | `rpc_api_addr`                | `127.0.0.1:4000`      |
| `SO3_OBJECT_REQUEST_TIMEOUT_SECS` | `object_request_timeout_secs` | `30`                  |
| `SO3_NETWORK_SKEW_MS`             | `network_skew_ms`             | `50`                  |
| `SO3_CONSENSUS_RPC_DEADLINE_MS`   | `consensus_rpc_deadline_ms`   | `3000`                |
| `SO3_DATA_DIR`                    | `data_dir`                    | `./var/so3`           |
| `SO3_METADATA_DIR`                | `metadata_dir`                | `{data_dir}/metadata` |
| `SO3_BLOB_DIR`                    | `blob_dir`                    | `{data_dir}/blobs`    |
| `SO3_CLUSTER_PEERS`               | `cluster.peers`               | (пусто)               |

## Запуск

```bash
cargo run -p so3
```

```bash
curl -X PUT --data-binary 'hello' http://127.0.0.1:3000/demo/key
curl -i http://127.0.0.1:3000/demo/key
curl -I http://127.0.0.1:3000/demo/key
curl -X DELETE http://127.0.0.1:3000/demo/key
```

## Проверка

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -W clippy::pedantic
```

Для нагрузочных сценариев нужна release-сборка:

```bash
cargo build --release -p so3 -p so3-maelstrom
```

## Документация

- [Архитектура](docs/architecture.md) — структура, потоки запросов, консенсус.
- [Результаты экспериментов](docs/results.md) — сводные таблицы, доказательство тезиса.

## Лицензия

MIT / Apache-2.0 на выбор.
