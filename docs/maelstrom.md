# Maelstrom

`so3-maelstrom` - отдельный бинарный пакет для запусков Jepsen Maelstrom со сценариями `lin-kv` и `g-set`.
Он повторно использует код объектов и консенсуса из `so3-core`, но заменяет tonic-транспорт между
узлами на JSON-сообщения через stdin/stdout Maelstrom.

В новой исследовательской рамке Maelstrom используется как smoke-проверка safety/correctness для
части протокола. Он не является performance benchmark и не закрывает весь proof of concept: для
доказательства применимости Accord к object storage дополнительно нужны production-node сценарии с
несколькими entrypoint'ами, отказами, восстановлением и hot-key конфликтами.

## Предварительные требования

- Java доступна в `PATH`.
- Исполняемый jar файл Maelstrom доступен в `PATH`, через `MAELSTROM_JAR` или через явный аргумент
  скрипта `--maelstrom-jar` / `--maelstrom-bin`.
- [uv](https://docs.astral.sh/uv/) установлен (для `uv run`).

## Установка

```bash
uv run python scripts/maelstrom/install.py
```

Установщик скачивает официальный релиз `jepsen-io/maelstrom` в `.tools/maelstrom/maelstrom`.

## Запуски

Все скрипты — кросс-платформенные Python.

Smoke-тест на одном узле:

```bash
uv run python scripts/maelstrom/smoke.py
```

Smoke-тест на трех узлах:

```bash
uv run python scripts/maelstrom/smoke_3node.py
```

Общий запуск с гибкими параметрами:

```bash
# lin-kv (read/write/cas, линейная согласованность)
uv run python scripts/maelstrom/run.py --workload lin-kv --node-count 3 --time-limit 30 --rate 10

# g-set (add/read, eventual inclusion)
uv run python scripts/maelstrom/run.py --workload g-set --node-count 3 --time-limit 30 --rate 10
```

Трехузловой lin-kv с partition nemesis:

```bash
uv run python scripts/maelstrom/fault_3node.py
```

Трехузловой g-set с partition nemesis:

```bash
uv run python scripts/maelstrom/set_3node.py
```

30 прогонов lin-kv с агрегацией pass/fail:

```bash
uv run python scripts/maelstrom/run_30.py
```

Значения по умолчанию для `run.py`:

| Параметр        | Значение |
|-----------------|----------|
| `--workload`    | `lin-kv` |
| `--node-count`  | `1`      |
| `--time-limit`  | `20`     |
| `--rate`        | `10`     |
| `--concurrency` | `2n`     |
| `--nemesis`     | (пусто)  |

Значения по умолчанию для `run_30.py`:

| Параметр        | Значение |
|-----------------|----------|
| `--runs`        | `30`     |
| `--node-count`  | `3`      |
| `--time-limit`  | `30`     |
| `--rate`        | `10`     |
| `--concurrency` | `2n`     |
| `--nemesis`     | (пусто)  |

Общие скрипты пробрасывают дополнительные настройки Maelstrom, включая `--nemesis`,
`--nemesis-interval`, `--latency`, `--latency-dist`, `--availability`, `--consistency-models`,
`--log-net-send` и `--log-net-recv`.

## Модель выполнения

Maelstrom запускает каждый узел отдельным процессом и передает исходный список узлов в сообщении
`init`. Адаптер строит изолированный стек `so3-core` для каждого Maelstrom-узла:

- SQLite-метаданные и журнал консенсуса находятся в `metadata/<node_id>`;
- blob-файлы находятся в `blobs/<node_id>`;
- `AccordConsensusCoordinatorService` координирует команды этого узла;
- `InboundConsensusUseCaseImpl` обрабатывает входящие сообщения консенсуса;
- Maelstrom-клиенты узлов кодируют запросы ядра в JSON payloads.

Каждый Maelstrom-узел обрабатывает поступающие клиентские запросы локально, координируя операцию
через собственный Accord-координатор. Это соответствует поведению production-узла `so3`, где любой
узел может координировать запросы, пришедшие на его S3-подобный API.

## Модель консистентности

### lin-kv (линейная согласованность)

lin-kv проверяет линейную согласованность (linearizability) через read/write/cas операции.
3-узловой кластер проходит проверку как без nemesis, так и с partition nemesis при `rate=10` —
это подтверждает корректность базового консенсуса и quorum reads.

При высоких rate (>10) возможны stale reads из-за асинхронного apply на репликах:
replica возвращает CommitResponse до завершения apply, и следующий read на этой реплике
может увидеть устаревшие metadata. Это ожидаемое поведение, сопоставимое с Raft follower reads.

### g-set (eventual inclusion)

g-set проверяет что элементы, добавленные через успешные `add` операции, никогда не теряются
(eventual set inclusion). Эта модель слабее линейной согласованности и соответствует гарантии
quorum reads: данные не теряются, но чтение может вернуть устаревшее состояние во время партиций.
3-узловой кластер проходит g-set с partition nemesis.

## Текущие ограничения

Адаптер полезен для smoke-проверки семантики команд через истории Maelstrom, но пока не достигает
полного соответствия production runtime:

- `cas` с `create_if_not_exists=true` выполняет скоординированное чтение, затем запись, поэтому две
  конкурентные create-операции могут обе вернуть `cas_ok`;
- blob push/fetch использует один JSON payload и не проверяет размер или SHA-256 так, как production
  tonic `BlobService`;
- ожидающие consensus, blob и metadata-query-запросы ждут oneshot-ответы без дедлайнов операций.

Результаты Maelstrom следует использовать как smoke-покрытие протокола, а не как полное доказательство
поведения production-узлов. Полный набор требуемых PoC-проверок описан в [research-plan.md](research-plan.md).

## Последняя проверка

Последняя локальная проверка Maelstrom: 2026-05-23 на macOS, M4 Pro, release-сборка.

| Сценарий         | Workload | Узлы | Rate | Nemesis        | Результат      |
|------------------|----------|------|------|----------------|----------------|
| `smoke.py`       | lin-kv   | 1    | 10   | none           | `:valid? true` |
| `smoke_3node.py` | lin-kv   | 3    | 10   | none           | `:valid? true` |
| `fault_3node.py` | lin-kv   | 3    | 10   | `partition/5s` | `:valid? true` |
| `set_3node.py`   | g-set    | 3    | 10   | none           | `:valid? true` |
| `set_3node.py`   | g-set    | 3    | 10   | `partition/5s` | `:valid? true` |

lin-kv при rate>10 даёт stale reads из-за асинхронного apply на репликах — см. «Модель консистентности».

## Платформенные заметки

- macOS/Linux: Python-скрипты запускаются через `uv run` или напрямую.
- Maelstrom пишет подробные истории в `store/lin-kv/` или `store/g-set/`; эти директории игнорируются git.
- Helper-скрипты создают свежую временную `SO3_MAELSTROM_DATA_DIR`, если она не задана явно.
