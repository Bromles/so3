# Maelstrom

`so3-maelstrom` - отдельный бинарный пакет для запусков Jepsen Maelstrom со сценарием `lin-kv`.
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

Общий запуск `lin-kv` с гибкими параметрами:

```bash
uv run python scripts/maelstrom/run.py --node-count 3 --time-limit 30 --rate 100 --concurrency 2n
```

Трехузловой запуск с partition nemesis:

```bash
uv run python scripts/maelstrom/fault_3node.py
```

30 прогонов с агрегацией pass/fail:

```bash
uv run python scripts/maelstrom/run_30.py --node-count 3 --rate 100 --nemesis partition --nemesis-interval 5
```

Значения по умолчанию для fault-wrapper:

| Параметр           | Значение    |
|--------------------|-------------|
| `NODE_COUNT`       | `3`         |
| `TIME_LIMIT`       | `30`        |
| `RATE`             | `20`        |
| `CONCURRENCY`      | `2n`        |
| `NEMESIS`          | `partition` |
| `NEMESIS_INTERVAL` | `5`         |

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

Последняя локальная проверка Maelstrom: 2026-05-22 на macOS, M4 Pro, debug-сборка.

3-узловой запуск с partition nemesis (`fault_3node.py`, `run_30.py` с `--allow-low-runs --runs 1`)
показал нарушения линеаризуемости на ключах 2 и 3 (Knossos `:valid? false`):
stale reads и дублированные значения после network partition. Это указывает на сохраняющуюся проблему
с visibility guarantees при partition recovery.

Ранее (2026-05-15) на release-сборке были зафиксированы успешные результаты:

| Сценарий              | Узлы | Rate | Concurrency | Nemesis        | Ops |  Ok | Fail | Info | Результат      |
|-----------------------|------|------|-------------|----------------|----:|----:|-----:|-----:|----------------|
| `smoke.py`            | 1    | 20   | `2n`        | none           | 197 | 144 |   53 |    0 | `:valid? true` |
| `smoke_3node.py`      | 3    | 10   | `2n`        | none           |  95 |  67 |   28 |    0 | `:valid? true` |
| `fault_3node.py`      | 3    | 20   | `2n`        | `partition/5s` | 238 |  53 |  108 |   77 | `:valid? true` |

## Платформенные заметки

- macOS/Linux: Python-скрипты запускаются через `uv run` или напрямую.
- Maelstrom пишет подробные истории в `store/lin-kv/`; эта директория игнорируется git.
- Helper-скрипты создают свежую временную `SO3_MAELSTROM_DATA_DIR`, если она не задана явно.
