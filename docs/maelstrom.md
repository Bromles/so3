# Maelstrom

`so3-maelstrom` - отдельный бинарный пакет для запусков Jepsen Maelstrom со сценарием `lin-kv`.
Он повторно использует код объектов и консенсуса из `so3-core`, но заменяет tonic-транспорт между
узлами на JSON-сообщения через stdin/stdout Maelstrom.

## Предварительные требования

- Java доступна в `PATH`.
- Исполняемый jar файл Maelstrom доступен в `PATH`, через `MAELSTROM_JAR` или через явный аргумент
  скрипта `-MaelstromJar` / `-MaelstromBin`.

## Установка

```bash
./scripts/maelstrom/install-maelstrom.sh
```

```powershell
./scripts/maelstrom/install-maelstrom.ps1
```

Установщик скачивает официальный релиз `jepsen-io/maelstrom` в `.tools/maelstrom/maelstrom`.

## Запуски

Smoke-тест на одном узле:

```bash
./scripts/maelstrom/smoke-lin-kv.sh
```

```powershell
./scripts/maelstrom/smoke-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

Smoke-тест на трех узлах:

```bash
./scripts/maelstrom/smoke-3-node-lin-kv.sh
```

```powershell
./scripts/maelstrom/smoke-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

Общий запуск `lin-kv`:

```bash
./scripts/maelstrom/run-lin-kv.sh
```

```powershell
./scripts/maelstrom/run-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

Трехузловой запуск с partition nemesis:

```bash
./scripts/maelstrom/fault-3-node-lin-kv.sh
```

```powershell
./scripts/maelstrom/fault-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
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

Общие скрипты пробрасывают дополнительные настройки Maelstrom, включая `NEMESIS`,
`NEMESIS_INTERVAL`, `LATENCY`, `LATENCY_DIST`, `AVAILABILITY`, `CONSISTENCY_MODELS`,
`LOG_NET_SEND` и `LOG_NET_RECV`.

## Модель выполнения

Maelstrom запускает каждый узел отдельным процессом и передает исходный список узлов в сообщении
`init`. Адаптер строит изолированный стек `so3-core` для каждого Maelstrom-узла:

- SQLite-метаданные и журнал консенсуса находятся в `metadata/<node_id>`;
- blob-файлы находятся в `blobs/<node_id>`;
- `AccordConsensusCoordinatorService` координирует команды этого узла;
- `InboundConsensusUseCaseImpl` обрабатывает входящие сообщения консенсуса;
- Maelstrom-клиенты узлов кодируют запросы ядра в JSON payloads.

Маршрутизация клиентских запросов намеренно отличается от production-узла:

- `node_ids.first()` считается координатором;
- клиентские запросы, доставленные followers, пересылаются этому координатору;
- координатору исполняет операцию ядра и возвращает ответ через пересылающий узел.

В production-бинаре `so3` такого слоя пересылки координатору нет: любой узел может координировать
запросы, пришедшие через его S3-подобный API.

## Текущие ограничения

Адаптер полезен для smoke-проверки семантики команд через истории Maelstrom, но пока не достигает
полного соответствия production runtime:

- он скрывает конкурентных production-координаторов, потому что все клиентские команды Maelstrom
  пересылаются одному координатору;
- `cas` с `create_if_not_exists=true` выполняет скоординированное чтение, затем запись, поэтому две
  конкурентные create-операции могут обе вернуть `cas_ok`;
- blob push/fetch использует один JSON payload и не проверяет размер или SHA-256 так, как production
  tonic `BlobService`;
- ожидающие consensus, blob и forward-запросы ждут oneshot-ответы без дедлайнов операций.

Результаты Maelstrom следует использовать как smoke-покрытие протокола, а не как полное доказательство
поведения production-узлов.

## Последняя проверка

Локальные запуски от 2026-05-05 с `target/release/so3-maelstrom` прошли Knossos (`:valid? true`)
для следующих сценариев:

| Сценарий              | Узлы | Rate | Concurrency | Nemesis        | Ops |  Ok | Fail | Info | Результат      |
|-----------------------|------|------|-------------|----------------|----:|----:|-----:|-----:|----------------|
| `smoke-lin-kv`        | 1    | 20   | `2n`        | none           | 197 | 144 |   53 |    0 | `:valid? true` |
| `smoke-3-node-lin-kv` | 3    | 10   | `2n`        | none           |  95 |  67 |   28 |    0 | `:valid? true` |
| `fault-3-node-lin-kv` | 3    | 20   | `2n`        | `partition/5s` | 238 |  53 |  108 |   77 | `:valid? true` |

Полные счетчики и ограничения интерпретации находятся в [results.md](results.md).

## Платформенные заметки

- Windows: используйте `*.ps1` под PowerShell 7.
- macOS/Linux: используйте `*.sh` под bash/zsh.
- WSL: предпочтительно собирать Linux-бинарь `so3-maelstrom` внутри WSL.
- Maelstrom пишет подробные истории в `store/lin-kv/`; эта директория игнорируется git.
- Helper-скрипты создают свежую временную `SO3_MAELSTROM_DATA_DIR`, если она не задана явно.
