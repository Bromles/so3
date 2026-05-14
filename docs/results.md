# Результаты проверок

Последняя локальная проверка: 2026-05-05 на macOS, один физический хост.

## Команды

Сборка:

```bash
cargo build --release -p so3 -p so3-maelstrom
```

Maelstrom:

```bash
BINARY_PATH=target/release/so3-maelstrom NO_BUILD=1 bash scripts/maelstrom/smoke-lin-kv.sh
BINARY_PATH=target/release/so3-maelstrom NO_BUILD=1 bash scripts/maelstrom/smoke-3-node-lin-kv.sh
BINARY_PATH=target/release/so3-maelstrom NO_BUILD=1 bash scripts/maelstrom/fault-3-node-lin-kv.sh
```

k6 S3 benchmark:

```bash
SO3_OBJECT_ADDR=127.0.0.1:3301 SO3_RPC_ADDR=127.0.0.1:4301 SO3_DATA_DIR=/tmp/so3-k6-release-30 target/release/so3
SO3_ADDR=http://127.0.0.1:3301 bash scripts/k6/run-benchmark.sh --runs 30 --outdir /tmp/so3-k6-release-30-json
```

Maelstrom и k6 localhost-запуски выполнялись вне sandbox. Попытки запуска внутри sandbox
завершались ошибками прав для локальных процессов/сети.

## Maelstrom

Бинарный файл: `target/release/so3-maelstrom`.
Workload: `lin-kv`.
Checker: strict-serializable / Knossos linearizability.

| Сценарий | Узлы | Время | Rate | Concurrency | Nemesis        | Ops |  Ok | Fail | Info | ok-fraction | Valid |
|----------|------|-------|------|-------------|----------------|----:|----:|-----:|-----:|------------:|-------|
| Smoke    | 1    | 10 s  | 20   | `2n`        | none           | 197 | 144 |   53 |    0 |       0.731 | true  |
| Smoke    | 3    | 10 s  | 10   | `2n`        | none           |  95 |  67 |   28 |    0 |       0.705 | true  |
| Fault    | 3    | 30 s  | 20   | `2n`        | `partition/5s` | 238 |  53 |  108 |   77 |       0.223 | true  |

Файлы результатов:

| Сценарий     | Путь к результату                                   |
|--------------|-----------------------------------------------------|
| Smoke 1-node | `store/lin-kv/20260505T231122.384+0300/results.edn` |
| Smoke 3-node | `store/lin-kv/20260505T231151.117+0300/results.edn` |
| Fault 3-node | `store/lin-kv/20260505T231219.289+0300/results.edn` |

Интерпретация:

- все три запуска вернули `:valid? true`;
- `fail` ожидаем для чтений отсутствующих ключей/CAS и CAS precondition failures при конкуренции;
- partition-запуск включает nemesis-операции типа `info`;
- это результаты Maelstrom-адаптера; адаптер все еще пересылает клиентские команды на
  `node_ids.first()`, поэтому не проверяет production-входные точки с несколькими координаторами.

## k6 S3 benchmark

Бинарный файл: `target/release/so3`.
Конфигурация: один узел, `SO3_ADDR=http://127.0.0.1:3301`, 10 VU, 30 запусков по 30 секунд,
объекты по 64 байта. Каждая итерация выполняет `PUT -> GET -> HEAD -> DELETE` через S3-подобный API.

Raw exports: `/tmp/so3-k6-release-30-json/run_*.json`.
Resource samples: `/tmp/so3-k6-release-30-json/resources.tsv`.

Пропускная способность:

| Метрика         |  n |    mean |   sigma | variance |    CV |     min |      max |
|-----------------|---:|--------:|--------:|---------:|------:|--------:|---------:|
| S3 requests/s   | 30 | 27.3134 | 24.4967 | 600.0859 | 89.7% | 12.8153 | 143.5046 |
| S3 requests/run | 30 |   840.0 |   734.7 | 539840.0 | 87.5% |   400.0 |   4320.0 |
| S3 error rate   | 30 |  0.0000 |  0.0000 |   0.0000 |  0.0% |  0.0000 |   0.0000 |

Задержка по 30 запускам:

| Операция | Статистика |  n |      mean |     sigma | variance |    CV |       min |        max |
|----------|------------|---:|----------:|----------:|---------:|------:|----------:|-----------:|
| `PUT`    | median     | 30 | 488.87 ms | 174.78 ms | 30547.93 | 35.8% |  89.00 ms |  787.00 ms |
| `PUT`    | avg        | 30 | 495.97 ms | 180.75 ms | 32670.81 | 36.4% |  86.96 ms |  778.22 ms |
| `PUT`    | p90        | 30 | 541.31 ms | 206.94 ms | 42823.72 | 38.2% | 124.00 ms |  913.10 ms |
| `PUT`    | p95        | 30 | 580.82 ms | 248.42 ms | 61710.85 | 42.8% | 131.00 ms | 1036.10 ms |
| `GET`    | median     | 30 | 475.48 ms | 175.34 ms | 30745.32 | 36.9% |  61.00 ms |  740.50 ms |
| `GET`    | avg        | 30 | 486.16 ms | 186.25 ms | 34688.74 | 38.3% |  60.43 ms |  824.56 ms |
| `GET`    | p90        | 30 | 529.64 ms | 217.87 ms | 47466.16 | 41.1% | 106.00 ms |  991.00 ms |
| `GET`    | p95        | 30 | 576.68 ms | 290.65 ms | 84475.67 | 50.4% | 112.05 ms | 1501.00 ms |
| `HEAD`   | median     | 30 | 477.15 ms | 175.43 ms | 30776.00 | 36.8% |  62.00 ms |  742.50 ms |
| `HEAD`   | avg        | 30 | 486.89 ms | 183.26 ms | 33583.08 | 37.6% |  63.22 ms |  760.06 ms |
| `HEAD`   | p90        | 30 | 533.16 ms | 219.98 ms | 48391.02 | 41.3% | 111.00 ms | 1100.10 ms |
| `HEAD`   | p95        | 30 | 564.10 ms | 244.14 ms | 59604.06 | 43.3% | 118.00 ms | 1104.00 ms |
| `DELETE` | median     | 30 | 487.28 ms | 177.86 ms | 31634.59 | 36.5% |  66.00 ms |  759.50 ms |
| `DELETE` | avg        | 30 | 496.87 ms | 184.47 ms | 34027.99 | 37.1% |  67.77 ms |  757.98 ms |
| `DELETE` | p90        | 30 | 543.09 ms | 207.46 ms | 43039.63 | 38.2% | 114.00 ms |  926.20 ms |
| `DELETE` | p95        | 30 | 585.39 ms | 256.63 ms | 65856.60 | 43.8% | 119.00 ms | 1314.05 ms |

Потребление ресурсов процесса во время серии k6:

| Метрика           | Выборки |       mean |      sigma |      min |        max |
|-------------------|--------:|-----------:|-----------:|---------:|-----------:|
| CPU, macOS `%cpu` |     926 |    135.50% |      15.73 |    0.00% |    148.70% |
| RSS               |     926 | 330.06 MiB | 174.45 MiB | 4.84 MiB | 750.22 MiB |

Заметки:

- `scripts/k6/run-benchmark.sh` теперь параллельно снимает CPU/RSS и печатает агрегаты ресурсов.
- Benchmark отказывается запускаться против обнаруженного non-release процесса `so3`, если только
  `SO3_REQUIRE_RELEASE=0` не задан для локальной отладки скрипта.
- k6-скрипт использует локальную копию клиента Grafana S3 в `scripts/k6/lib/s3.js`.
- Высокая дисперсия throughput/RSS отражает один долгоживущий release-процесс и одну директорию данных
  на протяжении всех 30 запусков; неизменяемые blob-файлы не удаляются benchmark-циклом.
- macOS `%cpu` считает 100% как одно логическое ядро, поэтому 135.50% означает примерно 1.36 ядра в среднем.
- `GET` и `HEAD` сейчас координируют команды `Read` через consensus coordinator; это не локальные
  metadata-only чтения.

## Текущие ограничения

Известные пробелы, которые эти запуски пока не покрывают:

- production multi-node тесты через реальное tonic-взаимодействие с несколькими одновременными координаторами;
- recovery со специфичными только для локального узла зависимостями или выбором highest accepted ballot;
- обработка missed-PreAccept `Accept`, когда принимающая реплика обнаруживает дополнительные конфликты;
- проверка blob fetch/repair по ожидаемому размеру и SHA-256 из метаданных;
- атомарная устойчивая запись идентичности узла;
- гонки Maelstrom CAS в режиме create-if-missing.
