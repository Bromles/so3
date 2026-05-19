# Руководство по исследовательской инфраструктуре SO3

Этот документ описывает инфраструктуру запуска, формат результатов и интерпретацию метрик
исследовательских сценариев SO3. Руководство предназначено для воспроизведения экспериментов,
чтения результатов и понимания ограничений.

Исследовательская инфраструктура реализована в `scripts/research/` и управляется CLI
`run-scenario.py`. Подробный план экспериментов находится в [research-plan.md](research-plan.md),
план реализации инфраструктуры — в [research-implementation-plan.md](research-implementation-plan.md).

## Быстрый старт

### Сборка

```bash
cargo build --release -p so3 -p so3-maelstrom
```

### Зависимости

Python 3.11+:

```bash
pip install -r scripts/requirements.txt
```

k6: должен быть установлен и доступен в `PATH`. Проверка: `k6 version`.

### Минимальный запуск

```bash
# Базовый сценарий k6-mixed, 30 прогонов по 30 секунд
python scripts/research/run-scenario.py k6-mixed

# Для отладки: 3 прогона (требуется --allow-low-runs)
python scripts/research/run-scenario.py k6-mixed --runs 3 --allow-low-runs
```

Результаты записываются в `results/research/{scenario}-{timestamp}/`.

## Сценарии

### k6-mixed

**Что проверяет:** базовый S3-профиль PUT/GET/HEAD/DELETE при отсутствии отказов.

**Драйвер:** k6, workload `scripts/k6/workloads/s3_mixed.js`.

**Фазы:** одна фаза `baseline`, длительность управляется `--duration`.

**Ключевые параметры:** `--vus`, `--duration`, `--object-size`.

```bash
python scripts/research/run-scenario.py k6-mixed --vus 10 --duration 30s --runs 30
```

Результат: абсолютные метрики пропускной способности и задержек для каждого прогона.
Служит референсом для нормализованных сравнений в сценариях с отказами.

### e2-fault-safety

**Что проверяет:** корректность объектных операций при конкурентных сбоях узлов (crash/restart).
Параллельно с нагрузкой запускается fault-injector, который по очереди выключает и
перезапускает узлы с сохранением кворума.

**Драйвер:** Python CorrectnessDriver (boto3). Нагрузка — конкурентные
PUT/GET/HEAD/DELETE/overwrite через пул boto3-клиентов. После прогона история операций
проверяется верификатором.

**Фазы:** одна фаза `baseline` с конкурентным fault injection.

**Ключевые параметры:**

- `--correctness-ops N` — число операций (по умолчанию 120)
- `--correctness-concurrency N` — конкурентность (по умолчанию 12)
- `--e2-fault-cycles N` — число циклов crash/restart (по умолчанию = число узлов)
- `--e2-cycle-interval-secs F` — пауза между циклами (по умолчанию 10.0)
- `--e2-crash-duration-secs F` — длительность простоя узла в цикле (по умолчанию 5.0)

```bash
python scripts/research/run-scenario.py e2-fault-safety \
  --node-count 3 --correctness-ops 200 --correctness-concurrency 16
```

Результат: `client-history.jsonl` + `verifier-result.json` + `fault-cycles.json`.
Статус прогона `passed` только если верификатор подтвердил корректность.

### e3-degradation

**Что проверяет:** предсказуемую динамику деградации при отказе одного узла и восстановлении.
Фазированный сценарий: baseline → fail → degraded → recover → recovery → restored.

**Драйвер:** k6, workload `scripts/k6/workloads/s3_degradation.js`.

**Фазы:**

1. `baseline` — нагрузка на полную конфигурацию, замер базового уровня.
2. `(crash)` — отказ выбранного узла (event `fail`).
3. `degraded` — нагрузка без одного узла.
4. `(restart)` — возврат узла (event `recover`).
5. `recovery` — нагрузка в процессе синхронизации.
6. `restored` — нагрузка после стабилизации.

**Ключевые параметры:**

- `--fault-node N` — индекс узла для сбоя (1-based, по умолчанию 1)
- `--fault-node-policy {fixed,round_robin}` — политика выбора узла между прогонами
- `--baseline-duration`, `--degraded-duration`, `--recovery-duration`, `--restored-duration` — длительности фаз

```bash
python scripts/research/run-scenario.py e3-degradation \
  --node-count 3 --fault-node-policy round_robin --runs 30
```

Результат: четыре k6-export файла (`k6-summary-baseline.json` и т.д.),
относительные метрики (`relative`) и время стабилизации.

### e4-hot-key

**Что проверяет:** локализацию конфликта горячего ключа. В workload часть запросов
направлена в «горячий» ключ, часть — в независимые. Сравниваются задержки между классами.

**Драйвер:** k6, workload `scripts/k6/workloads/s3_hot_key.js`.

**Фазы:** одна фаза `baseline` с tag `key_class=hot|independent` в k6 stream.

**Ключевые параметры:** `--vus`, `--duration`.

```bash
python scripts/research/run-scenario.py e4-hot-key --vus 10 --duration 30s
```

Результат: `key_class_metrics` (статистика задержек по классам ключей),
`hot_vs_independent_p95_ratio` (отношение p95 задержки горячего ключа к независимому).
График `hot_key.png` сравнивает hot и independent задержки.

### e5-leaderless

**Что проверяет:** отсутствие постоянного лидера. Запросы направляются к разным узлам,
затем сравнивается распределение нагрузки.

**Драйвер:** k6, workload `scripts/k6/workloads/s3_leaderless.js`.

**Фазы:** одна фаза `baseline` с tag `entry_node` в k6 stream.

**Ключевые параметры:** `--vus`, `--duration`, `--node-count`.

```bash
python scripts/research/run-scenario.py e5-leaderless --node-count 3 --vus 10
```

Результат: `entry_node_metrics` (статистика задержек по входным узлам).
График `nodes.png` показывает распределение запросов по узлам.
Полная проверка симметрии отказов (отказ каждого узла по очереди) покрывается
через matrix-runs или через Maelstrom.

### e6-recovery

**Что проверяет:** безопасное восстановление отставшего узла после продлённого простоя.
Расширенная версия E3 с дополнительным временем простоя и верификацией sentinel-объектов.

**Драйвер:** k6, workload `scripts/k6/workloads/s3_recovery.js`.

**Фазы:**

1. `(sentinel write)` — запись sentinel-объектов через boto3 перед нагрузкой.
2. `baseline` — нагрузка на полную конфигурацию.
3. `(crash)` — отказ узла.
4. `degraded` — нагрузка без одного узла.
5. `(long downtime)` — дополнительное время простоя (`--e6-long-downtime-secs`).
6. `(restart)` — возврат узла.
7. `recovery` — нагрузка в процессе синхронизации.
8. `restored` — нагрузка после стабилизации.
9. `(sentinel verify)` — проверка, что sentinel-объекты сохранились.

При включении `--e6-re-crash` добавляются фазы:

10. `(re-crash)` — повторный отказ того же узла.
11. `re_crash_degraded` — нагрузка.
12. `(re-start)` — повторный возврат.
13. `re_recovery` — нагрузка при восстановлении.
14. `re_restored` — финальная нагрузка.

**Ключевые параметры:**

- Все параметры E3
- `--e6-long-downtime-secs F` — дополнительное время простоя (по умолчанию 0.0)
- `--e6-re-crash` — флаг включения под-сценария повторного сбоя
- `--e6-re-crash-duration DURATION` — длительность фазы после повторного сбоя (по умолчанию `15s`)

```bash
python scripts/research/run-scenario.py e6-recovery \
  --node-count 3 --e6-long-downtime-secs 30 --runs 30

# С повторным сбоем
python scripts/research/run-scenario.py e6-recovery \
  --node-count 3 --e6-re-crash --runs 30
```

Результат: четыре (+ три при re-crash) k6-export, `verifier-result.json` (sentinel),
время стабилизации для каждой фазы восстановления.

## Формат результатов

### Структура каталога одиночного сценария

```
results/research/{scenario}-{timestamp}/
  manifest.json                  # глобальный манифест (не прогона)
  run-001/
    manifest.json                # манифест прогона: топология, конфигурация, git revision
    events.jsonl                 # хронология событий (run_start, baseline_start, fail, recover, ...)
    summary.json                 # агрегированные метрики прогона
    k6-summary.json              # k6 JSON export (однофазные сценарии)
    k6-summary-{phase}.json      # k6 JSON export по фазам (e3, e6)
    k6-stream.jsonl              # k6 JSONL stream с тегами (e4, e5)
    k6.stdout.log                # stdout k6
    k6.stderr.log                # stderr k6
    cluster.log                  # агрегированный stdout/stderr процессов so3
    resources.jsonl              # семплы CPU/RSS по узлам
    client-history.jsonl         # история операций (e2: correctness driver)
    verifier-result.json         # вердикт верификатора (e2, e6)
    fault-cycles.json            # детали fault-циклов (e2)
    data/                        # временные данные узлов (удаляются, если нет --keep-data-dirs)
  run-002/
    ...
  aggregate-summary.json         # статистическая агрегация по всем прогонам
  report.md                      # человекочитаемый отчёт с таблицами и ссылками на графики
  plots/
    repeatability.png            # разброс метрик по прогонам
    phases.png                   # нормализованное поведение по фазам (e3, e6)
    timeline.png                 # хронология с маркерами событий (e3, e6)
    symmetry.png                 # симметрия отказов (e3, e6)
    recovery.png                 # поведение восстановления (e6)
    accord_paths.png             # распределение путей консенсуса
    hot_key.png                  # hot vs independent (e4)
    nodes.png                    # распределение по узлам (e5)
```

### manifest.json

Манифест прогона содержит полную конфигурацию для воспроизведения:

```json
{
  "schema_version": 1,
  "scenario": "e3-degradation",
  "run_index": 1,
  "seed": 1747800000,
  "created_at": "2026-05-21T12:00:00+00:00",
  "topology": {
    "node_count": 3,
    "entry_url": "http://127.0.0.1:3000",
    "entry_urls": [
      "http://127.0.0.1:3000",
      "http://127.0.0.1:3001",
      "http://127.0.0.1:3002"
    ],
    "nodes": [
      {
        "index": 1,
        "node_id": "00000000-0000-0000-0000-000000000001",
        "object_addr": "127.0.0.1:3000",
        "rpc_addr": "127.0.0.1:4000",
        "data_dir": "...",
        "url": "http://127.0.0.1:3000"
      },
      ...
    ]
  },
  "workload": {
    "driver": "k6",
    "script": "scripts/k6/workloads/s3_degradation.js",
    "mix": "s3_put_get_head_delete",
    "bucket": "bench",
    "object_size": 64,
    "vus": 10,
    "duration": "30s"
  },
  "phases": {
    "baseline": {
      "duration": "30s"
    },
    "degraded": {
      "duration": "30s"
    },
    "recovery": {
      "duration": "30s"
    },
    "restored": {
      "duration": "30s"
    }
  },
  "binary": {
    "path": "target/release/so3",
    "exists": true,
    "mtime": ...,
    "size_bytes": ...
  },
  "git_revision": "abc123...",
  "fault_injection": {
    "kind": "crash_restart",
    "node_index": 1,
    "node_policy": "fixed"
  },
  "environment": {
    "platform": "macOS-15.5-arm64-arm-64bit",
    "python": "3.12.0"
  }
}
```

### events.jsonl

Хронология событий прогона в формате JSONL (одна запись на строку):

```
{"ts":"2026-05-21T12:00:01Z","monotonic_secs":1.0,"event":"run_start","run_index":1,"seed":1747800000}
{"ts":"2026-05-21T12:00:02Z","monotonic_secs":2.0,"event":"cluster_start"}
{"ts":"2026-05-21T12:00:05Z","monotonic_secs":5.0,"event":"cluster_ready","pids":[12345,12346,12347]}
{"ts":"2026-05-21T12:00:06Z","monotonic_secs":6.0,"event":"baseline_start"}
{"ts":"2026-05-21T12:00:36Z","monotonic_secs":36.0,"event":"baseline_end"}
{"ts":"2026-05-21T12:00:36Z","monotonic_secs":36.1,"event":"fail","kind":"crash","node_index":1}
{"ts":"2026-05-21T12:01:07Z","monotonic_secs":66.1,"event":"recover","kind":"restart","node_index":1,"recovery_seconds":0.3}
...
{"ts":"2026-05-21T12:02:36Z","monotonic_secs":156.0,"event":"run_end"}
```

Каждое событие содержит `ts` (UTC ISO 8601), `monotonic_secs` (`time.monotonic()`),
`event` (имя события) и произвольные дополнительные поля.

### summary.json

Агрегированные метрики одного прогона:

```json
{
  "schema_version": 1,
  "scenario": "e3-degradation",
  "run_index": 1,
  "status": "passed",
  "metrics": {
    "phases": {
      "baseline": {
        "latency": {
          "put": {
            "avg_ms": 85.2,
            "p95_ms": 120.3,
            "p99_ms": 145.6
          },
          "get": {
            "avg_ms": 55.1,
            "p95_ms": 78.4,
            "p99_ms": 95.2
          }
        },
        "throughput": {
          "http_reqs": {
            "rate": 142.5,
            "count": 4275
          }
        },
        "errors": {
          "s3_errors": {
            "rate": 0.0,
            "passes": 4275,
            "fails": 0
          }
        },
        "successes": {
          "s3_successes": {
            "rate": 142.5,
            "passes": 4275
          }
        },
        "duration": {
          "test_run_seconds": 30.0
        }
      },
      "degraded": {
        ...
      },
      "recovery": {
        ...
      },
      "restored": {
        ...
      }
    },
    "relative": {
      "degraded": {
        "throughput": {
          "http_reqs_rate_ratio": 0.72
        },
        "latency": {
          "put": {
            "p95_multiplier": 1.45,
            "p99_multiplier": 1.6
          }
        },
        "success": {
          "s3_success_rate_ratio": 0.98
        },
        "timeout": {
          "s3_timeout_rate_ratio": 0.0
        }
      },
      "recovery": {
        ...
      },
      "restored": {
        ...
      }
    },
    "fault": {
      "node_index": 1,
      "recovery_seconds": 0.3,
      "total_downtime_secs": 35.0,
      "stabilization_secs": 8.3
    },
    "server": {
      "consensus": {
        ...
      },
      "apply": {
        ...
      }
    }
  }
}
```

Однофазные сценарии (`k6-mixed`, `e4-hot-key`, `e5-leaderless`) содержат `metrics` напрямую
(без вложенности `phases` и `relative`).

### aggregate-summary.json

Статистическая агрегация по N прогонам:

```json
{
  "schema_version": 1,
  "runs_total": 30,
  "runs_successful": 30,
  "runs_failed": 0,
  "failed_reasons": {},
  "verdict": "passed",
  "metrics": {
    "phases.baseline.latency.put.p95_ms": {
      "n": 30,
      "mean": 119.5,
      "ci_lower": 117.2,
      "ci_upper": 121.8,
      "ci_confidence": 0.95,
      "median": 119.0,
      "stddev": 6.1,
      "min": 108.3,
      "max": 135.7,
      "cv_percent": 5.1,
      "p10": 112.0,
      "p25": 115.5,
      "p75": 123.0,
      "p90": 127.8,
      "p95": 131.2,
      "p99": 134.1
    },
    ...
  },
  "phase_metrics": {
    "baseline": {
      "latency.put.p95_ms": {
        ...
      },
      ...
    },
    "degraded": {
      ...
    },
    ...
  },
  "relative_metrics": {
    "degraded": {
      "throughput.http_reqs_rate_ratio": {
        "n": 30,
        "mean": 0.73,
        "ci_lower": 0.70,
        ...
      },
      "latency.put.p95_multiplier": {
        "n": 30,
        "mean": 1.42,
        ...
      },
      ...
    },
    ...
  }
}
```

Каждое числовое поле представлено описательной статистикой:
`n`, `mean`, `ci_lower`, `ci_upper`, `ci_confidence`, `median`, `variance`, `stddev`,
`min`, `max`, `cv_percent`, `p10`, `p25`, `p75`, `p90`, `p95`, `p99`.

### client-history.jsonl (сценарии e2, e1)

История операций correctness driver (JSONL, одна запись на операцию):

```json
{
  "schema_version": 1,
  "operation_id": "PUT-000042",
  "idempotency_key": null,
  "operation_type": "PUT",
  "key": "correctness/key-0042",
  "input_value_hash": "a1b2c3...",
  "returned_value_hash": null,
  "observed_version": null,
  "etag": "\"abc123\"",
  "start_timestamp": "2026-05-21T12:00:10Z",
  "end_timestamp": "2026-05-21T12:00:10.050Z",
  "start_monotonic_secs": 10.0,
  "end_monotonic_secs": 10.05,
  "latency_ms": 50.0,
  "entry_node": "http://127.0.0.1:3001",
  "endpoint": "put_object",
  "result_code": 200,
  "success": true,
  "timeout": false,
  "error": null,
  "error_code": null,
  "client": "boto3",
  "api": "s3"
}
```

### verifier-result.json (сценарии e2, e6)

Результат проверки инвариантов:

```json
{
  "schema_version": 1,
  "verdict": "passed",
  "operation_count": 120,
  "checked": [
    "reads_return_only_successfully_written_values",
    "head_etag_matches_successfully_written_values",
    "successful_delete_hides_prior_value_until_next_successful_put"
  ],
  "unsupported": [
    "cas_success_requires_matching_version",
    "if_none_match_success_requires_absence",
    "same_idempotency_key_does_not_create_second_change"
  ],
  "issues": []
}
```

Для e6 (RecoverySentinel) структура аналогичная, но проверяется инвариант
`recovery_preserves_confirmed_writes`.

### resources.jsonl

Семплы ресурсов (CPU, RSS) по узлам:

```json
{
  "ts": "2026-05-21T12:00:06Z",
  "monotonic_secs": 6.0,
  "node_index": 1,
  "cpu_percent": 45.2,
  "rss_mb": 42.3
}
```

### fault-cycles.json (e2)

Детали fault-циклов:

```json
{
  "fault_cycles_planned": 3,
  "fault_cycles_completed": 3,
  "total_node_unavailable_secs": 18.5,
  "mean_node_unavailable_secs": 6.17,
  "cycles": [
    {
      "cycle": 0,
      "node_index": 1,
      "crash_monotonic": 15.0,
      "restart_monotonic": 20.0,
      "ready_monotonic": 20.5,
      "node_unavailable_secs": 5.5
    },
    ...
  ]
}
```

## Интерпретация метрик

### Нормализация

Все выводы формулируются через относительные метрики, а не через абсолютные значения.
Базовый уровень — фаза `baseline` в каждом прогоне.

| Метрика                        | Формула                                      | Интерпретация                                         |
|--------------------------------|----------------------------------------------|-------------------------------------------------------|
| `http_reqs_rate_ratio`         | `rate_phase / rate_baseline`                 | < 1.0 — снижение пропускной способности; > 1.0 — рост |
| `p95_multiplier`               | `p95_phase / p95_baseline`                   | > 1.0 — деградация задержки относительно baseline     |
| `p99_multiplier`               | `p99_phase / p99_baseline`                   | аналогично для хвоста распределения                   |
| `s3_success_rate_ratio`        | `success_rate_phase / success_rate_baseline` | < 1.0 — рост доли ошибок                              |
| `s3_timeout_rate_ratio`        | `timeout_rate_phase / timeout_rate_baseline` | > 1.0 — рост доли таймаутов                           |
| `hot_vs_independent_p95_ratio` | `p95_hot / p95_independent`                  | > 1.0 — горячий ключ медленнее независимых            |

### Время стабилизации

`stabilization_secs` — время от начала фазы восстановления до момента, когда пропускная
способность достигает 90% от baseline-уровня и удерживается в окне 5 секунд.

Алгоритм: k6 JSONL stream разбивается на окна по `window_secs` секунд (по умолчанию 5.0).
Считается количество точек (Point-записей метрик задержки) в каждом окне. Первое окно,
в котором `count / window_secs >= threshold * baseline_rate`, определяет время стабилизации.

Если порог не достигнут за время фазы, `stabilization_secs` = `None`.

### Статистическая агрегация

Каждый сценарий запускается минимум 30 раз. Для каждого числового показателя
рассчитывается описательная статистика по N значениям:

- **n** — количество наблюдений
- **mean** — среднее арифметическое
- **median** — медиана
- **stddev** — стандартное отклонение (выборочное, `ddof=1`)
- **variance** — дисперсия
- **cv_percent** — коэффициент вариации (`stddev / mean * 100`), мера разброса
- **ci_lower, ci_upper** — 95%-ный доверительный интервал (t-распределение)
- **min, max** — минимум и максимум
- **p10, p25, p75, p90, p95, p99** — процентили

Если часть прогонов завершилась ошибкой:

- `runs_successful` и `runs_failed` показывают разбивку
- `failed_reasons` — словарь `{причина: количество}`
- `verdict` = `"failed"`, если хотя бы один прогон упал
- статистика считается только по успешным прогонам

### Метрики серверного консенсуса

Метрики парсятся из `cluster.log` — структурированных строк с `coordination_event`.
Добавляются в `summary.json` под ключом `server`.

**Пути консенсуса** (`server.consensus.path`):

| Путь       | Описание                                              |
|------------|-------------------------------------------------------|
| `fast`     | Быстрый путь: PreAccept получил кворум без конфликтов |
| `slow`     | Медленный путь: потребовался Accept после конфликта   |
| `recovery` | Путь восстановления: восстановление после сбоя        |

Каждый путь содержит `count` (абсолютное количество) и `ratio` (доля от общего числа операций).

**Операции** (`server.consensus.operation`):
Распределение по типам: `put`, `get`, `delete`, `head` — `count` + `ratio`.

**Фазовые тайминги** (в миллисекундах, `mean` + `max` + `total`):

| Метрика          | Описание                         |
|------------------|----------------------------------|
| `pre_accept_ms`  | Длительность фазы PreAccept      |
| `accept_ms`      | Длительность фазы Accept         |
| `commit_ms`      | Длительность фазы Commit         |
| `apply_ms`       | Длительность фазы Apply          |
| `recover_ms`     | Длительность фазы Recovery       |
| `total_ms`       | Общее время координации операции |
| `quorum_wait_ms` | Время ожидания кворума           |

**Метрики зависимостей и конфликтов:**

| Метрика                | Описание                                     |
|------------------------|----------------------------------------------|
| `dependency_count`     | Количество зависимостей операции             |
| `dependency_depth`     | Глубина цепочки зависимостей (нижняя оценка) |
| `retry_count`          | Количество повторов                          |
| `in_flight_operations` | Число операций в полёте                      |
| `pre_accept_failures`  | Число неудачных PreAccept                    |

**Apply-метрики** (`server.apply`):

| Метрика               | Описание                      |
|-----------------------|-------------------------------|
| `reorder_buffer_size` | Размер reorder buffer         |
| `dependency_wait_ms`  | Время ожидания зависимостей   |
| `journal_apply_ms`    | Время применения к журналу    |
| `metadata_apply_ms`   | Время применения к метаданным |
| `apply_total_ms`      | Общее время apply             |

## Верификатор корректности

### Как работает

Верификатор (`scripts/verify/verify_history.py`) читает `client-history.jsonl`,
строит модель известных значений и проверяет инварианты объектного уровня.

Алгоритм:

1. История сортируется по `end_monotonic_secs`.
2. Первый проход: собираются множества успешно записанных значений (`known_values_by_key`)
   и etag-ов (`known_etags_by_key`) для каждого ключа.
3. Второй проход: для каждого успешного GET/HEAD проверяется, что возвращённое значение
   или etag присутствуют в известном множестве.
4. Для операций после успешного DELETE проверяется, что не возвращаются старые значения
   до следующего успешного PUT.

### Проверяемые инварианты

| Инвариант                                                       | Описание                                                              |
|-----------------------------------------------------------------|-----------------------------------------------------------------------|
| `reads_return_only_successfully_written_values`                 | GET возвращает только хеши, которые были успешно PUT                  |
| `head_etag_matches_successfully_written_values`                 | HEAD возвращает etag, наблюдённый при PUT или GET                     |
| `successful_delete_hides_prior_value_until_next_successful_put` | После DELETE значение не видно до следующего PUT                      |
| `recovery_preserves_confirmed_writes` (e6)                      | Sentinel-объекты до сбоя читаются после восстановления с тем же хешем |

### Неподдерживаемые инварианты

Эти инварианты помечаются как `unsupported` в `verifier-result.json`:

| Инвариант                                            | Почему не поддерживается                                      |
|------------------------------------------------------|---------------------------------------------------------------|
| `cas_success_requires_matching_version`              | S3 API не выражает CAS / If-Match напрямую в текущем драйвере |
| `if_none_match_success_requires_absence`             | S3 API не поддерживает If-None-Match для условной записи      |
| `same_idempotency_key_does_not_create_second_change` | S3 API не поддерживает idempotency key                        |

### Результат верификации

`verdict`:

- `"passed"` — все проверяемые инварианты выполнены
- `"failed"` — хотя бы один инвариант нарушен; детали в массиве `issues`

Для e2-fault-safety статус прогона (`summary.json.status`) = `"passed"` только при
`verifier-result.json.verdict == "passed"`.

Для e6-recovery sentinel верификация независима от k6-метрик: статус прогона `"passed"`,
но `fault.verifier_passed` показывает результат sentinel-проверки.

## Maelstrom: проверка линеаризуемости

[Maelstrom](https://github.com/jepsen-io/maelstrom) — фреймворк от Jepsen для проверки распределённых алгоритмов.
SO3 использует его для **E1 (correctness under concurrency)** и **части E2 (partition safety)** через
бинарный адаптер `so3-maelstrom` и workload `lin-kv`.

Maelstrom даёт формально строгую проверку **linearizability** через Knossos checker — это более
сильная гарантия, чем инварианты верификатора `scripts/verify/`. В то время как верификатор
проверяет object-level инварианты (GET не возвращает «левые» данные, DELETE семантика), Maelstrom
проверяет, что **вся история операций** линейизуема, то есть существует корректный последовательный
порядок, согласованный с реальным временем вызовов.

### Роль в исследовательском плане

| Сценарий                               | Что проверяет Maelstrom                                                           |
|----------------------------------------|-----------------------------------------------------------------------------------|
| **E1** — Correctness under concurrency | Линеаризуемость конкурентных `read`/`write`/`cas` без отказов                     |
| **E2** — Fault safety                  | Линеаризуемость при partition nemesis (сбой сети между узлами)                    |
| **E5** — Leaderless behavior           | Симметрия отказов через partition по всем узлам; Knossos проверяет линейизуемость |

### Как работает

1. `so3-maelstrom` — отдельный бинарный пакет, который переиспользует код консенсуса из `so3-core`,
   но заменяет tonic-транспорт на JSON-сообщения через stdin/stdout Maelstrom.
2. Maelstrom запускает каждый узел как отдельный процесс и передаёт список узлов в сообщении `init`.
3. Адаптер строит изолированный стек для каждого узла: SQLite metadata, Accord consensus coordinator,
   inbound consensus handler.
4. Maelstrom генерирует клиентские запросы (`read`, `write`, `cas`) с заданным `rate` и `concurrency`.
5. При включённом nemesis (`partition`) Maelstrom разбивает сеть на группы и проверяет, что система
   сохраняет корректность.
6. Knossos checker анализирует полную историю операций и выносит вердикт `:valid? true/false`.

### Скрипты

Все скрипты находятся в `scripts/maelstrom/`. Они кросс-платформенные (Python).

| Скрипт           | Описание                                                              |
|------------------|-----------------------------------------------------------------------|
| `install.py`     | Скачивает Maelstrom в `.tools/maelstrom/`                             |
| `run.py`         | Единый запуск lin-kv теста (гибкие параметры)                         |
| `run_30.py`      | 30 прогонов с агрегацией pass/fail, `aggregate.json`, `report.md`     |
| `smoke.py`       | Быстрый 1-узловой smoke (10 сек, rate=20)                             |
| `smoke_3node.py` | 3-узловой smoke без nemesis (10 сек, rate=10)                         |
| `fault_3node.py` | 3-узловой с partition nemesis (30 сек, rate=20, nemesis=partition/5s) |

### Nemesis

Maelstrom поддерживает следующие nemesis-типы:

- **`partition`** — разделяет кластер на две группы, блокируя сеть между ними. Проверяет, что minority
  partition не принимает небезопасные записи и что после heal partition реплики сходятся.
- **`partition-ring`** — кольцевой раздел: каждый узел видит только одного соседа.
- **`pause-start-stop`** — приостанавливает и возобновляет узлы через SIGSTOP/SIGCONT (Unix-only).
- Комбинации: `--nemesis "partition,pause-start-stop"`.

### Формат результатов Maelstrom

**Единичный запуск** (`run.py`): Maelstrom пишет историю и логи в `store/lin-kv/<timestamp>/`:

- `history.edn` — полная история операций
- `results.edn` — вердикт Knossos (`:valid?`, `:conflict-count`, `:anomaly-count`)
- `node-*.log` — stdout/stderr каждого узла
- `maelstrom.log` — лог Maelstrom

**30 прогонов** (`run_30.py`): пишет в `results/maelstrom/lin-kv-<timestamp>/`:

- `run-001/result.json` ... `run-030/result.json` — per-run outcome:
  ```json
  {
    "run_index": 1,
    "returncode": 0,
    "passed": true,
    "elapsed_secs": 35.2
  }
  ```
- `aggregate.json` — сводка:
  ```json
  {
    "schema_version": 1,
    "runs_total": 30,
    "runs_passed": 30,
    "runs_failed": 0,
    "pass_rate": 1.0,
    "verdict": "passed"
  }
  ```
- `report.md` — markdown-отчёт с таблицей результатов по прогонам

### Ограничения адаптера

Адаптер `so3-maelstrom` намеренно отличается от production-бинарника `so3`:

1. **Координатор:** все клиентские запросы пересылаются на один координатор (`node_ids.first()`),
   в отличие от production, где каждый узел координирует самостоятельно. Это смягчает проверку
   leaderless-поведения через Maelstrom, но Knossos всё равно проверяет линейизуемость.
2. **CAS:** `cas` с `create_if_not_exists=true` выполняет read-then-write без атомарности;
   две конкурентные create-операции могут обе вернуть `cas_ok`.
3. **Blob:** использует JSON payload вместо tonic `BlobService`; не проверяет размер/SHA-256.
4. **Таймауты:** ожидающие запросы ждут oneshot-ответы без дедлайнов.

Эти ограничения не влияют на корректность проверки линейизуемости для операций `lin-kv`,
но означают, что Maelstrom не покрывает все production-сценарии. Полный набор PoC-проверок
описан в [research-plan.md](research-plan.md).

Подробная документация по Maelstrom-адаптеру: [maelstrom.md](maelstrom.md).

## Server-side observability

Сервер SO3 логирует структурированные события консенсуса в stdout. Runner собирает
stdout/stderr всех узлов в `cluster.log`, затем парсит строки с маркером
`coordination_event`.

### Формат событий

Строки в `cluster.log` содержат пары `key=value`:

```
coordination_event=consensus_operation consensus_path=fast operation=put pre_accept_ms=12.3 ...
```

### Что парсится

- События `consensus_operation`: путь (fast/slow/recovery), операция, фазовые тайминги,
  количество зависимостей, глубина, повторы.
- События `apply_backlog`: размер reorder buffer, время ожидания зависимостей,
  время применения.

### Ограничения парсинга

- Зависимости: логируется `dependency_count` (нижняя оценка), но не полный граф
  зависимостей.
- Recovery-специфичные breakdown: сервер пишет общие `apply_backlog` события,
  но не различает recovery-специфичные фазы.
- Парсинг зависит от формата логов: при изменении формата на стороне сервера
  требуется обновление `metrics.summary_from_cluster_log`.

## Графики

Генерация графиков — best-effort: отсутствие метрик просто пропускает соответствующий
график. Графики сохраняются в `plots/` внутри каталога результатов.

| График          | Файл                | Когда генерируется          | Содержание                             |
|-----------------|---------------------|-----------------------------|----------------------------------------|
| Повторяемость   | `repeatability.png` | Всегда                      | Точечная диаграмма метрик по прогонам  |
| Пути консенсуса | `accord_paths.png`  | При наличии server-метрик   | Доля fast/slow/recovery путей          |
| Фазы            | `phases.png`        | e3-degradation, e6-recovery | Нормализованные метрики по фазам       |
| Хронология      | `timeline.png`      | e3-degradation, e6-recovery | Метрики во времени с маркерами событий |
| Симметрия       | `symmetry.png`      | e3-degradation, e6-recovery | Деградация при отказе разных узлов     |
| Восстановление  | `recovery.png`      | e6-recovery                 | Динамика восстановления                |
| Горячий ключ    | `hot_key.png`       | e4-hot-key                  | Сравнение hot vs independent задержек  |
| Узлы            | `nodes.png`         | e5-leaderless               | Распределение запросов по узлам        |

Для генерации графиков требуется `matplotlib` (входит в `scripts/requirements.txt`).

## Matrix-runs

Matrix-runs позволяют запустить сценарий для нескольких размеров кластера (3, 5, 7 узлов)
в одном вызове CLI. Поддерживается для `e3-degradation` и `e6-recovery`.

### Структура результатов

```
results/research/{scenario}-{timestamp}/
  nodes-3/
    run-001/... run-002/... aggregate-summary.json report.md plots/
  nodes-5/
    run-001/...
  nodes-7/
    run-001/...
  matrix-summary.json            # кросс-узловая агрегация
  matrix-report.md               # сравнительные таблицы
```

### Запуск

```bash
python scripts/research/run-scenario.py e3-degradation --matrix-node-counts --runs 30
```

`--matrix-node-counts` и `--node-count` взаимоисключающие.

### matrix-summary.json

Содержит агрегацию по каждому размеру кластера + кросс-узловое сравнение:

- `stabilization_time` по node-count
- `throughput_ratio` по node-count
- `verifier_pass_rate` по node-count (для e6)

### Требования к оборудованию

5-узловый кластер требует ~5× ресурсов одноузлового; 7-узловый — ~7×.
На одном хосте с 16 ГБ RAM и 4 ядрами стабильнее ограничиться 3 узлами.

## Ограничения

### Неподдерживаемые проверки S3 API

Следующие проверки помечаются верификатором как `unsupported` и не влияют на вердикт:

1. **CAS / If-Match** — S3 API в текущем драйвере не отправляет условные заголовки.
2. **If-None-Match** — S3 API не поддерживает условную запись «только если отсутствует».
3. **Idempotency key** — S3 API не имеет встроенного механизма идемпотентности.

### Сетевые разделы

**Реальные сетевые разделы не поддерживаются** в `run-scenario.py`. Текущая инфраструктура
использует только crash/restart на уровне процессов. Для проверки поведения при сетевом разделении:

- **Используйте Maelstrom** (`scripts/maelstrom/`): Maelstrom эмулирует partition nemesis
  и проверяет linearizability через Knossos. Это покрывает E1 (correctness), E2/partition (fault safety)
  и E5 (symmetry of failures). См. раздел [Maelstrom: проверка линеаризуемости](#maelstrom-проверка-линеаризуемости)
  и [maelstrom.md](maelstrom.md).
- **Proxy-based fault layer** не реализован: настоящий сетевой раздел между процессами
  SO3 на localhost требует прокси-слой (например, `toxiproxy`), который пока не интегрирован.

### Частичные проверки

1. **Re-crash во время синхронизации** — реализован через `--e6-re-crash`, но тайминг
   crash-based (после полной фазы recovery), а не mid-operation. Полная проверка
   mid-operation re-crash требует инъекции на стороне сервера.
2. **Полный граф зависимостей** — сервер логирует `dependency_count` (нижняя оценка),
   но не полный traversal графа зависимостей.
3. **Recovery-specific breakdown** — сервер пишет общие `apply_backlog` события,
   но не различает recovery-специфичные под-фазы (sync backlog, dependency resolution, catchup).
4. **Распределение нагрузки между координаторами** — E5 проверяет распределение
   запросов по entry-узлам, но не проверяет, что разные узлы координируют операции
   (это visible только через server-side `coordinator_node` метрики).

### Требования к окружению

- k6 должен быть в `PATH`
- Python ≥ 3.11 с установленными зависимостями
- `cargo build --release` должен быть выполнен до запуска
- Для matrix-runs с 5/7 узлами — достаточный объём RAM (ориентировочно: ~200 МБ на узел)
- Все сценарии запускают SO3 на `127.0.0.1`, порты начинаются с 3000 (object) и 4000 (rpc)

Для Maelstrom дополнительно:

- Java ≥ 11 должна быть в `PATH`
- Maelstrom jar: установить через `scripts/maelstrom/install.py` или указать через `--maelstrom-jar`
- На Windows: требуются symlinks (Developer Mode или elevated shell)

## Примеры команд

### Базовые сценарии

```bash
# Базовый S3-профиль, 30 прогонов
python scripts/research/run-scenario.py k6-mixed

# С явными параметрами
python scripts/research/run-scenario.py k6-mixed \
  --vus 20 --duration 60s --object-size 1024 --runs 30

# Кастомный каталог результатов
python scripts/research/run-scenario.py k6-mixed --outdir /tmp/my-results
```

### Корректность с отказами

```bash
# E2: 200 операций, конкурентность 16, 5 fault-циклов
python scripts/research/run-scenario.py e2-fault-safety \
  --node-count 3 --correctness-ops 200 --correctness-concurrency 16 \
  --e2-fault-cycles 5 --e2-cycle-interval-secs 8 --e2-crash-duration-secs 5

# Отладка: 3 прогона
python scripts/research/run-scenario.py e2-fault-safety \
  --runs 3 --allow-low-runs --correctness-ops 50
```

### Деградация

```bash
# E3: 3 узла, отказ узла round-robin
python scripts/research/run-scenario.py e3-degradation \
  --node-count 3 --fault-node-policy round_robin --runs 30

# Кастомные длительности фаз
python scripts/research/run-scenario.py e3-degradation \
  --baseline-duration 60s --degraded-duration 45s \
  --recovery-duration 30s --restored-duration 60s --runs 30

# Matrix: 3, 5, 7 узлов
python scripts/research/run-scenario.py e3-degradation \
  --matrix-node-counts --runs 30
```

### Горячий ключ

```bash
python scripts/research/run-scenario.py e4-hot-key --vus 20 --duration 60s
```

### Leaderless

```bash
# 5 узлов: проверить распределение нагрузки
python scripts/research/run-scenario.py e5-leaderless --node-count 5 --vus 20
```

### Восстановление

```bash
# E6: стандартное восстановление
python scripts/research/run-scenario.py e6-recovery --node-count 3 --runs 30

# Продлённый простой (30 секунд)
python scripts/research/run-scenario.py e6-recovery \
  --e6-long-downtime-secs 30 --runs 30

# Продлённый простой + повторный сбой
python scripts/research/run-scenario.py e6-recovery \
  --e6-long-downtime-secs 30 --e6-re-crash --e6-re-crash-duration 20s --runs 30

# Matrix-runs
python scripts/research/run-scenario.py e6-recovery \
  --matrix-node-counts --e6-long-downtime-secs 20 --runs 30
```

### Maelstrom: линеаризуемость

```bash
# Установить Maelstrom (один раз)
python scripts/maelstrom/install.py

# Собрать адаптер
cargo build --release -p so3-maelstrom

# Быстрый 1-узловой smoke (10 секунд)
python scripts/maelstrom/smoke.py

# 3-узловой smoke без отказов (10 секунд)
python scripts/maelstrom/smoke_3node.py

# 3-узловой с partition nemesis (30 секунд)
python scripts/maelstrom/fault_3node.py

# 30 прогонов с агрегацией, partition nemesis
python scripts/maelstrom/run_30.py \
  --node-count 3 --rate 100 --nemesis partition --nemesis-interval 5

# 5-узловой кастомный запуск
python scripts/maelstrom/run.py \
  --node-count 5 --time-limit 60 --rate 50 --concurrency 10 \
  --nemesis partition --nemesis-interval 3 --log-stderr

# Без nemesis (только проверка конкурентности)
python scripts/maelstrom/run.py --node-count 3 --time-limit 30 --rate 100

# С кастомным путём к Maelstrom
python scripts/maelstrom/run_30.py \
  --maelstrom-jar .tools/maelstrom/maelstrom/lib/maelstrom.jar \
  --binary-path target/release/so3-maelstrom
```

### Отладка и диагностика

```bash
# Вывод k6 в реальном времени
python scripts/research/run-scenario.py k6-mixed --debug-k6 --runs 3 --allow-low-runs

# Сохранять директории данных узлов (для ручной инспекции)
python scripts/research/run-scenario.py e3-degradation --keep-data-dirs --runs 3 --allow-low-runs

# Фиксированный seed для воспроизводимости
python scripts/research/run-scenario.py k6-mixed --seed 42 --runs 3 --allow-low-runs

# Кастомный бинарник
python scripts/research/run-scenario.py k6-mixed --so3-bin ./target/debug/so3 --runs 3 --allow-low-runs
```

## Полный список параметров CLI

| Параметр                            | По умолчанию                       | Описание                                                                                                |
|-------------------------------------|------------------------------------|---------------------------------------------------------------------------------------------------------|
| `scenario` (позиционный)            | `k6-mixed`                         | Сценарий: `k6-mixed`, `e2-fault-safety`, `e3-degradation`, `e4-hot-key`, `e5-leaderless`, `e6-recovery` |
| `--runs N`                          | 30                                 | Число прогонов (минимум 30, если нет `--allow-low-runs`)                                                |
| `--allow-low-runs`                  | —                                  | Разрешить `--runs` меньше 30 (для отладки)                                                              |
| `--node-count {1,3,5,7}`            | 3                                  | Размер кластера SO3                                                                                     |
| `--matrix-node-counts`              | —                                  | Запустить для 3, 5 и 7 узлов (только e3, e6)                                                            |
| `--outdir PATH`                     | `results/research/{scenario}-{ts}` | Каталог результатов                                                                                     |
| `--so3-bin PATH`                    | `target/release/so3`               | Путь к бинарнику SO3                                                                                    |
| `--k6-script PATH`                  | автоподбор по сценарию             | Переопределить k6 workload                                                                              |
| `--host`                            | `127.0.0.1`                        | Адрес привязки                                                                                          |
| `--object-base-port`                | 3000                               | Начальный порт object API                                                                               |
| `--rpc-base-port`                   | 4000                               | Начальный порт RPC                                                                                      |
| `--duration DURATION`               | `30s`                              | Длительность фазы                                                                                       |
| `--baseline-duration`               | = `--duration`                     | Длительность фазы baseline (e3, e6)                                                                     |
| `--degraded-duration`               | = `--duration`                     | Длительность фазы degraded (e3, e6)                                                                     |
| `--recovery-duration`               | = `--duration`                     | Длительность фазы recovery (e3, e6)                                                                     |
| `--restored-duration`               | = `--duration`                     | Длительность фазы restored (e3, e6)                                                                     |
| `--fault-node N`                    | 1                                  | Индекс узла для сбоя (1-based)                                                                          |
| `--fault-node-policy`               | `fixed`                            | `fixed` или `round_robin`                                                                               |
| `--vus N`                           | 10                                 | Число виртуальных пользователей k6                                                                      |
| `--object-size N`                   | 64                                 | Размер объекта в байтах                                                                                 |
| `--bucket NAME`                     | `bench`                            | Имя S3 бакета                                                                                           |
| `--seed N`                          | `int(time.time())`                 | Seed генератора случайных чисел                                                                         |
| `--correctness-ops N`               | 120                                | Число операций (e2)                                                                                     |
| `--correctness-concurrency N`       | 12                                 | Конкурентность (e2)                                                                                     |
| `--e2-fault-cycles N`               | `--node-count`                     | Число crash/restart циклов (e2)                                                                         |
| `--e2-cycle-interval-secs F`        | 10.0                               | Пауза между fault-циклами (e2)                                                                          |
| `--e2-crash-duration-secs F`        | 5.0                                | Длительность простоя в цикле (e2)                                                                       |
| `--e6-long-downtime-secs F`         | 0.0                                | Дополнительное время простоя (e6)                                                                       |
| `--e6-re-crash`                     | —                                  | Включить под-сценарий повторного сбоя (e6)                                                              |
| `--e6-re-crash-duration`            | `15s`                              | Длительность degraded после re-crash (e6)                                                               |
| `--start-timeout-secs F`            | 20.0                               | Таймаут запуска кластера                                                                                |
| `--stop-timeout-secs F`             | 10.0                               | Таймаут остановки кластера                                                                              |
| `--resource-sample-interval-secs F` | 1.0                                | Интервал семплирования ресурсов                                                                         |
| `--keep-data-dirs`                  | —                                  | Не удалять директории данных после прогона                                                              |
| `--debug-k6`                        | —                                  | Выводить k6 stdout/stderr в реальном времени                                                            |
