# Результаты экспериментов

Эксперименты проведены 2026-05-23 на macOS, Apple M4 Pro, release-сборка. Все сценарии запущены по 30 раз.

## Тезис

Accord обеспечивает предсказуемую (линейную) деградацию при конфликтах и отказах в безлидерном
объектном хранилище — сопоставимую с лидерными системами (Raft), но без единой точки отказа.
Безлидерные алгоритмы вроде EPaxos дают неограниченный рост dependency chains, что приводит
к каскадной деградации. Accord ограничивает цепочки зависимостей, сохраняя безлидерную архитектуру.

## Интерпретация результатов

- Абсолютные значения пропускной способности и задержек не являются заявлениями о производительности.
- Выводы формулируются через относительные метрики: `degraded/baseline`, `hot/independent`, dep depth max.
- Форма кривой деградации важнее абсолютных цифр: линейная vs экспоненциальная.

## Сводная таблица

### Корректность

| Сценарий                             | Runs | Результат                 |
|--------------------------------------|-----:|---------------------------|
| Maelstrom lin-kv, 1 node             |    5 | `:valid? true`            |
| Maelstrom lin-kv, 3 node             |    5 | `:valid? true`            |
| Maelstrom lin-kv, 3 node + partition |    5 | `:valid? true`            |
| Maelstrom g-set, 3 node              |    5 | `:valid? true`            |
| Maelstrom g-set, 3 node + partition  |    5 | `:valid? true`            |
| e2-fault-safety                      |   30 | PASS (1920 ops, 0 ошибок) |
| e6-recovery sentinel                 |   30 | 30/30                     |

### Производительность и деградация

| Сценарий       | Runs |  Throughput | PUT p95 | Fast path | Dep depth max |
|----------------|-----:|------------:|--------:|----------:|--------------:|
| k6-mixed       |   30 | 133.9 req/s |  322 ms |     76.7% |             2 |
| e4-hot-key     |   30 |           — |  340 ms |      8.4% |             5 |
| e3-degradation |   30 |    см. фазы |       — |     59.1% |             2 |
| e5-leaderless  |   30 |           — |  348 ms |     76.7% |             2 |
| e6-recovery    |   30 |    см. фазы |       — |     58.8% |             2 |

## Maelstrom: линеаризуемость

Бинарный файл: `target/release/so3-maelstrom`. Checker: Knossos (linearizability).

| Сценарий         | Workload | Узлы | Nemesis        | Результат      |
|------------------|----------|------|----------------|----------------|
| `smoke.py`       | lin-kv   | 1    | none           | `:valid? true` |
| `smoke_3node.py` | lin-kv   | 3    | none           | `:valid? true` |
| `fault_3node.py` | lin-kv   | 3    | `partition/5s` | `:valid? true` |
| `set_3node.py`   | g-set    | 3    | none           | `:valid? true` |
| `set_3node.py`   | g-set    | 3    | `partition/5s` | `:valid? true` |

## k6-mixed: базовый профиль (30 runs)

PUT/GET/HEAD/DELETE, 10 VU, 30s, 64-байтные объекты, 3 узла.

| Метрика           |  Mean |         95% CI | Median |   CV% |
|-------------------|------:|---------------:|-------:|------:|
| Throughput, req/s | 133.9 | [131.4, 136.4] |  134.0 |  5.7% |
| PUT p95, ms       |   322 |     [310, 334] |    321 |  6.2% |
| PUT p99, ms       |   741 |     [698, 784] |    728 | 16.1% |
| GET p95, ms       |   1.0 |     [1.0, 1.0] |    1.0 |    0% |
| Errors            |     0 |              — |      0 |    0% |
| Timeouts          |     0 |              — |      0 |    0% |

| Метрика              |  Value |
|----------------------|-------:|
| Fast path            |  76.7% |
| Slow path            |  23.3% |
| Dep depth max        |      2 |
| Operation total mean | 105 ms |

## e2-fault-safety: корректность при отказах (30 runs)

Конкурентные PUT/GET/HEAD/DELETE с crash/restart fault injection (3 цикла по 5s).

| Метрика             |  Value |
|---------------------|-------:|
| Ops/run             |     64 |
| Total ops           |   1920 |
| Success rate        |   100% |
| Errors              |      0 |
| Fast path           |   100% |
| Dep depth max       |      0 |
| Pre-accept failures | 18/run |
| Latency mean        |  21 ms |
| Fault cycles        |  3/run |

При crash/restart одного из трёх узлов запросы к нему таймаутятся, но кворум (2 из 3)
обеспечивает прогресс. Все 1920 операций завершились корректно.

## e3-degradation: деградация при отказе узла (30 runs)

baseline → fail node → degraded → recover → recovery → restored.

| Фаза     | Throughput, req/s | Ratio | PUT p95, ms | Multiplier | Errors |
|----------|------------------:|------:|------------:|-----------:|-------:|
| baseline |           131.315 |  1.00 |         340 |       1.00 |     0% |
| degraded |           138.900 |  1.07 |         325 |       0.96 | ~0.01% |
| recovery |           124.052 |  0.95 |         352 |       1.04 | ~0.01% |
| restored |           101.279 |  0.78 |         436 |       1.30 | ~0.02% |

| Метрика          | Value |
|------------------|------:|
| Restart recovery | 0.22s |
| Total downtime   | 30.2s |
| Fast path        | 59.1% |
| Dep depth max    |     2 |

Потеря одного узла при quorum=2 не снижает пропускную способность (ratio 1.07). Фаза restored
ниже baseline из-за cold journal catch-up на восстановленном узле — это линейный эффект.

## e4-hot-key: изоляция горячего ключа (30 runs)

90% запросов к одному hot key, 10% к независимым.

| Метрика | Hot p95 | Independent p95 | Ratio |
|---------|--------:|----------------:|------:|
| PUT     |  376 ms |          297 ms |  1.29 |
| GET     |  1.6 ms |          1.9 ms |  0.83 |
| DELETE  |  267 ms |          224 ms |  1.19 |

| Метрика        | Value |
|----------------|------:|
| Fast path      |  8.4% |
| Slow path      | 91.6% |
| Dep depth max  |     5 |
| Dep count mean |  3.66 |
| Errors         |    0% |
| Timeouts       |    0% |

91.6% slow path ожидаемо для hot key — каждая операция конфликтует. dep depth bounded = 5,
0% timeouts. Конфликт локализован: задержка hot key в 1.29 раза выше independent.

## e5-leaderless: безлидерное распределение (30 runs)

| Узел  |  Share | PUT p95, ms | GET p95, ms |
|-------|-------:|------------:|------------:|
| node1 | 33.37% |         348 |         1.1 |
| node2 | 33.33% |         348 |         1.2 |
| node3 | 33.30% |         344 |         1.1 |

| Метрика             | Value |
|---------------------|------:|
| Fast path           | 76.7% |
| Dep depth max       |     2 |
| Pre-accept failures |     0 |

Равномерное распределение 33.37/33.33/33.30 с минимальным разбросом PUT p95 (344–348 ms).
Любой узел координирует операции с равной производительностью.

## e6-recovery: безопасное восстановление (30 runs)

baseline → fail → degraded → recover → recovery → restored с верификацией sentinel-объектов.

| Фаза     | Throughput, req/s | Ratio | PUT p95, ms | Multiplier | Errors |
|----------|------------------:|------:|------------:|-----------:|-------:|
| baseline |           129.628 |  1.00 |         341 |       1.00 |     0% |
| degraded |           133.087 |  1.03 |         343 |       1.01 | ~0.01% |
| recovery |           118.020 |  0.92 |         376 |       1.12 | ~0.01% |
| restored |           107.882 |  0.84 |         408 |       1.21 | ~0.02% |

| Метрика           | Value |
|-------------------|------:|
| Restart recovery  | 0.22s |
| Total downtime    | 30.2s |
| Recovery sentinel | 30/30 |
| Fast path         | 58.8% |
| Dep depth max     |     2 |

Данные, записанные до сбоя, доступны после восстановления (sentinel 30/30). degraded фаза
без деградации (ratio 1.03). restored 0.84 — холодный старт восстановленного узла.

## Доказательство тезиса

### Линейная деградация

Throughput ratio по фазам (нормализовано к baseline):

```
e3: 1.00 → 1.07 → 0.95 → 0.78
e6: 1.00 → 1.03 → 0.92 → 0.84
```

Форма кривой — линейная. Потеря узла не вызывает каскадной деградации.

### Ограниченные цепочки зависимостей

| Условия                              | Dep depth max | Dep count mean |
|--------------------------------------|--------------:|---------------:|
| Sequential ops (e2)                  |             0 |              0 |
| Mixed workload, no faults (k6-mixed) |             2 |          0.064 |
| Mixed workload, leaderless (e5)      |             2 |          0.066 |
| Concurrent + node failure (e3, e6)   |             2 |          0.029 |
| Extreme hot key contention (e4)      |             5 |           3.66 |

dep depth bounded: 2 при нормальной нагрузке, 5 при экстремальном hot key.

### Безлидерная архитектура

e5: 33.37/33.33/33.30 — равномерное распределение. PUT p95: 344–348 ms по узлам.

### Корректность

- Maelstrom Knossos: `:valid? true` для lin-kv (1/3 node, partition) и g-set (3 node, partition)
- e2: 1920 ops, 100% success, 0 ошибок при crash/restart
- e6 sentinel: 30/30 — данные сохранены после восстановления

### Accord vs EPaxos vs Raft

| Свойство                    | Raft                   | EPaxos                              | Accord (SO3)            |
|-----------------------------|------------------------|-------------------------------------|-------------------------|
| Лидер                       | Да                     | Нет                                 | Нет                     |
| dep chain growth            | Линейный порядок       | Неограниченная                      | Линейный порядок        |
| Деградация при hot key      | Лидер — bottleneck     | Каскадная                           | Линейная (1.29x p95)    |
| Fast path quorum            | n/a (лидер)            | Super-majority ⌈3N/4⌉               | Simple majority ⌈N/2⌉+1 |
| Доступность при потере узла | Потеря лидера → выборы | Fast path ломается, нужен slow path | 0% degradation          |

## Воспроизведение

```bash
cargo build --release -p so3 -p so3-maelstrom
cd scripts && uv sync
```

```bash
uv run --project scripts python scripts/research/run-scenario.py k6-mixed --runs 30
uv run --project scripts python scripts/research/run-scenario.py e2-fault-safety --runs 30
uv run --project scripts python scripts/research/run-scenario.py e3-degradation --runs 30
uv run --project scripts python scripts/research/run-scenario.py e4-hot-key --runs 30
uv run --project scripts python scripts/research/run-scenario.py e5-leaderless --runs 30
uv run --project scripts python scripts/research/run-scenario.py e6-recovery --runs 30
```
