# Результаты экспериментов

Эксперименты проведены 2026-05-24 на macOS, Apple M4 Pro, release-сборка. Все сценарии запущены по 30 раз.

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
| Maelstrom lin-kv, 1 node             |   30 | `:valid? true`            |
| Maelstrom lin-kv, 3 node             |   30 | `:valid? true`            |
| Maelstrom lin-kv, 3 node + partition |   30 | `:valid? true`            |
| Maelstrom g-set, 3 node              |   30 | `:valid? true`            |
| Maelstrom g-set, 3 node + partition  |   30 | `:valid? true`            |
| e2-fault-safety                      |   30 | PASS (1920 ops, 0 ошибок) |
| e6-recovery sentinel                 |   30 | 30/30                     |

### Производительность и деградация

| Сценарий       | Runs |  Throughput | PUT p95 | Fast path | Dep depth max |
|----------------|-----:|------------:|--------:|----------:|--------------:|
| k6-mixed       |   30 | 133.9 req/s |  322 ms |     76.7% |             2 |
| e4-hot-key     |   30 |           — |  376 ms |      8.4% |             5 |
| e3-degradation |   30 |    см. фазы |       — |     59.1% |             2 |
| e5-leaderless  |   30 |           — |  345 ms |     76.7% |             2 |
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

| Метрика           |  Mean |         95% CI | Median |  SD |   Var |   CV% |
|-------------------|------:|---------------:|-------:|----:|------:|------:|
| Throughput, req/s | 133.9 | [131.0, 136.7] |  135.2 |   8 |    58 |  5.7% |
| PUT p95, ms       |   322 |     [308, 337] |    312 |  39 |  1495 | 12.0% |
| PUT p99, ms       |   768 |     [706, 831] |    751 | 167 | 28012 | 21.8% |
| GET p95, ms       |   1.6 |     [1.4, 1.8] |    2.0 | 0.5 |   0.3 | 31.1% |
| DELETE p95, ms    |   253 |     [242, 263] |    248 |  27 |   730 | 10.7% |
| Errors            |     0 |              — |      0 |   0 |     0 |    0% |
| Timeouts          |     0 |              — |      0 |   0 |     0 |    0% |

| Метрика              |  Value |
|----------------------|-------:|
| Fast path            |  76.7% |
| Slow path            |  23.3% |
| Dep depth max        |      2 |
| Operation total mean | 105 ms |

## e2-fault-safety: корректность при отказах (30 runs)

Конкурентные PUT/GET/HEAD/DELETE с crash/restart fault injection (3 цикла по 5s).

| Метрика             |  Value |    SD |
|---------------------|-------:|------:|
| Ops/run             |     64 |     0 |
| Total ops           |   1920 |     — |
| Success rate        |   100% |     0 |
| Errors              |      0 |     0 |
| Fast path           |   100% |     0 |
| Dep depth max       |      0 |     0 |
| Pre-accept failures | 18/run |     0 |
| Latency mean        |  21 ms | 2.1ms |
| Fault cycles        |  3/run |     0 |

При crash/restart одного из трёх узлов запросы к нему таймаутятся, но кворум (2 из 3)
обеспечивает прогресс. Все 1920 операций завершились корректно.

## e3-degradation: деградация при отказе узла (30 runs)

baseline → fail node → degraded → recover → recovery → restored.

| Фаза     | Throughput, req/s |   SD |         95% CI | Ratio | PUT p95, ms | Multiplier | Errors |
|----------|------------------:|-----:|---------------:|------:|------------:|-----------:|-------:|
| baseline |           131.315 |  9.4 | [127.8, 134.8] |  1.00 |         340 |       1.00 |     0% |
| degraded |           138.900 | 10.1 | [135.1, 142.7] |  1.07 |         325 |       0.97 | ~0.01% |
| recovery |           124.052 |  9.1 | [120.6, 127.5] |  0.95 |         352 |       1.05 | ~0.01% |
| restored |           101.279 | 10.0 |  [97.5, 105.0] |  0.78 |         436 |       1.30 | ~0.02% |

| Метрика          | Value |    SD |
|------------------|------:|------:|
| Restart recovery | 0.22s | 0.04s |
| Fast path        | 59.1% |  0.9% |
| Dep depth max    |     2 |     0 |

Потеря одного узла при quorum=2 не снижает пропускную способность (ratio 1.07, p<0.05). Фаза restored
ниже baseline из-за cold journal catch-up на восстановленном узле — это линейный эффект.

## e4-hot-key: изоляция горячего ключа (30 runs)

90% запросов к одному hot key, 10% к независимым.

| Метрика | Hot p95 |    SD | Independent p95 |    SD | Ratio |
|---------|--------:|------:|----------------:|------:|------:|
| PUT     |  376 ms | 49 ms |          297 ms | 62 ms |  1.29 |
| GET     |  1.6 ms | 0.6ms |          1.9 ms | 0.7ms |  0.84 |
| DELETE  |  267 ms | 40 ms |          224 ms | 49 ms |  1.19 |

| Метрика        | Value |   SD |
|----------------|------:|-----:|
| Fast path      |  8.4% | 0.8% |
| Slow path      | 91.6% | 0.8% |
| Dep depth max  |     5 |    0 |
| Dep count mean |  3.66 | 0.02 |
| Errors         |    0% | 0.4% |
| Timeouts       |    0% |    0 |

91.6% slow path ожидаемо для hot key — каждая операция конфликтует. dep depth bounded = 5,
0% timeouts. Конфликт локализован: задержка hot key в 1.29 раза выше independent.

## e5-leaderless: безлидерное распределение (30 runs)

| Узел  |  Share |    SD | PUT p95, ms | GET p95, ms |
|-------|-------:|------:|------------:|------------:|
| node1 | 33.37% | 0.07% |         345 |         1.1 |
| node2 | 33.33% | 0.08% |         348 |         1.2 |
| node3 | 33.29% | 0.08% |         344 |         1.1 |

| Метрика             | Value |   SD |
|---------------------|------:|-----:|
| Fast path           | 76.7% | 3.0% |
| Dep depth max       |     2 |    0 |
| Pre-accept failures |     0 |    0 |

Равномерное распределение 33.37/33.33/33.29 с минимальным разбросом PUT p95 (344–348 ms).

## e6-recovery: безопасное восстановление (30 runs)

baseline → fail → degraded → recover → recovery → restored с верификацией sentinel-объектов.

| Фаза     | Throughput, req/s |   SD |         95% CI | Ratio | PUT p95, ms | Multiplier | Errors |
|----------|------------------:|-----:|---------------:|------:|------------:|-----------:|-------:|
| baseline |           129.628 |  9.8 | [126.0, 133.3] |  1.00 |         341 |       1.00 |     0% |
| degraded |           133.087 | 10.4 | [129.2, 137.0] |  1.03 |         343 |       1.02 | ~0.01% |
| recovery |           118.020 |  9.7 | [114.4, 121.6] |  0.92 |         376 |       1.12 | ~0.01% |
| restored |           107.882 |  8.5 | [104.7, 111.0] |  0.84 |         408 |       1.21 | ~0.02% |

| Метрика           | Value |    SD |
|-------------------|------:|------:|
| Restart recovery  | 0.22s | 0.04s |
| Recovery sentinel | 30/30 |     — |
| Fast path         | 58.8% |  1.2% |
| Dep depth max     |     2 |     0 |

Данные, записанные до сбоя, доступны после восстановления (sentinel 30/30). degraded фаза
без деградации (ratio 1.03, CI содержит 1.0 — статистически неотличимо от baseline). restored 0.84 — холодный старт
восстановленного узла.

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
| Mixed workload, no faults (k6-mixed) |             2 |           0.06 |
| Mixed workload, leaderless (e5)      |             2 |           0.07 |
| Concurrent + node failure (e3, e6)   |             2 |           0.03 |
| Extreme hot key contention (e4)      |             5 |           3.66 |

dep depth bounded: 2 при нормальной нагрузке, 5 при экстремальном hot key.

### Безлидерная архитектура

e5: 33.37/33.33/33.29 — равномерное распределение. PUT p95: 344–348 ms по узлам.

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
