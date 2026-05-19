# План работ по реализации исследовательских бенчмарков SO3

Этот документ описывает итоговый план переделки скриптов, бенчмарков и вспомогательной инфраструктуры для реализации
`docs/research-plan.md`.

SO3 рассматривается как экспериментальный proof of concept. Поэтому цель бенчмарков — не доказать абсолютное
превосходство по производительности, а показать корректность, безопасность при отказах, предсказуемую деградацию,
отсутствие постоянного лидера и безопасное восстановление.

## Текущий статус реализации

Статус обновлен по текущему состоянию дерева `scripts/`.

Сделано:

- Общая структура Python-инфраструктуры вынесена на уровень `scripts/`:
    - `scripts/requirements.txt` содержит общие зависимости (`psutil`, `boto3`, `numpy`, `scipy`, `matplotlib`);
    - `scripts/venv/` используется как общий локальный venv для `scripts/research`, `scripts/verify` и legacy k6 runner;
    - venv больше не находится внутри `scripts/k6/`.
- Скрипты `scripts/maelstrom/` переписаны с bash/PowerShell на Python:
    - `install.py` — кросс-платформенный установщик Maelstrom;
    - `run.py` — запуск одного Maelstrom теста;
    - `run_30.py` — пакетный запуск 30 раз с агрегацией pass/fail в `aggregate.json` и `report.md`;
      передаёт явный `--data-dir` на каждый прогон, чтобы не накапливать 30 временных директорий;
    - `smoke.py`, `smoke_3node.py`, `fault_3node.py` — convenience wrappers.
- Реализован общий research runner `scripts/research/run-scenario.py`:
    - поддерживает `--runs` с default `30`;
    - запрещает `runs < 30` без `--allow-low-runs`;
    - создает единый каталог результата;
    - пишет `manifest.json`, `events.jsonl`, `summary.json`, `aggregate-summary.json`, `report.md`;
    - поддерживает `k6-mixed`, `e1-correctness`, `e2-fault-safety`, `e3-degradation`, `e4-hot-key`,
      `e5-leaderless`, `e6-recovery` как CLI-сценарии;
    - делегирует каждый сценарий своему модулю в `scenarios/`;
    - для `e3-degradation` и `e6-recovery` умеет выполнять phase-aware crash/restart timeline:
      `baseline -> fail -> degraded -> recover -> recovery -> restored`;
    - `e6-recovery` поддерживает `--e6-long-downtime-secs` для тестирования длительного простоя;
    - пробрасывает `args.entry_urls` из topology и `run_seed` в `run_e6_recovery`;
    - `entry_urls` извлекаются из `topology.to_json()` и сохраняются в `args.entry_urls`;
    - `--e6-re-crash` включает сценарий повторного падения узла во время восстановления;
    - `--e6-re-crash-duration` задаёт длительность degraded-фазы после повторного падения (default `15s`);
    - `--matrix-node-counts` запускает scenario по 3/5/7 узлам с подкаталогами `nodes-{N}/`
      и генерирует `matrix-summary.json` + `matrix-report.md` на уровне родительского каталога.
- Реализованы scenario-модули в `scripts/research/scenarios/`:
    - `e2_fault_safety.py` — CorrectnessDriver + ConcurrentFaultInjector параллельно;
    - `e3_node_degradation.py` — четырёхфазный phased runner с crash/restart; включены потоки
      (`with_stream=True`) для фаз recovery и restored через `run_k6_phase(..., with_stream=True)`;
      `stabilization_secs` вычисляется через `metrics_timeseries.stabilization_time_secs(baseline_rate)`
      и сохраняется в `run_metrics["fault"]["stabilization_secs"]`;
    - `e4_hot_key.py` — k6 с per-tag stream metrics (`key_class`);
    - `e5_leaderless.py` — k6 с per-node entry metrics из stream; symmetry-of-failures покрыт Maelstrom;
    - `e6_recovery.py` — как E3, но с configurable `long_downtime_secs`; принимает `run_seed`;
      пишет `e6-sentinel/key-NNNN` объекты до fault через `RecoverySentinel`, верифицирует их
      после restored-фазы, пишет `verifier-result.json` (schema_version 1, checked/unsupported/issues/verdict)
      и `verifier_passed` (как `1.0`/`0.0` для корректной статистической агрегации) в `run_metrics["fault"]`; включены
      потоки для фаз recovery и restored через
      `run_k6_phase(..., with_stream=True)`; `stabilization_secs` вычисляется через
      `metrics_timeseries.stabilization_time_secs(baseline_rate)` так же, как в E3;
      sentinel-события (`sentinel_write`, `sentinel_verify`) записываются в event log.
- Реализован общий модуль `scripts/research/runner.py`:
    - `run_k6()` — subprocess wrapper для k6;
    - `run_k6_phase()` — фазовый runner с RESEARCH_PHASE/DURATION env vars;
    - импортируется всеми scenario-модулями напрямую.
- Реализованы базовые модули research harness:
    - `cluster.py` — start/stop/kill/restart узлов, wait-ready, resource sampling через `psutil` (прямой import);
    - `topology.py` — генерация 1/3/5/7-node topology, ports, node ids, peer lists, data dirs;
    - `manifest.py` — manifest и event timeline;
    - `metrics.py` — нормализация k6 summary;
    - `metrics_timeseries.py` — per-tag агрегация из k6 JSONL stream; добавлена
      `stabilization_time_secs(stream_path, *, baseline_rate, threshold=0.9, window_secs=5.0)` —
      парсит временные метки Points (RFC3339 с optional nanoseconds через `_parse_ts`),
      разбивает на окна фиксированного размера через `bisect` и возвращает секунды от начала фазы
      до первого окна с request rate ≥ threshold × baseline_rate;
    - `_common.py` — общие helpers `get_nested`, `get_number`, `load_run_summaries`,
      `detect_scenario`, `node_total_samples`, вынесенные из дубликатов в `plot.py` и `report.py`;
    - `stats.py` — descriptive statistics (scipy t.interval CI, numpy percentiles) и aggregate summary;
      дисперсия и stddev считаются с `ddof=1` (выборочные оценки), согласованно с `scipy.stats.sem`;
    - `report.py` — markdown report;
    - `plot.py` — PNG-графики по aggregate/run summaries;
    - `faults.py` — базовые crash/restart primitives, network partition явно `unsupported` до proxy layer.
- Разделены k6 workloads:
    - `scripts/k6/lib/research.js` — общие helpers для endpoint selection, key distribution, tagging и метрик;
    - `scripts/k6/workloads/s3_mixed.js`;
    - `scripts/k6/workloads/s3_hot_key.js`;
    - `scripts/k6/workloads/s3_degradation.js`;
    - `scripts/k6/workloads/s3_leaderless.js`;
    - `scripts/k6/workloads/s3_recovery.js`.
- Добавлен correctness driver через настоящий S3 SDK:
    - `scripts/research/correctness_driver.py` использует `boto3.client("s3")` и реальные S3 calls: `put_object`,
      `get_object`, `head_object`, `delete_object` (прямой import без fallback);
    - пишет `client-history.jsonl` с `client: "boto3"` и `api: "s3"`;
    - счётчик `errors` не учитывает таймауты повторно (они считаются отдельно в `timeouts`);
    - добавлен класс `RecoverySentinel`: пишет детерминированные объекты с префиксом `e6-sentinel/`
      (нет коллизии с k6-ключами `obj/vu.../...`) до fault-события и верифицирует их сохранность
      после восстановления; `write()` возвращает `{key: sha256_hex}` для подтверждённых PUT,
      `verify(confirmed)` читает назад и сравнивает хеши тел; инвариант
      `recovery_preserves_confirmed_writes` проверяется для каждого ключа, результаты включают
      `schema_version`, `checked`, `unsupported`, `confirmed_writes`, `issues` и `verdict`.
- Добавлен verifier:
    - `scripts/verify/verify-history.py` — CLI;
    - `scripts/verify/verify_history.py` — импортируемый verifier; `final_successful_get_is_explainable`
      удалён из `SUPPORTED_INVARIANTS` как незадокументированный дубликат;
    - `scripts/verify/history-schema.md` — схема истории.
- Проверки, которые S3 API пока не выражает (`CAS`, `If-None-Match`, idempotency key), verifier помечает как
  `unsupported`.
- `GET` и `HEAD` проверяются раздельно: для `GET` сравнивается SHA-256 тела с хешами из успешных `PUT`,
  для `HEAD` ETag сравнивается с ETag-ами, наблюдёнными в успешных `PUT`- и `GET`-ответах.
- Проведены smoke-проверки:
    - `ruff check scripts` проходит;
    - Python compile для `scripts/research`, `scripts/verify`, `scripts/k6/run-backend-benchmark.py` проходит;
    - `k6-mixed` smoke через `scripts/venv/bin/python` проходит;
    - `e1-correctness` smoke через `boto3` driver проходит на 1-node и коротком 3-node запуске;
    - `client-history.jsonl`, `resources.jsonl` и `verifier-result.json` создаются.

Остается доработать:

- Server-side observability частично реализована: обычный `so3` пишет structured `tracing` events для
  `fast`/`slow`/`recovery` consensus path, coordinator/origin node, quorum, participating replicas,
  dependency count/depth lower bound, phase timings, quorum wait, retry/commit attempts,
  in-flight operations, pre-accept/accept/recovery counters; research runner парсит их из
  `cluster.log` в `server.consensus.*`. Inbound apply path пишет `apply_backlog` events для reorder buffer,
  dependency wait и apply latency; runner парсит их в `server.apply.*`. Остаются полноценный dependency depth
  по графу зависимостей и более детальные recovery-specific backlog breakdowns.
- Maelstrom hidden-leader behavior исправлен для обычных client requests: узел, получивший клиентскую операцию,
  координирует ее локально и пишет structured `tracing` log через подключенный logger с `entry_node`,
  `coordinator_node`, `operation_id`, `operation`, `source`, `consensus_path`.
- E6: сценарий повторного падения узла во время синхронизации реализован через флаг `--e6-re-crash`:
  после начальной recovery-фазы узел снова «убивается» (`re_crash`), выполняется
  `re_crash_degraded`-фаза (длительность через `--e6-re-crash-duration`, default `15s`),
  затем вторичный restart (`re_recovery`) и финальная `re_restored`-фаза;
  метрики `re_crash_downtime_secs`, `re_crash_recovery_seconds`, `re_crash_stabilization_secs`
  записываются в `run_metrics["fault"]`; sentinel-верификация выполняется после `re_restored`.
- E3/E6: matrix-runs по 3/5/7 узлам реализованы через флаг `--matrix-node-counts`:
  runner последовательно запускает сценарий с `--node-count 3`, `5`, `7`, создавая подкаталоги
  `nodes-3/`, `nodes-5/`, `nodes-7/` внутри result-dir; для каждого подкаталога генерируются
  aggregate-summary, plots и report; на уровне родительского каталога пишутся `matrix-summary.json`
  (per-node-count агрегаты + cross-node comparison) и `matrix-report.md` (сравнительная таблица
  throughput degradation, recovery time, stabilization time, verifier pass rate); поддерживается
  только для `e3-degradation` и `e6-recovery`.

## Обязательные принципы

1. Каждый числовой бенчмарк должен выполняться минимум 30 раз.
2. Для всех числовых результатов нужно считать:
    - среднее значение;
    - медиану;
    - минимум и максимум;
    - дисперсию;
    - стандартное отклонение;
    - коэффициент вариации;
    - percentiles, минимум p90, p95 и p99 для latency;
    - доверительный интервал, если размер выборки и распределение позволяют корректно его интерпретировать.
3. Основные выводы должны строиться на относительных метриках:
    - `throughput_scenario / throughput_baseline`;
    - `p95_scenario / p95_baseline`;
    - `p99_scenario / p99_baseline`;
    - `successful_ops / attempted_ops`;
    - `timeouts / attempted_ops`;
    - hot-key latency относительно independent-key latency;
    - recovery latency относительно baseline latency.
4. Абсолютные числа сохраняются для воспроизводимости, но не используются как продуктовые заявления.
5. Результаты одиночного прогона не считаются достаточными для выводов.
6. Correctness и safety нельзя доказывать только через throughput/latency. Для них нужна история операций и верификатор
   инвариантов.
7. Инфраструктуру нельзя складывать в один большой файл. Нужно разделить сценарии, запуск кластера, fault injection,
   сбор метрик, агрегацию и построение отчетов.

## Целевая структура файлов

Рекомендуемая структура:

- `scripts/`
    - `requirements.txt` — общие Python-зависимости для research, verify и legacy k6 runner;
    - `venv/` — локальное виртуальное окружение для всех Python-скриптов из `scripts/`; каталог не является частью
      исходного кода.
- `scripts/research/`
    - `run-scenario.py` — главный CLI для запуска исследовательских сценариев;
    - `cluster.py` — запуск, остановка, restart и kill узлов SO3;
    - `topology.py` — генерация 1-, 3-, 5- и 7-node конфигураций;
    - `faults.py` — node crash, restart, partition, heal, delayed recovery;
    - `metrics.py` — сбор и нормализация client/server/resource метрик;
    - `correctness_driver.py` — драйвер correctness-сценариев через настоящий S3 SDK (`boto3`);
    - `stats.py` — статистическая агрегация по 30+ прогонам;
    - `manifest.py` — описание прогона, seed, окружение, версии бинарей;
    - `report.py` — генерация markdown/JSON summaries;
    - `plot.py` — генерация PNG-графиков для reports;
    - `runner.py` — общий `run_k6()` и `run_k6_phase()`, импортируется scenario-модулями;
    - `scenarios/` — отдельные модули сценариев:
        - `e1_correctness.py`;
        - `e2_fault_safety.py`;
        - `e3_node_degradation.py`;
        - `e4_hot_key.py`;
        - `e5_leaderless.py`;
        - `e6_recovery.py`.
- `scripts/k6/`
    - `workloads/`
        - `s3_mixed.js`;
        - `s3_hot_key.js`;
        - `s3_degradation.js`;
        - `s3_leaderless.js`;
        - `s3_recovery.js`.
    - `lib/`
        - текущий `s3.js` оставить как S3-клиент;
        - добавить общие helpers для key distribution, tagging и endpoint selection.
- `scripts/verify/`
    - `verify-history.py` — CLI для object-level и cluster-level инвариантов;
    - `verify_history.py` — импортируемый модуль verifier для `run-scenario.py`;
    - `history-schema.md` — формат истории операций.
- `results/research/`
    - каталог для воспроизводимых результатов;
    - каждый сценарий сохраняет отдельный подкаталог с manifest, raw history, raw k6 exports, time series и summary.

Текущий `scripts/k6/run-backend-benchmark.py` не нужно бесконечно расширять. Его лучше использовать как основу, но
разнести ответственность по отдельным модулям.

## Формат результатов каждого сценария

Каждый сценарий должен сохранять отдельный каталог результата:

- `manifest.json`:
    - имя сценария;
    - номер прогона;
    - общий seed;
    - topology;
    - число узлов;
    - workload mix;
    - object size;
    - длительность фаз;
    - binary path;
    - git revision, если доступен;
    - параметры fault injection;
    - адреса узлов.
- `events.jsonl`:
    - `run_start`;
    - `baseline_start`;
    - `baseline_end`;
    - `fail`;
    - `partition`;
    - `heal`;
    - `recover`;
    - `degraded_start`;
    - `recovery_start`;
    - `normal_restored`;
    - `run_end`.
- `client-history.jsonl`:
    - требуется для correctness/safety сценариев;
    - содержит каждую клиентскую операцию.
- `k6-summary.json` или несколько summary-файлов:
    - raw export каждого k6-прогона.
- `client-timeseries.jsonl`:
    - latency, throughput, success/error/timeout по времени.
- `server-timeseries.jsonl`:
    - consensus path, quorum wait, conflicts, dependencies, retries, recovery backlog.
- `resources.tsv` или `resources.jsonl`:
    - CPU/RSS/network по каждому узлу.
- `summary.json`:
    - агрегированные метрики одного прогона.
- `aggregate-summary.json`:
    - статистика по 30+ прогонам.
- `report.md`:
    - человекочитаемый отчет.

## Статистическая агрегация

Для каждого сценария нужно выполнять минимум 30 независимых прогонов.

Агрегация считается не только внутри одного k6 run, но и между прогонами. Например, если каждый прогон дает
`p95 latency`, то итоговая таблица должна содержать статистику по 30 значениям `p95 latency`.

Для каждого числового показателя считать:

- `n`;
- `mean`;
- `median`;
- `stddev`;
- `variance`;
- `min`;
- `max`;
- `p10`;
- `p25`;
- `p75`;
- `p90`;
- `p95`;
- `p99`, если достаточно точек;
- `cv_percent = stddev / mean * 100`, если mean не равен нулю.

Для относительных метрик считать те же статистики:

- throughput ratio;
- p95 latency multiplier;
- p99 latency multiplier;
- success ratio;
- timeout ratio;
- hot/independent key ratio;
- recovery/baseline ratio.

Если часть прогонов завершилась ошибкой, это не нужно скрывать. В summary должны быть:

- число успешных прогонов;
- число failed runs;
- причины failed runs;
- отдельно статистика только по successful runs;
- общий verdict сценария.

## Этап 1. Подготовить общий research runner

Статус: в основном сделано. `run-scenario.py`, `cluster.py`, `topology.py`, manifest/events, `--runs` и guard для
low-runs реализованы. Полноценные scenario-specific phase runners для всех E-сценариев еще развиваются поверх общего
harness.

Цель: заменить набор разрозненных benchmark-скриптов единым, но модульным research harness.

Работы:

1. Создать `scripts/research/`.
2. Вынести управление SO3-кластером из `scripts/k6/run-backend-benchmark.py` в отдельный модуль `cluster.py`.
3. Добавить поддержку cluster size:
    - 1 node;
    - 3 nodes;
    - 5 nodes;
    - 7 nodes, если протокол и окружение это поддерживают.
4. Убрать hardcode 3-node topology.
5. Генерировать:
    - node id;
    - object ports;
    - RPC ports;
    - peer lists;
    - per-node data dirs.
6. Добавить lifecycle-команды:
    - start node;
    - stop node;
    - kill node;
    - restart node with same data dir;
    - wait ready;
    - cleanup.
7. Добавить общий формат manifest и event timeline.
8. Добавить режим `--runs`, default не меньше 30 для исследовательских сценариев.
9. Добавить проверку, что сценарии с числовыми результатами не запускаются с `runs < 30` без явного `--allow-low-runs`
   для отладки.

Результат этапа: можно запускать любой сценарий 30+ раз и получать единый каталог результатов.

## Этап 2. Реализовать статистический модуль

Статус: сделано. `stats.py` использует `numpy`/`scipy`: scipy t.interval для 95% CI, numpy percentiles; добавлены
`phase_metrics` и `relative_metrics` для `baseline`, `degraded`, `recovery`, `restored`; `report.md` содержит
markdown-таблицу с CI-колонкой.

Цель: все числовые результаты агрегируются одинаково и воспроизводимо.

Работы:

1. Создать `scripts/research/stats.py`.
2. Реализовать функции для:
    - mean;
    - median;
    - variance;
    - stddev;
    - min/max;
    - percentiles;
    - coefficient of variation;
    - ratios and multipliers.
3. Добавить агрегацию по run summaries.
4. Добавить отдельную агрегацию по фазам:
    - baseline;
    - degraded;
    - recovery;
    - restored.
5. Добавить экспорт `aggregate-summary.json`.
6. Добавить markdown-таблицы для `report.md`.

Результат этапа: любой сценарий автоматически получает статистически пригодный summary по минимум 30 прогонам.

## Этап 3. Разделить k6 workloads

Статус: сделано для базового набора workload-файлов. Добавлены `scripts/k6/lib/research.js` и workloads `s3_mixed.js`,
`s3_hot_key.js`, `s3_degradation.js`, `s3_leaderless.js`, `s3_recovery.js`; smoke-запуски проходят. Старый
`s3-benchmark.js` оставлен как legacy benchmark.

Цель: текущий `s3-benchmark.js` перестает быть единственным сценарием и превращается в набор читаемых workload-файлов.

Работы:

1. Оставить общий S3 client в `scripts/k6/lib/s3.js`.
2. Добавить common helpers:
    - выбор entry node;
    - генерация ключей;
    - uniform distribution;
    - hot key distribution;
    - 90/10 distribution;
    - Zipf distribution;
    - tagging metrics.
3. Разбить workload scripts:
    - mixed S3 операции;
    - degradation workload;
    - hot key workload;
    - leaderless workload;
    - recovery workload.
4. Добавить tags к k6 metrics:
    - `scenario`;
    - `operation`;
    - `entry_node`;
    - `key_class`;
    - `phase`;
    - `status`.
5. Для fault-сценариев убрать жесткий threshold `s3_errors < 0.01` как критерий падения запуска.
6. Ошибки под отказами считать как данные:
    - success ratio;
    - timeout ratio;
    - error ratio.

Результат этапа: k6 покрывает разные workload patterns и дает пригодные tagged metrics.

## Этап 4. Добавить correctness driver и verifier

Статус: частично сделано. Добавлен `correctness_driver.py` на реальном S3 SDK (`boto3`), `client-history.jsonl`,
`verify-history.py`, импортируемый `verify_history.py` и `history-schema.md`; `e1-correctness` подключен к
`run-scenario.py`. CAS/`If-None-Match`/idempotency пока маркируются как `unsupported`; E2 safety history еще предстоит
расширить. `GET` и `HEAD` верифицируются раздельно: `GET` — через SHA-256 тела, `HEAD` — через ETag из
PUT/GET-ответов. Добавлен класс `RecoverySentinel` для инварианта `recovery_preserves_confirmed_writes`: пишет
детерминированные sentinel-объекты до fault-события, верифицирует их после recovery. Используется в E6.

Цель: E1 и часть E2 проверяются не latency-графиками, а историей операций и инвариантами.

Работы:

1. Создать отдельный correctness driver.
2. Driver должен выполнять:
    - concurrent `PUT` по разным ключам;
    - concurrent overwrite одного ключа;
    - concurrent `PUT`/`DELETE`;
    - `GET`/`HEAD` во время записей;
    - CAS-like операции;
    - retries после timeout, если idempotency будет поддержана.
3. Каждая операция пишет в `client-history.jsonl`:
    - operation id;
    - idempotency key, если есть;
    - operation type;
    - key;
    - input value hash;
    - returned value hash;
    - observed version;
    - start timestamp;
    - end timestamp;
    - entry node;
    - result code;
    - timeout/error.
4. Создать `scripts/verify/verify-history.py`.
5. Проверять object-level инварианты:
    - `GET` не возвращает значение, которое никогда не было успешно записано;
    - после successful `PUT` видна записанная или более поздняя версия;
    - после successful `DELETE` старая версия не возвращается как текущая;
    - конфликтующие successful writes имеют объяснимый порядок;
    - CAS успешен только при совпадении версии;
    - `if-none-match` успешен только при отсутствии объекта;
    - повтор с тем же idempotency key не создает независимое второе изменение;
    - после recovery подтвержденные записи остаются видимыми.
6. Если S3 API пока не поддерживает условные операции или idempotency, помечать эти проверки как `unsupported`, а не как
   passed.

Результат этапа: E1 получает строгий verifier, а не только k6 latency.

## Этап 5. Исправить Maelstrom path для leaderless-проверок

Статус: в основном сделано. Скрипты `scripts/maelstrom/` переписаны на Python. `run_30.py` запускает Maelstrom 30 раз
и пишет `aggregate.json` + `report.md` с pass_rate и per-run verdict. `fault_3node.py` использует `nemesis=partition`
и покрывает partition/leaderless symmetry. Hidden-leader path исправлен: каждый узел координирует локально.
Остается: majority/minority partition split через отдельный nemesis config.

Цель: Maelstrom не должен скрыто превращать систему в leader-based вариант.

Проблема была в том, что `so3-maelstrom` форвардил client operations на первый узел. Для проверки отсутствия
постоянного лидера это некорректно.

Работы:

1. Изменить client path в `so3-maelstrom` так, чтобы каждый узел мог сам координировать клиентскую операцию.
2. Убрать обязательное forwarding на `node_ids[0]` для обычных client requests.
3. Оставить peer RPC для consensus/blob обмена.
4. Добавить метрики или логи:
    - entry node;
    - coordinator node;
    - operation id;
    - consensus path.
5. Обновить Maelstrom scripts:
    - baseline linearizable run;
    - partition majority/minority;
    - repeated 30-run mode;
    - fault safety scenario;
    - 3-node и 5-node режимы.

Результат этапа: Maelstrom можно использовать для E2 и E5 без искажения архитектурного свойства leaderless.

## Этап 6. E1. Correctness under concurrency → Maelstrom

Статус: покрыт Maelstrom. Линеаризуемость проверяется через `scripts/maelstrom/run_30.py`
(30 прогонов, `--nemesis partition`) и `smoke_3node.py` (без фейлов). Knossos checker даёт
формально строгую гарантию linearizability. Отдельный boto3 correctness driver для E1 удалён —
S3 API-level correctness (PUT/GET/DELETE семантика) покрывает E2 fault safety driver под нагрузкой.

Цель: доказать object-level correctness без fault injection.

Сценарии:

1. concurrent `PUT` по разным ключам;
2. concurrent overwrite одного ключа;
3. concurrent `PUT`/`DELETE`;
4. `GET`/`HEAD` во время записей;
5. условные операции и CAS-like сценарии;
6. retry после timeout, если idempotency key поддерживается.

Требования:

- минимум 30 прогонов;
- каждый прогон сохраняет историю операций;
- verifier запускается после каждого прогона;
- aggregate summary считает долю passed/failed runs;
- числовые метрики latency/success также агрегируются со stddev/variance.

Критерий успеха: все supported object-level инварианты проходят во всех успешных прогонах.

## Этап 7. Реализовать E2. Fault safety

Статус: базовый crash/restart вариант реализован в `scenarios/e2_fault_safety.py`.
`ConcurrentFaultInjector` крашит и рестартует узлы round-robin пока работает `CorrectnessDriver`.
Partition и heal через реальный кластер не реализованы — для этого нужен proxy-слой; для проверки partition
использовать Maelstrom (`fault-3-node-lin-kv.sh`).

Цель: доказать, что crash, restart и partition не ломают safety.

Сценарии:

1. crash coordinator во время операции;
2. crash replica до commit;
3. crash replica после commit;
4. crash after commit before client ack, если можно воспроизвести;
5. restart узла;
6. network partition majority/minority;
7. heal partition.

Требования:

- минимум 30 прогонов на каждый fault scenario;
- история операций обязательна;
- verifier проверяет отсутствие противоречивых committed states;
- minority partition не должна принимать unsafe writes;
- после heal partition реплики должны сходиться.

Инструменты:

- Maelstrom для partition/fault safety;
- real SO3 cluster runner для crash/restart;
- proxy-based network fault layer для real cluster partitions, если нужно проверять именно S3-кластер.

Критерий успеха: safety-инварианты не нарушаются ни в одном supported сценарии.

## Этап 8. Реализовать E3. Degradation under node failures

Статус: в основном сделано. `run-scenario.py e3-degradation` выполняет последовательность фаз с crash/restart
выбранного узла, пишет отдельные `k6-summary-<phase>.json`, timeline events и normalized phase-vs-baseline
метрики. Фазы recovery и restored записывают k6 JSONL stream через `run_k6_phase(..., with_stream=True)`;
`stabilization_secs` вычисляется через `metrics_timeseries.stabilization_time_secs(stream, baseline_rate)`
с порогом 0.9×baseline и окном 5 сек, и сохраняется в `run_metrics["fault"]["stabilization_secs"]`.
Остается: провести полные matrix-runs через `--matrix-node-counts`.

Цель: показать предсказуемую динамику при отказах узлов.

Сценарий одного прогона:

1. start cluster;
2. warmup;
3. baseline phase;
4. fail one node;
5. degraded steady-state phase;
6. restart failed node;
7. recovery phase;
8. restored normal phase;
9. stop cluster;
10. save summary.

Конфигурации:

- 3-node;
- 5-node;
- 7-node, если поддерживается.

Требования:

- минимум 30 прогонов на каждую node-count конфигурацию;
- считать normalized throughput и latency multipliers;
- считать stabilization time;
- считать recovery time;
- считать variance между прогонами.

Критерий успеха: деградация повторяемая и объяснимая, нет зависших навсегда операций при наличии кворума.

## Этап 9. Реализовать E4. Hot key conflict behavior

Статус: частично сделано. Workload `s3_hot_key.js` и CLI alias `e4-hot-key` реализованы.
Добавлена per-tag агрегация через `metrics_timeseries.py`: `hot_vs_independent_p95_ratio`
и `key_class_metrics` (hot / independent) пишутся в `run_metrics` через k6 `--out json` stream.

Цель: проверить, что конкуренция за горячий ключ деградирует локально, а не валит весь кластер.

Сценарии:

1. uniform key distribution;
2. 100% writes в один объект;
3. 90% writes в один hot object и 10% independent keys;
4. Zipf distribution;
5. CAS storm по одной версии, если условные операции доступны.

Требования:

- минимум 30 прогонов на каждый distribution scenario;
- отдельно считать hot-key metrics и independent-key metrics;
- считать hot/independent latency ratio;
- считать conflict count, retry count, dependency depth, fast/slow path ratio, если серверные метрики доступны.

Критерий успеха: hot key деградирует объяснимо, independent keys продолжают работать без глобального коллапса.

## Этап 10. Реализовать E5. Leaderless behavior

Статус: частично сделано. Workload `s3_leaderless.js` и `scenarios/e5_leaderless.py` реализованы.
Per-node entry metrics (`entry_node_metrics`) через k6 `--out json` stream добавлены в `run_metrics`.
Symmetry of failures покрыта Maelstrom (`fault_3node.py` / `run_30.py`) — Knossos checker проверяет
linearizability при partition по всем узлам, что является более строгой гарантией.

Цель: показать, что нет постоянного лидера как единственной точки координации.

Сценарии:

1. клиенты подключаются к разным узлам;
2. операции координируются разными узлами;
3. каждый узел по очереди выключается;
4. сравнивается degradation factor при отказе каждого узла.

Требования:

- минимум 30 прогонов для отказа каждого узла;
- per-node entry metrics;
- per-node coordinator metrics;
- сравнение degradation factor по узлам;
- отсутствие election pause;
- Maelstrom path не должен форвардить все операции на первый узел.

Критерий успеха: нет узла, отказ которого статистически и принципиально хуже остальных как отказ постоянного лидера.

## Этап 11. Реализовать E6. Recovery and lagging node

Статус: в основном сделано. `scenarios/e6_recovery.py` реализован: phase-aware crash/restart runner с
`--e6-long-downtime-secs` для тестирования длительного простоя; принимает `run_seed` и `entry_urls`.
Фазы recovery и restored записывают k6 JSONL stream через `run_k6_phase(..., with_stream=True)`;
`stabilization_secs` вычисляется через `metrics_timeseries.stabilization_time_secs(stream, baseline_rate)`.
`RecoverySentinel` из `correctness_driver.py` пишет `e6-sentinel/key-NNNN` объекты до fault-события,
верифицирует их после restored-фазы, пишет `verifier-result.json` (schema_version 1, инвариант
`recovery_preserves_confirmed_writes`, verdict passed/failed) и `verifier_passed` в `run_metrics["fault"]`.
Sentinel-события (`sentinel_write`, `sentinel_verify`) записываются в event log.
Сценарий повторного падения реализован через `--e6-re-crash`: после начальной recovery-фазы
узел снова падает (`re_crash`), выполняется `re_crash_degraded`-фаза, затем вторичный restart
(`re_recovery`) и финальная `re_restored`-фаза; метрики `re_crash_downtime_secs`,
`re_crash_recovery_seconds`, `re_crash_stabilization_secs` записываются в `run_metrics["fault"]`.
Остается: интеграция с matrix-runs для полного покрытия 3/5/7 узлов.

Цель: проверить безопасное возвращение отставшего узла.

Сценарии:

1. узел выключен на короткое время;
2. узел выключен на долгое время;
3. узел возвращается под нагрузкой;
4. узел снова падает во время синхронизации.

Требования:

- минимум 30 прогонов на каждый recovery scenario;
- persistent data dirs обязательны;
- verifier проверяет, что подтвержденные записи не потеряны;
- verifier проверяет, что старое состояние не перезаписывает новое;
- считать recovery time;
- считать latency/success impact во время recovery;
- собирать recovery backlog/lag, если сервер это экспортирует.

Критерий успеха: lagging node догоняет состояние безопасно, а восстановление оказывает измеримый, но ограниченный эффект
на клиентские операции.

## Этап 12. Добавить observability на стороне сервера

Цель: результаты должны объяснять, почему система перешла на fast path, slow path или recovery path.

Нужно логировать или экспортировать как метрики:

- coordinator node;
- operation id;
- phase timings:
    - `PreAccept`;
    - `Accept`;
    - `Commit`;
    - `Apply`;
    - `Recover`.
- quorum wait time;
- fast path / slow path / recovery path;
- conflict count;
- dependency count;
- dependency depth;
- retry count;
- participating replicas;
- commit result;
- apply result;
- in-flight operations;
- recovery backlog;
- per-node request count.

Без этих метрик можно получить графики latency/throughput, но нельзя полноценно выполнить `Observability` из
research-plan.

## Этап 13. Реализовать отчеты и графики

Статус: частично реализовано в `scripts/research/report.py` и `scripts/research/plot.py`.

Цель: каждый сценарий должен давать готовые данные для отчета.

Нужные графики:

1. Хронология отказа:
    - normalized p95/p99 latency;
    - normalized throughput;
    - отметки `fail`, `degraded`, `recover`, `restored`.
2. Повторяемость:
    - 30 прогонов одного сценария на одном графике или summarized bands.
3. Hot key isolation:
    - hot-key latency против independent-key latency.
4. Symmetry of failures:
    - degradation factor при отказе каждого узла.
5. Recovery:
    - accumulated lag;
    - success ratio;
    - latency during recovery.
6. Accord path metrics:
    - fast path ratio;
    - slow path ratio;
    - recovery path ratio;
    - conflicts;
    - dependency depth;
    - retries.

Отчеты должны явно разделять:

- raw absolute values;
- normalized values;
- statistical confidence across 30+ runs;
- verifier verdicts;
- unsupported checks.

Реализовано сейчас:

- `report.md` добавляет scenario-specific секции:
    - для `e3-degradation`/`e6-recovery` — compact phase summary, normalized phase-vs-baseline summary, fault timing
      (включая `stabilization_secs`), verifier pass rate (для E6, доля passed/total) и re-crash timing
      (`re_crash_downtime_secs`, `re_crash_recovery_seconds`, `re_crash_stabilization_secs` при наличии);
    - для `e4-hot-key` — comparison hot vs independent key class и explicit p95 ratio;
    - для `e5-leaderless` — per-node entry distribution.
- `plot.generate_plots()` создает `plots/*.png`:
    - `repeatability.png` для доступных per-run метрик;
    - `phases.png` для E3/E6 normalized phase behavior;
    - `timeline.png` для E3/E6 fault timeline с normalized throughput, put p95/p99 и event markers;
    - `symmetry.png` для E3/E6 symmetry-of-failures при `--fault-node-policy round_robin`;
    - `recovery.png` для E6 recovery behavior: success ratio и put p95/p99 latency по фазам;
    - `accord_paths.png` для server-side fast/slow/recovery consensus path ratios;
    - `hot_key.png` для E4;
    - `nodes.png` для E5.
- `run-scenario.py` вызывает генерацию графиков перед записью `report.md`, чтобы отчет мог ссылаться на созданные PNG.
- `run-scenario.py` поддерживает `--fault-node-policy round_robin` для E3/E6, чтобы один result-dir покрывал отказы
  разных узлов и строил `symmetry.png`.
- Обычный `so3` пишет structured `tracing` logs `coordination_event="consensus_operation"`; research runner парсит
  `cluster.log` в `server.consensus.*` metrics и добавляет server-side consensus section в `report.md`. Сейчас в summary
  попадают path ratios, operation/node distribution, quorum/replica counts, dependency count/depth lower bound,
  phase timings, quorum wait, retry/commit attempts, in-flight operations и pre-accept/accept/recovery counters.
  Inbound apply path дополнительно пишет `coordination_event="apply_backlog"`; runner агрегирует reorder buffer size,
  blocking earlier commands, pending dependency count, reorder/dependency wait и apply timings в `server.apply.*`, а
  `report.md` добавляет server-side apply backlog section.

Остается:

- полноценный dependency depth по графу зависимостей;
- более детальный recovery-specific backlog/lag breakdown, если потребуется отделять общий apply backlog от recovery.

## Этап 14. Обновить документацию

Статус: сделано.

- Создан `docs/research-results.md` (886 строк) — полное руководство по исследовательской инфраструктуре:
  быстрый старт, описание каждого сценария, формат результатов (структура каталогов, JSON-схемы),
  интерпретация метрик (нормализация, стабилизация, статистическая агрегация), верификатор корректности
  (инварианты, unsupported проверки), server-side observability (консенсус, apply backlog), описание всех
  8 типов графиков, matrix-runs по 3/5/7 узлам, ограничения (CAS, partition, proxy) и примеры команд.
- Обновлён `docs/results.md`: раздел «Следующие результаты, которые нужны для PoC» заменён на таблицу
  соответствия между прежними плановыми отчётами и реализованными командами `run-scenario.py`;
  добавлена ссылка на `docs/research-results.md`.

## Минимальная последовательность реализации

Рекомендуемый порядок, чтобы не смешать инфраструктурные изменения с экспериментами:

1. Создать модульную структуру `scripts/research/`.
2. Реализовать общий формат результатов и statistics aggregation.
3. Разнести k6 workloads по отдельным файлам.
4. Добавить correctness driver и history verifier.
5. Исправить Maelstrom hidden-leader behavior.
6. Реализовать E1.
7. Реализовать E2.
8. Реализовать E3.
9. Реализовать E4.
10. Реализовать E5.
11. Реализовать E6.
12. Добавить server observability.
13. Добавить графики и markdown reports.
14. Обновить документацию по запуску и интерпретации.

## Definition of Done

План считается реализованным, когда:

1. Каждый сценарий запускается минимум 30 раз по умолчанию.
2. Все числовые результаты имеют variance/stddev и другие базовые статистики.
3. Correctness/safety сценарии имеют raw history и verifier verdict.
4. Fault/degradation сценарии имеют timeline событий.
5. Все ключевые выводы формулируются через normalized metrics.
6. Hot-key результаты отделены от independent-key результатов.
7. Leaderless-проверки не используют скрытый постоянный coordinator.
8. Recovery-сценарии используют persistent node data dirs.
9. Результаты можно воспроизвести по manifest.
10. Код сценариев разделен на читаемые модули, а не собран в один большой файл.
