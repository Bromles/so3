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
    - `scripts/requirements.txt` содержит общие зависимости (`psutil`, `boto3`);
    - `scripts/venv/` используется как общий локальный venv для `scripts/research`, `scripts/verify` и legacy k6 runner;
    - venv больше не находится внутри `scripts/k6/`.
- Реализован общий research runner `scripts/research/run-scenario.py`:
    - поддерживает `--runs` с default `30`;
    - запрещает `runs < 30` без `--allow-low-runs`;
    - создает единый каталог результата;
    - пишет `manifest.json`, `events.jsonl`, `summary.json`, `aggregate-summary.json`, `report.md`;
    - поддерживает `k6-mixed`, `e1-correctness`, `e3-degradation`, `e4-hot-key`, `e5-leaderless`, `e6-recovery` как
      CLI-сценарии;
    - для `e3-degradation` и `e6-recovery` умеет выполнять phase-aware crash/restart timeline:
      `baseline -> fail -> degraded -> recover -> recovery -> restored`.
- Реализованы базовые модули research harness:
    - `cluster.py` — start/stop/kill/restart узлов, wait-ready, resource sampling через обязательный `psutil`;
    - `topology.py` — генерация 1/3/5/7-node topology, ports, node ids, peer lists, data dirs;
    - `manifest.py` — manifest и event timeline;
    - `metrics.py` — нормализация k6 summary;
    - `stats.py` — descriptive statistics и aggregate summary;
    - `report.py` — markdown report;
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
      `get_object`, `head_object`, `delete_object`;
    - пишет `client-history.jsonl` с `client: "boto3"` и `api: "s3"`.
- Добавлен verifier:
    - `scripts/verify/verify-history.py` — CLI;
    - `scripts/verify/verify_history.py` — импортируемый verifier;
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

Частично сделано / остается доработать:

- В `stats.py` есть базовая агрегация числовых метрик и phase-aware aggregation для `baseline`, `degraded`,
  `recovery`, `restored`; доверительные интервалы пока не добавлены.
- Fault/recovery сценарии уже имеют базовую phase-aware оркестрацию crash/restart и normalized phase-vs-baseline
  метрики; hot-key/leaderless сценарии пока в основном представлены workload-файлами и CLI aliases.
- Server-side observability (`fast/slow/recovery path`, conflicts, dependencies, quorum wait и т.п.) еще не реализована.
- Maelstrom hidden-leader behavior исправлен для обычных client requests: узел, получивший клиентскую операцию,
  координирует ее локально и пишет structured `tracing` log через подключенный logger с `entry_node`,
  `coordinator_node`, `operation_id`, `operation`, `source`, `consensus_path`.

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
    - `scenarios/` — запланированные отдельные модули сценариев; пока содержит только `__init__.py`,
      логика всех сценариев находится в `run-scenario.py`:
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

Статус: в основном сделано. Базовый `stats.py`, `aggregate-summary.json` и markdown-таблица в `report.md` реализованы;
добавлены отдельные `phase_metrics` и `relative_metrics` для `baseline`, `degraded`, `recovery`, `restored`.

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
PUT/GET-ответов.

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

Статус: базовый hidden-leader path исправлен. Обычные client requests больше не форвардятся на первый узел; каждый
Maelstrom-узел использует свой локальный `AccordConsensusCoordinatorService`. Coordination observability пишется через
структурированный `tracing::info!` лог с `entry_node`, `coordinator_node`, `operation_id`, `operation`, `source`,
`consensus_path`. Осталось расширить maelstrom scripts для 30-run fault/leaderless сценариев и majority/minority
partition checks.

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

## Этап 6. Реализовать E1. Correctness under concurrency

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

Статус: частично сделано. `run-scenario.py e3-degradation` выполняет последовательность фаз с crash/restart выбранного
узла, пишет отдельные `k6-summary-<phase>.json`, timeline events и normalized phase-vs-baseline метрики. Остается
расширить stabilization/recovery-time анализ и конфигурационные matrix-runs по 3/5/7 узлам.

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

Статус: частично сделано. `run-scenario.py e6-recovery` использует тот же phase-aware crash/restart runner с
persistent data dirs внутри прогона и отдельными фазами `degraded`, `recovery`, `restored`. Остается добавить сценарии
долгого отставания, падения во время синхронизации и verifier-проверку сохранности подтвержденных записей.

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

## Этап 14. Обновить документацию

Работы:

1. Обновить `docs/results.md` или добавить отдельный research-results документ.
2. Описать методику запуска каждого сценария.
3. Описать формат результатов.
4. Описать интерпретацию статистики.
5. Описать ограничения:
    - какие проверки не покрыты;
    - какие операции пока не поддерживаются S3 API;
    - какие fault scenarios проверяются только через Maelstrom;
    - какие сценарии требуют proxy-based network partition.

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
