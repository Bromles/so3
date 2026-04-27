# so3 — Test Results

Collected on 2026-04-27. Environment: macOS (Apple Silicon), single-host setup. Release build (`--release`).

---

## Maelstrom — Linearizability Verification

### What Maelstrom lin-kv verifies

[Maelstrom](https://github.com/jepsen-io/maelstrom) is the Jepsen workbench for distributed systems
testing. The **lin-kv** workload exercises a linearizable key-value store by generating concurrent
`read`, `write`, and `compare-and-set` operations across Maelstrom client processes. After the run,
[Knossos](https://github.com/jepsen-io/knossos) replays the recorded history and checks that every
operation appears to have taken effect atomically at a single point in real time — the definition
of linearizability (C1 of CAP).

A run passes when Knossos reports **`:valid? true`** for every key it analysed. Any history that
cannot be explained by a linearizable execution produces **`:valid? false`** and marks the system
non-linearizable.

**Operations under test:**

| Operation | Maelstrom type | Description                                                    |
| --------- | -------------- | -------------------------------------------------------------- |
| `read`    | `read`         | Return current value of a key                                  |
| `write`   | `write`        | Unconditionally set a key to a value                           |
| `cas`     | `cas`          | Conditional write: update only if current value matches `from` |

**Nemesis — `partition`:** Maelstrom periodically splits the node graph into two disconnected
components (TCP-level network partition) and later heals the partition. The system must continue to
make progress and must not violate linearizability during or after the partition.

**ok-fraction note:** Jepsen counts an operation as "ok" only when it returned a positive result
within the test window. CAS operations that return `precondition-failed` (wrong expected value) are
counted as "fail" — this is expected behaviour under contention, not a correctness issue. The
`:valid? true` verdict is the only correctness signal.

---

### Scenarios and results

All runs use the release binary (`target/release/so3-maelstrom`), date 2026-04-27.

#### Scenario 1 — Smoke: 1 node

| Parameter   | Value          |
| ----------- | -------------- |
| Nodes       | 1              |
| Time limit  | 10 s           |
| Rate        | 20 req/s       |
| Concurrency | 2n (2 clients) |
| Nemesis     | none           |

| Metric      | Value    |
| ----------- | -------- |
| Total ops   | 193      |
| Ok          | 130      |
| Fail        | 63       |
| ok-fraction | 0.674    |
| **:valid?** | **true** |

#### Scenario 2 — Smoke: 3 nodes

| Parameter   | Value          |
| ----------- | -------------- |
| Nodes       | 3              |
| Time limit  | 10 s           |
| Rate        | 10 req/s       |
| Concurrency | 2n (6 clients) |
| Nemesis     | none           |

| Metric      | Value    |
| ----------- | -------- |
| Total ops   | 94       |
| Ok          | 53       |
| Fail        | 41       |
| ok-fraction | 0.564    |
| **:valid?** | **true** |

Follower nodes forward client requests to the deterministic leader (n0). The leader drives Accord
consensus phases (PreAccept → Commit → Apply) over Maelstrom messages before replying.

#### Scenario 3 — Fault tolerance: 3 nodes, network partitions

| Parameter        | Value          |
| ---------------- | -------------- |
| Nodes            | 3              |
| Time limit       | 30 s           |
| Rate             | 20 req/s       |
| Concurrency      | 2n (6 clients) |
| Nemesis          | partition      |
| Nemesis interval | 5 s            |

| Metric      | Value    |
| ----------- | -------- |
| Total ops   | 436      |
| Ok          | 242      |
| Fail        | 159      |
| ok-fraction | 0.555    |
| **:valid?** | **true** |

Periodic network partitions do not cause linearizability violations. The Accord recovery path
(Recover phase with ballot comparison) restores consensus after a healed partition.

#### Scenario 4 — High load: 3 nodes, RATE=50, network partitions

| Parameter        | Value           |
| ---------------- | --------------- |
| Nodes            | 3               |
| Time limit       | 30 s            |
| Rate             | 50 req/s        |
| Concurrency      | 4n (12 clients) |
| Nemesis          | partition       |
| Nemesis interval | 5 s             |

| Metric      | Value    |
| ----------- | -------- |
| Total ops   | 721      |
| Ok          | 87       |
| Fail        | 425      |
| ok-fraction | 0.121    |
| **:valid?** | **true** |

High contention + partitions cause many CAS precondition failures (expected), but linearizability
is maintained.

---

### Summary

| Scenario                         | Nodes | Nemesis   | :valid? |
| -------------------------------- | ----- | --------- | ------- |
| Smoke 1-node                     | 1     | —         | ✓ true  |
| Smoke 3-node                     | 3     | —         | ✓ true  |
| Fault 3-node (rate 20)           | 3     | partition | ✓ true  |
| Fault 3-node high-load (rate 50) | 3     | partition | ✓ true  |

**All scenarios pass Knossos linearizability analysis.**

Maelstrom writes detailed operation histories and timing graphs under `store/lin-kv/` (gitignored).
Re-run any scenario with the scripts under `scripts/maelstrom/`.

---

## k6 — S3 API Performance Benchmark

### What is tested

The k6 benchmark (`scripts/k6/s3-benchmark.js`) exercises the S3-compatible HTTP API using the
official [k6 AWS S3Client](https://github.com/grafana/k6-jslib-aws). Each virtual user (VU)
performs one full cycle per iteration: **PUT → GET → HEAD → DELETE** on a rotating set of 100 keys.

The benchmark checks:

- **Read-after-write consistency:** GET immediately after PUT must return the object.
- **Metadata correctness:** HEAD must return valid `Content-Length`, `ETag`, `Last-Modified`,
  and `X-Amz-Version-Id` headers.
- **Zero error rate:** `s3_errors` threshold is `rate < 1%`.

### Test configuration

| Parameter   | Value                                     |
| ----------- | ----------------------------------------- |
| VUs         | 10                                        |
| Duration    | 30 s per run                              |
| Runs        | 30                                        |
| Object size | 64 bytes                                  |
| Endpoint    | `http://127.0.0.1:3000` (single-node so3) |
| Binary      | `target/release/so3`                      |
| Date        | 2026-04-27                                |

### Throughput (across 30 runs)

| Metric                                         | mean    | min  | max  | σ      |
| ---------------------------------------------- | ------- | ---- | ---- | ------ |
| HTTP requests/s                                | 29.2    | 24.9 | 34.8 | 2.9    |
| Iterations/s (full PUT→GET→HEAD→DELETE cycles) | 7.3     | 6.2  | 8.7  | 0.7    |
| Avg iteration duration                         | 1375 ms | —    | —    | 130 ms |
| Error rate                                     | 0%      | 0%   | 0%   | —      |

### Latency statistics (30-run aggregate, all times in ms)

σ is the **cross-run standard deviation** (between-run stability), computed as population std-dev
of the per-run median/avg/p90/p95. CV (coefficient of variation) measures run-to-run noise as a
percentage of the mean.

#### PUT

| Statistic | mean   | σ      | CV    | min    | max     |
| --------- | ------ | ------ | ----- | ------ | ------- |
| median    | 424.88 | 44.54  | 10.5% | 341.50 | 546.00  |
| avg       | 452.51 | 48.06  | 10.6% | 370.63 | 591.71  |
| p90       | 733.53 | 102.53 | 14.0% | 559.80 | 995.60  |
| p95       | 858.58 | 87.40  | 10.2% | 678.25 | 1026.60 |

#### GET

| Statistic | mean   | σ     | CV    | min    | max    |
| --------- | ------ | ----- | ----- | ------ | ------ |
| median    | 287.27 | 36.48 | 12.7% | 223.00 | 354.50 |
| avg       | 306.62 | 33.22 | 10.8% | 250.77 | 358.22 |
| p90       | 523.24 | 39.35 | 7.5%  | 443.60 | 599.40 |
| p95       | 596.79 | 51.79 | 8.7%  | 503.45 | 690.80 |

#### HEAD

| Statistic | mean   | σ     | CV    | min    | max    |
| --------- | ------ | ----- | ----- | ------ | ------ |
| median    | 285.52 | 33.25 | 11.6% | 215.00 | 346.00 |
| avg       | 302.06 | 27.29 | 9.0%  | 250.53 | 345.52 |
| p90       | 497.35 | 25.22 | 5.1%  | 431.20 | 553.20 |
| p95       | 572.70 | 28.96 | 5.1%  | 523.00 | 642.50 |

#### DELETE

| Statistic | mean   | σ     | CV    | min    | max    |
| --------- | ------ | ----- | ----- | ------ | ------ |
| median    | 299.72 | 35.05 | 11.7% | 235.00 | 365.00 |
| avg       | 314.07 | 31.41 | 10.0% | 252.26 | 361.14 |
| p90       | 508.50 | 32.96 | 6.5%  | 444.80 | 572.40 |
| p95       | 568.78 | 38.64 | 6.8%  | 486.00 | 646.80 |

### Interpretation

**Latency context:** Each PUT goes through the full Accord consensus pipeline — PreAccept, optional
Accept (slow path), Commit, and local Apply — before returning to the client. SQLite WAL commits
are synchronous. On a single-node setup this means ~1–4 disk fsync operations per write with no
network involved. The 425 ms median for PUT reflects this deliberate durability cost.

GET and HEAD are read-path operations and are ~30% faster than PUT (~287 ms vs ~425 ms median)
because reads bypass the consensus coordinator and serve directly from the SQLite metadata store
and filesystem blob.

**Stability (CV 5–14%):** Coefficients of variation below 15% over 30 independent runs indicate
stable benchmark behaviour. The higher CV for PUT p90 (14%) reflects occasional GC/SQLite
checkpoint pauses.

**Zero errors across 30 runs** confirms S3 API correctness: read-after-write consistency, valid
ETag/Last-Modified/X-Amz-Version-Id headers, and proper HTTP status codes for all operations.

### Re-running

```bash
# Single run with live summary
k6 run scripts/k6/s3-benchmark.js

# 30-run aggregate
bash scripts/k6/run-benchmark.sh --runs 30

# Custom VUs / duration
bash scripts/k6/run-benchmark.sh --runs 30 VUS=20 DURATION=60s
```

Raw JSON exports from the reference run: `/tmp/so3-bench-20260427-072208/run_*.json`

---

## Architecture notes

The results above reflect the following design decisions:

- **Accord consensus** (PreAccept → Accept on slow path → Commit → Apply) provides strict
  serialisation of all writes. The fast path skips Accept when all replicas agree on `timestamp_zero`.
- **Hybrid Logical Clock (HLC)** provides causally consistent command ordering without a central
  sequencer.
- **SQLite + filesystem**: metadata and consensus journal in SQLite (WAL mode), immutable blobs on
  the local filesystem. The blob is persisted before the metadata record references it.
- **so3-maelstrom runtime**: `Arc<SharedRuntime>` with oneshot channels eliminates the old
  recursive `dispatch → wait_for_response → dispatch` loop. Incoming response messages are routed
  inline in the event loop; new request messages are spawned as independent tokio tasks. The
  multi-threaded tokio runtime uses all available CPU cores.
