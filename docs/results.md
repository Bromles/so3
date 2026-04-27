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

In the Maelstrom adapter, client requests arriving at non-coordinator nodes are forwarded to `n0`,
which runs the Accord consensus phases (PreAccept → Commit → Apply) over Maelstrom messages before
replying. This forwarding is an artifact of the adapter's simplified stdin/stdout transport — in
the production `so3` binary any node acts as coordinator for its own requests.

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

### Scaling model

so3 is a **replicated** system — every node stores a full copy of all data. Adding nodes increases
**fault tolerance** (the cluster can lose `floor((n-1)/2)` nodes and remain available), but does
**not** increase write throughput or total storage capacity. Write scaling would require key-space
sharding, which is not implemented in the current PoC.

Reads can in principle be served locally from any node without consensus, so read throughput _can_
scale with the number of nodes (each node serves its own read traffic independently). The current
implementation reads from the local SQLite store without a consensus round trip.

### Test configurations

Two configurations were benchmarked for comparison:

| Configuration  | Nodes | Endpoint                               |
| -------------- | ----- | -------------------------------------- |
| Single node    | 1     | `127.0.0.1:3000` (standalone)          |
| 3-node cluster | 3     | `127.0.0.1:3001` (one node in cluster) |

Common parameters for both:

| Parameter   | Value                |
| ----------- | -------------------- |
| VUs         | 10                   |
| Duration    | 30 s per run         |
| Runs        | 30                   |
| Object size | 64 bytes             |
| Binary      | `target/release/so3` |
| Date        | 2026-04-27           |

### Throughput comparison (across 30 runs)

| Metric          | Single node | 3-node cluster | Δ    |
| --------------- | ----------- | -------------- | ---- |
| HTTP req/s mean | 29.2        | 21.3           | −27% |
| HTTP req/s σ    | 2.9         | 1.3            |      |
| Iterations/s    | 7.3         | 5.3            | −27% |
| Error rate      | 0%          | 0%             | —    |

Throughput drops ~27% in the cluster because each write now requires two extra consensus round
trips (PreAccept to 2 peers + Commit to 2 peers) over loopback gRPC before the client response.
Reads at the median are unaffected (local SQLite read, no consensus), but at higher percentiles
queuing behind writes adds latency.

### Single-node latency statistics (30-run aggregate, all times in ms)

Two levels of variability are reported:

- **σ_cross** — cross-run standard deviation: population std-dev of the per-run statistic across
  the 30 runs. Measures run-to-run stability (scheduling noise, SQLite checkpoint timing, etc.).
- **σ_within** — within-run standard deviation: estimated from the IQR of each run using the
  Gaussian IQR estimator σ ≈ IQR / 1.3490 (robust against outliers). The **mean** σ_within and
  **variance** (σ_within²) across all 30 runs are reported. Measures tail latency spread inside a
  single 30 s window.
- **CV** — coefficient of variation = σ_cross / mean, measures run-to-run noise as a percentage.

#### PUT

| Statistic | mean   | σ_cross | CV    | min    | max     |
| --------- | ------ | ------- | ----- | ------ | ------- |
| median    | 424.88 | 44.54   | 10.5% | 341.50 | 546.00  |
| avg       | 452.51 | 48.06   | 10.6% | 370.63 | 591.71  |
| p90       | 733.53 | 102.53  | 14.0% | 559.80 | 995.60  |
| p95       | 858.58 | 87.40   | 10.2% | 678.25 | 1026.60 |

| Within-run (mean over 30 runs) | σ_within | variance (ms²) |
| ------------------------------ | -------- | -------------- |
| mean σ_within                  | 183.6 ms | 35 238         |
| min σ_within (best run)        | 123.2 ms | 15 188         |
| max σ_within (worst run)       | 261.3 ms | 68 282         |

#### GET

| Statistic | mean   | σ_cross | CV    | min    | max    |
| --------- | ------ | ------- | ----- | ------ | ------ |
| median    | 287.27 | 36.48   | 12.7% | 223.00 | 354.50 |
| avg       | 306.62 | 33.22   | 10.8% | 250.77 | 358.22 |
| p90       | 523.24 | 39.35   | 7.5%  | 443.60 | 599.40 |
| p95       | 596.79 | 51.79   | 8.7%  | 503.45 | 690.80 |

| Within-run (mean over 30 runs) | σ_within | variance (ms²) |
| ------------------------------ | -------- | -------------- |
| mean σ_within                  | 168.6 ms | 28 784         |
| min σ_within (best run)        | 133.4 ms | 17 805         |
| max σ_within (worst run)       | 209.0 ms | 43 701         |

#### HEAD

| Statistic | mean   | σ_cross | CV    | min    | max    |
| --------- | ------ | ------- | ----- | ------ | ------ |
| median    | 285.52 | 33.25   | 11.6% | 215.00 | 346.00 |
| avg       | 302.06 | 27.29   | 9.0%  | 250.53 | 345.52 |
| p90       | 497.35 | 25.22   | 5.1%  | 431.20 | 553.20 |
| p95       | 572.70 | 28.96   | 5.1%  | 523.00 | 642.50 |

| Within-run (mean over 30 runs) | σ_within | variance (ms²) |
| ------------------------------ | -------- | -------------- |
| mean σ_within                  | 150.7 ms | 22 957         |
| min σ_within (best run)        | 117.1 ms | 13 718         |
| max σ_within (worst run)       | 195.7 ms | 38 300         |

#### DELETE

| Statistic | mean   | σ_cross | CV    | min    | max    |
| --------- | ------ | ------- | ----- | ------ | ------ |
| median    | 299.72 | 35.05   | 11.7% | 235.00 | 365.00 |
| avg       | 314.07 | 31.41   | 10.0% | 252.26 | 361.14 |
| p90       | 508.50 | 32.96   | 6.5%  | 444.80 | 572.40 |
| p95       | 568.78 | 38.64   | 6.8%  | 486.00 | 646.80 |

| Within-run (mean over 30 runs) | σ_within | variance (ms²) |
| ------------------------------ | -------- | -------------- |
| mean σ_within                  | 144.5 ms | 21 109         |
| min σ_within (best run)        | 105.3 ms | 11 081         |
| max σ_within (worst run)       | 172.0 ms | 29 578         |

### Resource consumption

Measured during a k6 run (10 VUs, 30 s, 64-byte objects), sampled every second via `ps`.

| Resource             | Idle     | Under load    | Peak     |
| -------------------- | -------- | ------------- | -------- |
| RSS (resident set)   | ~12.8 MB | ~12.8–13.1 MB | ~13.1 MB |
| CPU (`%cpu`, 1 core) | ~0%      | mean ~70%     | ~99%     |

**Memory** stays effectively flat because:

- SQLite stores all persistent data on disk; the page cache is bounded by SQLite defaults.
- Rust's ownership model avoids runtime GC pauses and heap fragmentation.
- Immutable blobs are addressed by hash and never loaded into the server heap during
  metadata-only operations (HEAD).

**CPU** peaks near 100% of one core for PUT-heavy workloads, which is expected: each write
serialises through the consensus pipeline (PreAccept → Commit → Apply → SQLite WAL fsync) on a
single logical path. The multi-threaded tokio runtime distributes concurrent reads and independent
consensus coordinators across cores.

### 3-node cluster latency statistics (30-run aggregate, all times in ms)

#### PUT (cluster)

| Statistic | mean    | σ_cross | CV    | min     | max     |
| --------- | ------- | ------- | ----- | ------- | ------- |
| median    | 649.32  | 70.14   | 10.8% | 538.00  | 924.50  |
| avg       | 733.39  | 53.08   | 7.2%  | 654.11  | 926.47  |
| p90       | 1305.54 | 175.05  | 13.4% | 1047.20 | 1723.60 |
| p95       | 1869.13 | 137.36  | 7.3%  | 1632.55 | 2336.75 |

#### GET (cluster)

| Statistic | mean   | σ_cross | CV    | min    | max     |
| --------- | ------ | ------- | ----- | ------ | ------- |
| median    | 278.70 | 41.50   | 14.9% | 189.00 | 391.00  |
| avg       | 360.17 | 36.16   | 10.0% | 293.13 | 463.62  |
| p90       | 741.08 | 69.38   | 9.4%  | 606.10 | 908.50  |
| p95       | 885.30 | 80.65   | 9.1%  | 756.50 | 1101.55 |

#### HEAD (cluster)

| Statistic | mean   | σ_cross | CV    | min    | max     |
| --------- | ------ | ------- | ----- | ------ | ------- |
| median    | 300.70 | 42.54   | 14.1% | 222.00 | 381.00  |
| avg       | 370.66 | 35.02   | 9.4%  | 314.49 | 486.76  |
| p90       | 751.49 | 67.46   | 9.0%  | 655.10 | 891.60  |
| p95       | 893.51 | 75.96   | 8.5%  | 793.75 | 1080.10 |

#### DELETE (cluster)

| Statistic | mean   | σ_cross | CV    | min    | max     |
| --------- | ------ | ------- | ----- | ------ | ------- |
| median    | 353.08 | 44.13   | 12.5% | 273.00 | 445.00  |
| avg       | 414.67 | 35.98   | 8.7%  | 354.26 | 561.83  |
| p90       | 823.06 | 74.24   | 9.0%  | 676.90 | 968.00  |
| p95       | 947.99 | 91.91   | 9.7%  | 796.50 | 1150.00 |

### Single vs cluster latency comparison (median, ms)

| Operation | Single node | 3-node cluster | Overhead |
| --------- | ----------- | -------------- | -------- |
| PUT       | 424.9       | 649.3          | +53%     |
| GET       | 287.3       | 278.7          | −3%      |
| HEAD      | 285.5       | 300.7          | +5%      |
| DELETE    | 299.7       | 353.1          | +18%     |

PUT overhead (+53%) reflects two loopback gRPC consensus round trips added by the cluster.
GET median is effectively unchanged (−3%): reads are served locally from SQLite without consensus.
HEAD and DELETE show minor overhead from tokio executor contention under increased overall load.

### Interpretation

**Latency context:** Each PUT traverses the full Accord consensus pipeline — PreAccept, optional
Accept (slow path), Commit, and local Apply — before returning to the client. SQLite WAL commits
are synchronous. On a single-node setup this means ~1–4 disk fsync operations per write with no
network round trips. The 425 ms median for PUT reflects this deliberate durability cost. In a
3-node cluster the same pipeline adds 2 loopback gRPC round trips, raising the PUT median to
649 ms.

GET and HEAD are read-path operations and are ~30% faster than PUT on a single node (~287 ms vs
~425 ms median) because reads bypass the consensus coordinator and serve directly from the SQLite
metadata store and filesystem blob.

**Within-run variance (σ_within ~145–184 ms)** is dominated by SQLite WAL checkpoint events that
periodically pause writers. This is normal SQLite behaviour and not specific to so3.

**Cross-run stability (CV 5–15%):** Coefficients of variation below 15% over 30 independent runs
indicate reproducible benchmark behaviour. The higher CV for PUT p90 (14%) reflects the less
predictable tail of SQLite checkpoint pauses.

**Zero errors across all 60 runs** (30 single-node + 30 cluster) confirms S3 API correctness:
read-after-write consistency, valid ETag/Last-Modified/X-Amz-Version-Id headers, and proper HTTP
status codes for all operations.

**Scaling limitations:** so3 is a replication-based system with no key-space sharding. Adding
nodes increases fault tolerance but does not increase write throughput — on the contrary, each
additional replica adds one more consensus round trip per write. Write throughput scales inversely
with cluster size. Read throughput _can_ scale: each node serves reads locally without consensus,
so routing reads to different nodes in parallel would increase aggregate read capacity. Linear
write scaling requires sharding, which is not implemented in the current PoC.

### Re-running

```bash
# Single node
k6 run scripts/k6/s3-benchmark.js

# 30-run aggregate (single node)
bash scripts/k6/run-benchmark.sh --runs 30

# 3-node cluster (start nodes first, then)
SO3_ADDR=http://127.0.0.1:3001 bash scripts/k6/run-benchmark.sh --runs 30
```

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
