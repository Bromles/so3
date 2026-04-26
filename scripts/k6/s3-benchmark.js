/**
 * so3 S3-compatible API benchmark
 *
 * Uses the official k6 AWS S3Client (Grafana jslib) for realistic S3 wire
 * compatibility testing.  Reports per-operation avg / median / std-dev /
 * variance / IQR / p90 / p95.
 *
 * Environment variables
 * ─────────────────────
 * SO3_ADDR              Server base URL          (default: http://127.0.0.1:3000)
 * SO3_BUCKET            Bucket name              (default: bench)
 * OBJECT_SIZE           Payload size in bytes    (default: 64)
 * VUS                   Virtual users            (default: 10)
 * DURATION              Steady-state duration    (default: 30s)
 * AWS_REGION            Fake region for signing  (default: us-east-1)
 * AWS_ACCESS_KEY_ID     Fake key id              (default: testkey)
 * AWS_SECRET_ACCESS_KEY Fake secret              (default: testsecret)
 *
 * Single run:
 *   k6 run scripts/k6/s3-benchmark.js
 *
 * Multi-run (30 iterations, aggregate statistics):
 *   bash scripts/k6/run-benchmark.sh --runs 30
 */

// Use local copy of the official Grafana jslib
import { AWSConfig, S3Client } from "./lib/s3.js";
import http from "k6/http";
import { check } from "k6";
import { Trend, Rate } from "k6/metrics";
import { randomBytes } from "k6/crypto";

// ── configuration ─────────────────────────────────────────────────────────────

const BASE_URL = __ENV.SO3_ADDR || "http://127.0.0.1:3000";
const BUCKET = __ENV.SO3_BUCKET || "bench";
const OBJECT_SIZE = parseInt(__ENV.OBJECT_SIZE || "64", 10);

const awsConfig = new AWSConfig({
  region: __ENV.AWS_REGION || "us-east-1",
  accessKeyId: __ENV.AWS_ACCESS_KEY_ID || "so3testkey000000",
  secretAccessKey:
    __ENV.AWS_SECRET_ACCESS_KEY || "so3testsecret0000000000000000000",
  endpoint: BASE_URL,
});

export const options = {
  // Include enough percentile points for IQR-based std-dev estimation.
  summaryTrendStats: [
    "avg",
    "med",
    "p(10)",
    "p(25)",
    "p(75)",
    "p(90)",
    "p(95)",
    "p(99)",
    "min",
    "max",
  ],
  vus: parseInt(__ENV.VUS || "10", 10),
  duration: __ENV.DURATION || "30s",
  thresholds: {
    s3_errors: ["rate<0.01"],
  },
};

// ── custom metrics ─────────────────────────────────────────────────────────────

// milliseconds, isTime=true so k6 formats them correctly
const putMs = new Trend("s3_put_ms", true);
const getMs = new Trend("s3_get_ms", true);
const headMs = new Trend("s3_head_ms", true); // plain http HEAD (not in jslib S3Client)
const deleteMs = new Trend("s3_delete_ms", true);
const s3Errors = new Rate("s3_errors");

// ── S3 client ─────────────────────────────────────────────────────────────────

// One client instance per VU (module-level init runs once per VU).
const s3 = new S3Client(awsConfig);

// ── main scenario ─────────────────────────────────────────────────────────────

export default async function () {
  // Each VU rotates through 100 object keys to exercise the consistency path.
  const key = `vu${__VU}/obj${__ITER % 100}`;
  const body = randomBytes(OBJECT_SIZE);

  // ── PUT ──────────────────────────────────────────────────────────────────────
  let t = Date.now();
  try {
    await s3.putObject(BUCKET, key, body);
    putMs.add(Date.now() - t);
    s3Errors.add(false);
  } catch (e) {
    putMs.add(Date.now() - t);
    s3Errors.add(true);
    // Skip GET/HEAD for this key — object may not exist yet.
    return;
  }

  // ── GET (read-after-write consistency check) ──────────────────────────────
  // Notes on S3Client response fields (k6 jslib 0.12.x):
  //   obj.etag       – undefined when server sends lowercase "etag" header;
  //                    jslib reads "ETag" case-sensitively from the raw map.
  //   obj.lastModified – Unix timestamp in ms (number), not a Date object.
  //   obj.size       – Content-Length (number).
  //   obj.data       – response body as string.
  t = Date.now();
  try {
    const obj = await s3.getObject(BUCKET, key);
    getMs.add(Date.now() - t);

    check(obj, {
      "get: object returned": (o) => o !== null && o !== undefined,
      // etag may be undefined due to header casing; guard before access.
      "get: etag present or server omits it": (o) =>
        o.etag === undefined ||
        (typeof o.etag === "string" && o.etag.length > 0),
      "get: lastModified positive": (o) =>
        typeof o.lastModified === "number" && o.lastModified > 0,
      "get: size matches upload": (o) => o.size === OBJECT_SIZE,
    });
    s3Errors.add(false);
  } catch (e) {
    getMs.add(Date.now() - t);
    s3Errors.add(true);
  }

  // ── HEAD (metadata-only via plain http) ────────────────────────────────────
  // headObject is not part of the k6 jslib S3Client API; we use k6's http
  // module directly and verify the S3 response headers that so3 sets.
  const objectUrl = `${BASE_URL}/${BUCKET}/${key}`;
  t = Date.now();
  {
    const res = http.head(objectUrl, { tags: { operation: "head" } });
    headMs.add(Date.now() - t);
    const ok = check(res, {
      "head: status 200": (r) => r.status === 200,
      "head: content-length matches upload": (r) =>
        parseInt(r.headers["Content-Length"] || "0", 10) === OBJECT_SIZE,
      "head: etag present": (r) => r.headers["Etag"] !== undefined,
      "head: etag is quoted": (r) => (r.headers["Etag"] || "").startsWith('"'),
      "head: last-modified present": (r) =>
        r.headers["Last-Modified"] !== undefined,
      "head: x-amz-version-id present": (r) =>
        r.headers["X-Amz-Version-Id"] !== undefined,
    });
    s3Errors.add(!ok);
  }

  // ── DELETE ────────────────────────────────────────────────────────────────
  t = Date.now();
  try {
    await s3.deleteObject(BUCKET, key);
    deleteMs.add(Date.now() - t);
    s3Errors.add(false);
  } catch (e) {
    deleteMs.add(Date.now() - t);
    s3Errors.add(true);
  }
}

// ── summary ───────────────────────────────────────────────────────────────────

/**
 * Compute descriptive statistics for a Trend metric.
 *
 * Standard deviation is estimated from the inter-quartile range using the
 * Gaussian IQR estimator: σ ≈ IQR / 1.3490  (IQR = Q3 − Q1, where Q3/Q1 are
 * the 75th/25th percentiles).  This is robust against outliers and requires no
 * raw sample access.  Variance is σ².
 *
 * For accurate cross-run statistics run with --summary-export and aggregate
 * with scripts/k6/run-benchmark.sh.
 */
function computeStats(metricKey, data) {
  const m = data.metrics[metricKey];
  if (!m) return null;
  const v = m.values;

  const q1 = v["p(25)"] ?? 0;
  const q3 = v["p(75)"] ?? 0;
  const iqr = q3 - q1;
  // IQR / (2 * Φ⁻¹(0.75)) = IQR / 1.34897950...
  const stdDev = iqr / 1.3489795003921634;
  const variance = stdDev * stdDev;

  return {
    avg: v.avg ?? 0,
    med: v.med ?? 0,
    stdDev,
    variance,
    iqr,
    p90: v["p(90)"] ?? 0,
    p95: v["p(95)"] ?? 0,
    p99: v["p(99)"] ?? 0,
    min: v.min ?? 0,
    max: v.max ?? 0,
  };
}

function fmtRow(label, s) {
  if (!s) return `  ${label.padEnd(7)}: no data`;
  return (
    `  ${label.padEnd(7)}:` +
    `  avg=${s.avg.toFixed(1).padStart(7)}` +
    `  med=${s.med.toFixed(1).padStart(7)}` +
    `  σ=${s.stdDev.toFixed(1).padStart(7)}` +
    `  var=${s.variance.toFixed(1).padStart(9)}` +
    `  IQR=${s.iqr.toFixed(1).padStart(7)}` +
    `  p90=${s.p90.toFixed(1).padStart(7)}` +
    `  p95=${s.p95.toFixed(1).padStart(7)}` +
    `  ms`
  );
}

export function handleSummary(data) {
  const put = computeStats("s3_put_ms", data);
  const get = computeStats("s3_get_ms", data);
  const head = computeStats("s3_head_ms", data);
  const del = computeStats("s3_delete_ms", data);

  const httpReqs = data.metrics.http_reqs?.values.count ?? 0;
  const durSec = (data.state.testRunDurationMs ?? 0) / 1000;
  const errRate = data.metrics.s3_errors?.values.rate ?? 0;

  const header =
    "            " +
    "     avg" +
    "      med" +
    "        σ" +
    "       var" +
    "      IQR" +
    "      p90" +
    "      p95";

  const lines = [
    "",
    "── so3 S3 benchmark ─────────────────────────────────────────────────────────",
    `  Throughput : ${(httpReqs / durSec).toFixed(1)} req/s  (${httpReqs} requests in ${durSec.toFixed(1)}s)`,
    `  Error rate : ${(errRate * 100).toFixed(2)}%`,
    "",
    header,
    fmtRow("PUT", put),
    fmtRow("GET", get),
    fmtRow("HEAD", head),
    fmtRow("DELETE", del),
    "",
    "  All times in milliseconds.  σ estimated from IQR (Gaussian IQR estimator).",
    "  For cross-run statistics: bash scripts/k6/run-benchmark.sh --runs 30",
    "",
  ];

  return { stdout: lines.join("\n") };
}
