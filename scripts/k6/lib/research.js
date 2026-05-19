import http from "k6/http";
import { check } from "k6";
import { randomBytes } from "k6/crypto";
import { Rate, Trend } from "k6/metrics";
import { AWSConfig, S3Client } from "./s3.js";

export const DEFAULT_BUCKET = __ENV.SO3_BUCKET || "bench";
export const DEFAULT_OBJECT_SIZE = parseInt(__ENV.OBJECT_SIZE || "64", 10);
export const DEFAULT_SCENARIO = __ENV.RESEARCH_SCENARIO || "unknown";
export const DEFAULT_PHASE = __ENV.RESEARCH_PHASE || "steady";

const _zipfDefaultSpace = Math.max(1, parseInt(__ENV.KEY_SPACE || "1000", 10));
const _zipfDefaultSkew = parseFloat(__ENV.ZIPF_SKEW || "1.1");
let _zipfDefaultHarmonic = 0;
for (let _i = 1; _i <= _zipfDefaultSpace; _i += 1) {
  _zipfDefaultHarmonic += 1 / Math.pow(_i, _zipfDefaultSkew);
}

export const summaryTrendStats = [
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
];

export const s3PutMs = new Trend("s3_put_ms", true);
export const s3GetMs = new Trend("s3_get_ms", true);
export const s3HeadMs = new Trend("s3_head_ms", true);
export const s3DeleteMs = new Trend("s3_delete_ms", true);
export const s3Errors = new Rate("s3_errors");
export const s3Timeouts = new Rate("s3_timeouts");
export const s3Successes = new Rate("s3_successes");

const defaultThresholds = {
  s3_errors: ["rate<0.01"],
  http_req_failed: ["rate<0.01"],
};

export function researchOptions(defaults = {}) {
  return {
    summaryTrendStats,
    vus: parseInt(__ENV.VUS || `${defaults.vus || 10}`, 10),
    duration: __ENV.DURATION || defaults.duration || "30s",
    thresholds: { ...defaultThresholds, ...(defaults.thresholds || {}) },
  };
}

export function endpointUrls() {
  const configured = __ENV.SO3_ENTRY_URLS || __ENV.SO3_ADDR || "http://127.0.0.1:3000";
  return configured
    .split(",")
    .map((url) => url.trim())
    .filter((url) => url.length > 0);
}

export function awsConfigFor(endpoint) {
  return new AWSConfig({
    region: __ENV.AWS_REGION || "us-east-1",
    accessKeyId: __ENV.AWS_ACCESS_KEY_ID || "so3testkey000000",
    secretAccessKey:
      __ENV.AWS_SECRET_ACCESS_KEY || "so3testsecret0000000000000000000",
    endpoint,
  });
}

export function buildClients() {
  return endpointUrls().map((endpoint, index) => ({
    endpoint,
    entryNode: `node${index + 1}`,
    s3: new S3Client(awsConfigFor(endpoint)),
  }));
}

export function selectClient(clients, policy = __ENV.ENTRY_NODE_POLICY || "round_robin") {
  if (clients.length === 0) {
    throw new Error("no SO3 endpoints configured");
  }
  if (policy === "random") {
    return clients[Math.floor(Math.random() * clients.length)];
  }
  if (policy === "by_vu") {
    return clients[(__VU - 1) % clients.length];
  }
  return clients[(__ITER + __VU - 1) % clients.length];
}

export function baseTags({ scenario = DEFAULT_SCENARIO, operation, entryNode, keyClass, phase = DEFAULT_PHASE, status }) {
  const tags = {
    scenario,
    operation,
    entry_node: entryNode,
    key_class: keyClass,
    phase,
  };
  if (status !== undefined) {
    tags.status = status;
  }
  return tags;
}

export function uniformKey(prefix = "obj", space = parseInt(__ENV.KEY_SPACE || "1000", 10)) {
  const index = Math.floor(Math.random() * Math.max(1, space));
  return {
    key: `${prefix}/vu${__VU}/obj${index}`,
    keyClass: "independent",
  };
}

export function rotatingKey(prefix = "obj", space = parseInt(__ENV.KEY_SPACE || "100", 10)) {
  return {
    key: `${prefix}/vu${__VU}/obj${__ITER % Math.max(1, space)}`,
    keyClass: "independent",
  };
}

export function hotKey(prefix = "hot") {
  return {
    key: `${prefix}/shared-hot-object`,
    keyClass: "hot",
  };
}

export function ninetyTenKey(prefix = "mixed", independentSpace = parseInt(__ENV.KEY_SPACE || "1000", 10)) {
  if (Math.random() < 0.9) {
    return hotKey(prefix);
  }
  return uniformKey(prefix, independentSpace);
}

export function zipfKey(prefix = "zipf", space = _zipfDefaultSpace, skew = _zipfDefaultSkew) {
  const n = Math.max(1, space);
  let harmonic;
  if (n === _zipfDefaultSpace && skew === _zipfDefaultSkew) {
    harmonic = _zipfDefaultHarmonic;
  } else {
    harmonic = 0;
    for (let i = 1; i <= n; i += 1) {
      harmonic += 1 / Math.pow(i, skew);
    }
  }

  let target = Math.random() * harmonic;
  for (let rank = 1; rank <= n; rank += 1) {
    target -= 1 / Math.pow(rank, skew);
    if (target <= 0) {
      return {
        key: `${prefix}/obj${rank - 1}`,
        keyClass: rank === 1 ? "hot" : "independent",
      };
    }
  }

  return {
    key: `${prefix}/obj${n - 1}`,
    keyClass: "independent",
  };
}

export function keyByDistribution(distribution = __ENV.KEY_DISTRIBUTION || "rotating", prefix = "obj") {
  if (distribution === "uniform") {
    return uniformKey(prefix);
  }
  if (distribution === "hot") {
    return hotKey(prefix);
  }
  if (distribution === "90_10" || distribution === "ninety_ten") {
    return ninetyTenKey(prefix);
  }
  if (distribution === "zipf") {
    return zipfKey(prefix);
  }
  return rotatingKey(prefix);
}

export function isTimeoutError(error) {
  const message = `${error && error.message ? error.message : error}`.toLowerCase();
  return message.includes("timeout") || message.includes("deadline") || message.includes("context canceled");
}

export function recordOutcome({ trend, startedAt, ok, timeout, tags }) {
  trend.add(Date.now() - startedAt, tags);
  s3Errors.add(!ok, tags);
  s3Timeouts.add(Boolean(timeout), tags);
  s3Successes.add(Boolean(ok), tags);
}

export async function putObject({ client, bucket = DEFAULT_BUCKET, key, body, tags }) {
  const startedAt = Date.now();
  try {
    await client.s3.putObject(bucket, key, body);
    recordOutcome({ trend: s3PutMs, startedAt, ok: true, timeout: false, tags: { ...tags, status: "success" } });
    return true;
  } catch (error) {
    const timeout = isTimeoutError(error);
    recordOutcome({ trend: s3PutMs, startedAt, ok: false, timeout, tags: { ...tags, status: timeout ? "timeout" : "error" } });
    if ((__ENV.DEBUG_ERRORS || "0") === "1") {
      console.error(`PUT failed for ${key}: ${error && error.message ? error.message : error}`);
    }
    return false;
  }
}

export async function getObject({ client, bucket = DEFAULT_BUCKET, key, expectedSize = DEFAULT_OBJECT_SIZE, tags }) {
  const startedAt = Date.now();
  try {
    const obj = await client.s3.getObject(bucket, key);
    const ok = check(
      obj,
      {
        "get: object returned": (o) => o !== null && o !== undefined,
        "get: etag present or server omits it": (o) =>
          o.etag === undefined || (typeof o.etag === "string" && o.etag.length > 0),
        "get: lastModified positive": (o) =>
          typeof o.lastModified === "number" && o.lastModified > 0,
        "get: size matches upload": (o) => o.size === expectedSize,
      },
      tags,
    );
    recordOutcome({ trend: s3GetMs, startedAt, ok, timeout: false, tags: { ...tags, status: ok ? "success" : "error" } });
    return ok;
  } catch (error) {
    const timeout = isTimeoutError(error);
    recordOutcome({ trend: s3GetMs, startedAt, ok: false, timeout, tags: { ...tags, status: timeout ? "timeout" : "error" } });
    return false;
  }
}

export function headObject({ endpoint, bucket = DEFAULT_BUCKET, key, expectedSize = DEFAULT_OBJECT_SIZE, tags }) {
  const startedAt = Date.now();
  const res = http.head(`${endpoint}/${bucket}/${key}`, { tags });
  const ok = check(
    res,
    {
      "head: status 200": (r) => r.status === 200,
      "head: content-length matches upload": (r) =>
        parseInt(r.headers["Content-Length"] || "0", 10) === expectedSize,
      "head: etag present": (r) => r.headers.Etag !== undefined,
      "head: etag is quoted": (r) => (r.headers.Etag || "").startsWith('"'),
      "head: last-modified present": (r) => r.headers["Last-Modified"] !== undefined,
    },
    tags,
  );
  recordOutcome({ trend: s3HeadMs, startedAt, ok, timeout: false, tags: { ...tags, status: ok ? "success" : "error" } });
  return ok;
}

export async function deleteObject({ client, bucket = DEFAULT_BUCKET, key, tags }) {
  const startedAt = Date.now();
  try {
    await client.s3.deleteObject(bucket, key);
    recordOutcome({ trend: s3DeleteMs, startedAt, ok: true, timeout: false, tags: { ...tags, status: "success" } });
    return true;
  } catch (error) {
    const timeout = isTimeoutError(error);
    recordOutcome({ trend: s3DeleteMs, startedAt, ok: false, timeout, tags: { ...tags, status: timeout ? "timeout" : "error" } });
    return false;
  }
}

export function randomBody(size = DEFAULT_OBJECT_SIZE) {
  return randomBytes(size);
}

export async function putGetHeadDeleteCycle({ clients, scenario, keyInfo, phase = DEFAULT_PHASE, objectSize = DEFAULT_OBJECT_SIZE }) {
  const client = selectClient(clients);
  const tags = baseTags({
    scenario,
    operation: "put",
    entryNode: client.entryNode,
    keyClass: keyInfo.keyClass,
    phase,
  });
  const body = randomBody(objectSize);
  const putOk = await putObject({ client, key: keyInfo.key, body, tags });
  if (!putOk) {
    return;
  }

  await getObject({
    client,
    key: keyInfo.key,
    expectedSize: objectSize,
    tags: baseTags({ scenario, operation: "get", entryNode: client.entryNode, keyClass: keyInfo.keyClass, phase }),
  });
  headObject({
    endpoint: client.endpoint,
    key: keyInfo.key,
    expectedSize: objectSize,
    tags: baseTags({ scenario, operation: "head", entryNode: client.entryNode, keyClass: keyInfo.keyClass, phase }),
  });
  await deleteObject({
    client,
    key: keyInfo.key,
    tags: baseTags({ scenario, operation: "delete", entryNode: client.entryNode, keyClass: keyInfo.keyClass, phase }),
  });
}
