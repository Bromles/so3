/**
 * Create an S3 bucket for external S3-compatible benchmark backends.
 *
 * This helper is intentionally separate from s3-benchmark.js so bucket setup
 * requests do not pollute the benchmark's exported metrics.
 */

import {AWSConfig, S3Client} from "./lib/s3.js";
import http from "k6/http";

const BASE_URL = __ENV.SO3_ADDR || "http://127.0.0.1:3000";
const BUCKET = __ENV.SO3_BUCKET || "bench";

const awsConfig = new AWSConfig({
    region: __ENV.AWS_REGION || "us-east-1",
    accessKeyId: __ENV.AWS_ACCESS_KEY_ID || "so3testkey000000",
    secretAccessKey:
        __ENV.AWS_SECRET_ACCESS_KEY || "so3testsecret0000000000000000000",
    endpoint: BASE_URL,
});

const s3 = new S3Client(awsConfig);

export const options = {
    vus: 1,
    iterations: 1,
};

export default async function () {
    const signed = s3.signature.sign(
        {
            method: "PUT",
            endpoint: s3.endpoint,
            path: `/${BUCKET}`,
            headers: {Host: s3.endpoint.host},
            body: "",
        },
        {},
    );

    const res = http.put(signed.url, signed.body || null, {
        headers: signed.headers,
    });

    if (res.status < 200 || (res.status >= 300 && res.status !== 409)) {
        throw new Error(
            `create bucket ${BUCKET} failed: HTTP ${res.status}: ${res.body}`,
        );
    }

    const key = `__benchmark_setup_smoke_${Date.now()}`;
    const body = "ready";
    await s3.putObject(BUCKET, key, body);
    const obj = await s3.getObject(BUCKET, key);
    if (obj.size !== body.length) {
        throw new Error(`setup smoke GET size mismatch: got ${obj.size}`);
    }
    await s3.deleteObject(BUCKET, key);
}
