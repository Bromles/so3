# SO3 client history schema

`client-history.jsonl` is an append-only JSON Lines file. Each line describes one client-visible S3 operation issued by
the correctness driver.

Required fields:

- `schema_version`: currently `1`.
- `operation_id`: unique operation id inside one run.
- `idempotency_key`: string when supported, otherwise `null`.
- `operation_type`: `PUT`, `GET`, `HEAD` or `DELETE`.
- `key`: S3 object key.
- `input_value_hash`: SHA-256 hex of the uploaded payload for `PUT`, otherwise `null`.
- `returned_value_hash`: for `GET` — SHA-256 hex of the returned body; for `HEAD` — ETag stripped of quotes; `null` for
  other operations or when unavailable.
- `observed_version`: value of `x-amz-version-id` when the server returns it.
- `etag`: S3 ETag when returned.
- `start_timestamp` / `end_timestamp`: UTC timestamps.
- `start_monotonic_secs` / `end_monotonic_secs`: local monotonic clock values for ordering intervals.
- `latency_ms`: operation latency.
- `entry_node`: node through which the client entered the cluster.
- `endpoint`: endpoint URL used by the S3 SDK.
- `result_code`: HTTP result code observed through the S3 SDK.
- `success`: whether the S3 SDK operation completed with a 2xx result.
- `timeout`: whether the operation timed out.
- `error` / `error_code`: SDK error details when available.
- `client`: currently `boto3` for Python correctness scenarios.
- `api`: currently `s3`.

Unsupported checks such as CAS, `If-None-Match` and idempotency are reported by the verifier as `unsupported` until the
S3 API exposes those semantics.
