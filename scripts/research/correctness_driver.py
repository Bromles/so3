"""Concurrent correctness workload driver using a real boto3 S3 client."""

from __future__ import annotations

import concurrent.futures
import hashlib
import importlib
import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    boto3: Any | None = importlib.import_module("boto3")
    botocore_client = importlib.import_module("botocore.client")
    botocore_exceptions = importlib.import_module("botocore.exceptions")
    Config: Any | None = botocore_client.Config
    BotoCoreError: type[Exception] = botocore_exceptions.BotoCoreError
    ClientError: type[Exception] = botocore_exceptions.ClientError
except ImportError:  # pragma: no cover - environment-specific failure
    boto3 = None
    Config = None
    BotoCoreError = Exception
    ClientError = Exception

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"
DEFAULT_REGION = "us-east-1"

BOTO3_REQUIRED_MESSAGE = (
    "error: boto3 is required for correctness scenarios; "
    "activate scripts/venv or install it with `python -m pip install -r scripts/requirements.txt`"
)


def require_boto3() -> tuple[Any, Any]:
    if boto3 is None or Config is None:
        raise RuntimeError(BOTO3_REQUIRED_MESSAGE)
    return boto3, Config


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def status_from_exception(error: Exception) -> int | None:
    if isinstance(error, ClientError):
        response = getattr(error, "response", {})
        metadata = response.get("ResponseMetadata", {})
        status = metadata.get("HTTPStatusCode")
        if isinstance(status, int):
            return status
    return None


def error_code(error: Exception) -> str:
    if isinstance(error, ClientError):
        response = getattr(error, "response", {})
        code = response.get("Error", {}).get("Code")
        if code:
            return str(code)
    return type(error).__name__


def is_timeout_error(error: Exception) -> bool:
    message = str(error).lower()
    name = type(error).__name__.lower()
    return "timeout" in message or "timed out" in message or "timeout" in name


@dataclass(frozen=True)
class S3CallResult:
    status: int | None
    headers: dict[str, str]
    body: bytes | None = None
    timeout: bool = False
    error: str | None = None
    error_code: str | None = None

    @property
    def ok(self) -> bool:
        return self.status is not None and 200 <= self.status < 300 and not self.timeout


class HistoryWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
        self.lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> None:
        with self.lock, self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")


class Boto3S3ClientPool:
    def __init__(self, entry_urls: list[str]) -> None:
        boto3_module, config_cls = require_boto3()
        self.clients = [
            {
                "entry_node": f"node{index + 1}",
                "endpoint": endpoint,
                "client": boto3_module.client(
                    "s3",
                    endpoint_url=endpoint,
                    aws_access_key_id=DEFAULT_ACCESS_KEY,
                    aws_secret_access_key=DEFAULT_SECRET_KEY,
                    region_name=DEFAULT_REGION,
                    config=config_cls(
                        signature_version="s3v4",
                        s3={"addressing_style": "path"},
                        retries={"total_max_attempts": 1, "mode": "standard"},
                        connect_timeout=1,
                        read_timeout=2,
                    ),
                ),
            }
            for index, endpoint in enumerate(entry_urls)
        ]

    def select(self, operation_number: int) -> dict[str, Any]:
        return self.clients[operation_number % len(self.clients)]


class CorrectnessDriver:
    """Generate a concurrent object history through boto3 S3 operations."""

    def __init__(
        self,
        *,
        entry_urls: list[str],
        history_path: Path,
        bucket: str,
        seed: int,
        operations: int = 120,
        concurrency: int = 12,
        object_size: int = 64,
    ) -> None:
        if not entry_urls:
            raise ValueError("at least one entry URL is required")
        self.entry_urls = entry_urls
        self.history = HistoryWriter(history_path)
        self.bucket = bucket
        self.seed = seed
        self.operations = operations
        self.concurrency = concurrency
        self.object_size = object_size
        self.random = random.Random(seed)
        self.pool = Boto3S3ClientPool(entry_urls)
        self.counter = 0
        self.counter_lock = threading.Lock()
        self.records: list[dict[str, Any]] = []
        self.records_lock = threading.Lock()

    def next_operation_id(self, prefix: str) -> str:
        with self.counter_lock:
            self.counter += 1
            return f"{prefix}-{self.counter:06d}"

    def body_for(self, operation_id: str, key: str) -> bytes:
        seed_material = f"{self.seed}:{operation_id}:{key}".encode()
        digest = hashlib.sha256(seed_material).digest()
        return (digest * ((self.object_size // len(digest)) + 1))[: self.object_size]

    def record_operation(
        self,
        *,
        operation_id: str,
        operation_type: str,
        key: str,
        entry_node: str,
        endpoint: str,
        input_body: bytes | None,
        result: S3CallResult,
        start_ts: str,
        start_monotonic: float,
        end_ts: str,
        end_monotonic: float,
    ) -> dict[str, Any]:
        response_hash = sha256_hex(result.body) if result.body is not None else None
        etag = result.headers.get("etag")
        if operation_type == "HEAD" and etag:
            response_hash = etag.strip('"')

        record = {
            "schema_version": 1,
            "operation_id": operation_id,
            "idempotency_key": None,
            "operation_type": operation_type,
            "key": key,
            "input_value_hash": sha256_hex(input_body)
            if input_body is not None
            else None,
            "returned_value_hash": response_hash,
            "observed_version": result.headers.get("x-amz-version-id"),
            "etag": etag,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "start_monotonic_secs": start_monotonic,
            "end_monotonic_secs": end_monotonic,
            "latency_ms": (end_monotonic - start_monotonic) * 1000.0,
            "entry_node": entry_node,
            "endpoint": endpoint,
            "result_code": result.status,
            "success": result.ok,
            "timeout": result.timeout,
            "error": result.error,
            "error_code": result.error_code,
            "client": "boto3",
            "api": "s3",
        }
        self.history.append(record)
        with self.records_lock:
            self.records.append(record)
        return record

    def execute_call(
        self,
        *,
        operation_number: int,
        operation_type: str,
        key: str,
        body: bytes | None = None,
    ) -> dict[str, Any]:
        selected = self.pool.select(operation_number)
        client = selected["client"]
        operation_id = self.next_operation_id(operation_type.lower())
        start_ts = utc_now()
        start_monotonic = time.monotonic()
        result: S3CallResult
        try:
            if operation_type == "PUT":
                response = client.put_object(
                    Bucket=self.bucket, Key=key, Body=body or b""
                )
                result = self.result_from_response(response)
            elif operation_type == "GET":
                response = client.get_object(Bucket=self.bucket, Key=key)
                payload = response["Body"].read()
                result = self.result_from_response(response, body=payload)
            elif operation_type == "HEAD":
                response = client.head_object(Bucket=self.bucket, Key=key)
                result = self.result_from_response(response)
            elif operation_type == "DELETE":
                response = client.delete_object(Bucket=self.bucket, Key=key)
                result = self.result_from_response(response)
            else:
                raise ValueError(f"unsupported operation type: {operation_type}")
        except (ClientError, BotoCoreError) as error:
            result = S3CallResult(
                status=status_from_exception(error),
                headers={},
                timeout=is_timeout_error(error),
                error=str(error),
                error_code=error_code(error),
            )
        except Exception as error:
            result = S3CallResult(
                status=None,
                headers={},
                timeout=is_timeout_error(error),
                error=str(error),
                error_code=type(error).__name__,
            )
        end_monotonic = time.monotonic()
        end_ts = utc_now()
        return self.record_operation(
            operation_id=operation_id,
            operation_type=operation_type,
            key=key,
            entry_node=selected["entry_node"],
            endpoint=selected["endpoint"],
            input_body=body if operation_type == "PUT" else None,
            result=result,
            start_ts=start_ts,
            start_monotonic=start_monotonic,
            end_ts=end_ts,
            end_monotonic=end_monotonic,
        )

    @staticmethod
    def result_from_response(
        response: dict[str, Any], body: bytes | None = None
    ) -> S3CallResult:
        metadata = response.get("ResponseMetadata", {})
        headers = {
            str(key).lower(): str(value)
            for key, value in metadata.get("HTTPHeaders", {}).items()
        }
        if "ETag" in response:
            headers.setdefault("etag", str(response["ETag"]))
        if "VersionId" in response:
            headers.setdefault("x-amz-version-id", str(response["VersionId"]))
        return S3CallResult(
            status=metadata.get("HTTPStatusCode"),
            headers=headers,
            body=body,
        )

    def build_plan(self) -> list[tuple[str, str]]:
        shared_key = "correctness/shared-object"
        race_key = "correctness/put-delete-race"
        plan: list[tuple[str, str]] = []

        for index in range(max(1, self.operations // 4)):
            plan.append(("PUT", f"correctness/independent-{index}"))
        for _ in range(max(1, self.operations // 4)):
            plan.append(("PUT", shared_key))
        for index in range(max(1, self.operations // 4)):
            plan.append(("GET" if index % 2 == 0 else "HEAD", shared_key))
        for index in range(max(1, self.operations - len(plan))):
            plan.append((("DELETE" if index % 3 == 0 else "PUT"), race_key))

        self.random.shuffle(plan)
        return plan[: self.operations]

    def run(self) -> dict[str, Any]:
        plan = self.build_plan()
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:
            futures = []
            for number, (operation_type, key) in enumerate(plan):
                operation_id_preview = f"planned-{number}"
                body = (
                    self.body_for(operation_id_preview, key)
                    if operation_type == "PUT"
                    else None
                )
                futures.append(
                    executor.submit(
                        self.execute_call,
                        operation_number=number,
                        operation_type=operation_type,
                        key=key,
                        body=body,
                    )
                )
            for future in concurrent.futures.as_completed(futures):
                future.result()

        # Final observations make post-run visibility explicit for the verifier.
        final_keys = sorted(
            {key for _, key in plan if not key.startswith("correctness/independent-")}
        )
        for number, key in enumerate(final_keys, start=len(plan)):
            self.execute_call(operation_number=number, operation_type="GET", key=key)
            self.execute_call(
                operation_number=number + 1, operation_type="HEAD", key=key
            )

        elapsed = time.monotonic() - start
        return self.summary(elapsed)

    def summary(self, elapsed_seconds: float) -> dict[str, Any]:
        with self.records_lock:
            records = list(self.records)
        attempted = len(records)
        successful = sum(1 for record in records if record["success"])
        timeouts = sum(1 for record in records if record["timeout"])
        errors = sum(1 for record in records if record["error"])
        latencies = [float(record["latency_ms"]) for record in records]
        return {
            "attempted_ops": attempted,
            "successful_ops": successful,
            "failed_ops": attempted - successful,
            "timeouts": timeouts,
            "errors": errors,
            "success_ratio": successful / attempted if attempted else 0.0,
            "timeout_ratio": timeouts / attempted if attempted else 0.0,
            "throughput_ops_per_sec": attempted / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0,
            "latency_ms": {
                "min": min(latencies) if latencies else 0.0,
                "max": max(latencies) if latencies else 0.0,
                "mean": sum(latencies) / len(latencies) if latencies else 0.0,
            },
        }
