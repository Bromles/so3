"""Async correctness workload driver using aioboto3 S3 client.

Operations are generated at a steady rate over the configured duration,
giving fault injection time to act on in-flight requests.
Final reads happen after the caller signals completion (e.g. after
cluster recovery), so they observe the durable state.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import aioboto3
import aiohttp
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"
DEFAULT_REGION = "us-east-1"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


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
        self._lock = asyncio.Lock()

    async def append(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, sort_keys=True) + "\n"
        async with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line)


class AsyncS3ClientPool:
    def __init__(
        self,
        entry_urls: list[str],
        faulty_node_fn: Any | None = None,
    ) -> None:
        self._session = aioboto3.Session()
        self._entry_urls = entry_urls
        self._faulty_node_fn = faulty_node_fn
        self._clients: list[dict[str, Any]] = [
            {
                "entry_node": f"node{index + 1}",
                "node_index": index + 1,
                "endpoint": endpoint,
            }
            for index, endpoint in enumerate(entry_urls)
        ]
        self._context_managers: list[Any] = []

    async def start(self) -> None:
        for entry in self._clients:
            ctx = self._session.client(
                "s3",
                endpoint_url=entry["endpoint"],
                aws_access_key_id=DEFAULT_ACCESS_KEY,
                aws_secret_access_key=DEFAULT_SECRET_KEY,
                region_name=DEFAULT_REGION,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    retries={"total_max_attempts": 1, "mode": "standard"},
                    connect_timeout=3,
                    read_timeout=10,
                ),
            )
            self._context_managers.append(ctx)
            entry["client"] = await ctx.__aenter__()

    async def close(self) -> None:
        for ctx in self._context_managers:
            await ctx.__aexit__(None, None, None)
        self._context_managers.clear()

    def select(self, operation_number: int) -> dict[str, Any]:
        candidates = self._clients
        if self._faulty_node_fn is not None:
            faulty = self._faulty_node_fn()
            if faulty is not None:
                filtered = [c for c in candidates if c["node_index"] != faulty]
                if filtered:
                    candidates = filtered
        return candidates[operation_number % len(candidates)]


class CorrectnessDriver:
    """Generate a concurrent object history through async S3 operations.

    Operations are produced at a configurable rate (``ops_per_sec``) over
    ``duration_secs``.  A semaphore bounds the number of in-flight
    operations to ``concurrency``.
    """

    def __init__(
        self,
        *,
        entry_urls: list[str],
        history_path: Path,
        bucket: str,
        seed: int,
        ops_per_sec: float = 2.0,
        duration_secs: float = 30.0,
        concurrency: int = 12,
        object_size: int = 64,
        faulty_node_fn: Any | None = None,
    ) -> None:
        if not entry_urls:
            msg = "at least one entry URL is required"
            raise ValueError(msg)
        self.entry_urls = entry_urls
        self.history = HistoryWriter(history_path)
        self.bucket = bucket
        self.seed = seed
        self.ops_per_sec = ops_per_sec
        self.duration_secs = duration_secs
        self.concurrency = concurrency
        self.object_size = object_size
        self.random = random.Random(seed)
        self.pool = AsyncS3ClientPool(entry_urls, faulty_node_fn=faulty_node_fn)
        self._counter = 0
        self._counter_lock = asyncio.Lock()
        self.records: list[dict[str, Any]] = []

    @property
    def operations(self) -> int:
        return max(1, int(self.ops_per_sec * self.duration_secs))

    async def _next_operation_id(self, prefix: str) -> str:
        async with self._counter_lock:
            self._counter += 1
            return f"{prefix}-{self._counter:06d}"

    def body_for(self, operation_id: str, key: str) -> bytes:
        seed_material = f"{self.seed}:{operation_id}:{key}".encode()
        digest = hashlib.sha256(seed_material).digest()
        return (digest * ((self.object_size // len(digest)) + 1))[: self.object_size]

    async def _record_operation(
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
            "client": "aioboto3",
            "api": "s3",
        }
        await self.history.append(record)
        self.records.append(record)
        return record

    async def _execute_call(
        self,
        *,
        operation_number: int,
        operation_type: str,
        key: str,
        body: bytes | None = None,
    ) -> dict[str, Any]:
        selected = self.pool.select(operation_number)
        client = selected["client"]
        operation_id = await self._next_operation_id(operation_type.lower())
        start_ts = utc_now()
        start_monotonic = time.monotonic()
        result: S3CallResult
        try:
            if operation_type == "PUT":
                response = await client.put_object(
                    Bucket=self.bucket, Key=key, Body=body or b""
                )
                result = self._result_from_response(response)
            elif operation_type == "GET":
                response = await client.get_object(Bucket=self.bucket, Key=key)
                payload = await response["Body"].read()
                result = self._result_from_response(response, body=payload)
            elif operation_type == "HEAD":
                response = await client.head_object(Bucket=self.bucket, Key=key)
                result = self._result_from_response(response)
            elif operation_type == "DELETE":
                response = await client.delete_object(Bucket=self.bucket, Key=key)
                result = self._result_from_response(response)
            else:
                msg = f"unsupported operation type: {operation_type}"
                raise ValueError(msg)
        except (ClientError, BotoCoreError, aiohttp.ClientError) as error:
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
        return await self._record_operation(
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
    def _result_from_response(
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

    def _generate_operation(self, index: int) -> tuple[str, str]:
        shared_key = "correctness/shared-object"
        race_key = "correctness/put-delete-race"
        r = index % 12
        if r < 3:
            return "PUT", f"correctness/independent-{index % 20}"
        if r < 6:
            return "PUT", shared_key
        if r < 8:
            return "GET", shared_key
        if r < 10:
            return "HEAD", shared_key
        if r == 10:
            return "DELETE", race_key
        return "PUT", race_key

    async def run(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        original_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, _ctx: None)
        try:
            await self.pool.start()
            try:
                return await self._run_with_pool()
            finally:
                await self.pool.close()
        finally:
            loop.set_exception_handler(original_handler)

    async def _run_with_pool(self) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(self.concurrency)
        interval = 1.0 / self.ops_per_sec if self.ops_per_sec > 0 else 0.0
        total = self.operations

        async def guarded_call(
            number: int, op_type: str, key: str, body: bytes | None
        ) -> dict[str, Any]:
            async with semaphore:
                return await self._execute_call(
                    operation_number=number,
                    operation_type=op_type,
                    key=key,
                    body=body,
                )

        start = time.monotonic()
        tasks: list[asyncio.Task[dict[str, Any]]] = []
        for i in range(total):
            op_type, key = self._generate_operation(i)
            body = self.body_for(f"planned-{i}", key) if op_type == "PUT" else None
            tasks.append(asyncio.create_task(guarded_call(i, op_type, key, body)))
            if i < total - 1 and interval > 0:
                elapsed = time.monotonic() - start
                target = (i + 1) * interval
                delay = target - elapsed
                if delay > 0:
                    await asyncio.sleep(delay)

        await asyncio.gather(*tasks)

        final_keys = sorted(
            {
                key
                for t in tasks
                if (r := t.result())
                and r.get("key", "").startswith("correctness/")
                and not r["key"].startswith("correctness/independent-")
                for key in [r["key"]]
            }
        )
        for number, key in enumerate(final_keys, start=total):
            await self._execute_call(
                operation_number=number, operation_type="GET", key=key
            )
            await self._execute_call(
                operation_number=number + 1, operation_type="HEAD", key=key
            )

        elapsed = time.monotonic() - start
        return self._summary(elapsed)

    def _summary(self, elapsed_seconds: float) -> dict[str, Any]:
        records = list(self.records)
        attempted = len(records)
        successful = sum(1 for record in records if record["success"])
        timeouts = sum(1 for record in records if record["timeout"])
        errors = sum(
            1 for record in records if record["error"] and not record["timeout"]
        )
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


class RecoverySentinel:
    """Write objects before a fault and verify they persist after recovery."""

    def __init__(
        self,
        *,
        entry_urls: list[str],
        bucket: str,
        seed: int,
        count: int = 20,
        object_size: int = 64,
    ) -> None:
        if not entry_urls:
            msg = "at least one entry URL is required"
            raise ValueError(msg)
        self.bucket = bucket
        self.seed = seed
        self.count = count
        self.object_size = object_size
        self.pool = AsyncS3ClientPool(entry_urls)

    def _key(self, index: int) -> str:
        return f"e6-sentinel/key-{index:04d}"

    def _body(self, index: int) -> bytes:
        material = f"{self.seed}:e6-sentinel:{index}".encode()
        digest = hashlib.sha256(material).digest()
        return (digest * ((self.object_size // len(digest)) + 1))[: self.object_size]

    async def write(self) -> dict[str, str]:
        await self.pool.start()
        try:
            confirmed: dict[str, str] = {}
            for index in range(self.count):
                key = self._key(index)
                body = self._body(index)
                selected = self.pool.select(index)
                try:
                    await selected["client"].put_object(
                        Bucket=self.bucket, Key=key, Body=body
                    )
                    confirmed[key] = sha256_hex(body)
                except Exception:
                    pass
            return confirmed
        finally:
            await self.pool.close()

    async def verify(self, confirmed: dict[str, str]) -> dict[str, Any]:
        await self.pool.start()
        try:
            issues: list[dict[str, Any]] = []
            for index, (key, expected_hash) in enumerate(confirmed.items()):
                selected = self.pool.select(index)
                try:
                    response = await selected["client"].get_object(
                        Bucket=self.bucket, Key=key
                    )
                    actual_hash = sha256_hex(await response["Body"].read())
                    if actual_hash != expected_hash:
                        issues.append(
                            {
                                "invariant": "recovery_preserves_confirmed_writes",
                                "key": key,
                                "kind": "hash_mismatch",
                                "expected_hash": expected_hash,
                                "actual_hash": actual_hash,
                            }
                        )
                except (ClientError, BotoCoreError) as error:
                    issues.append(
                        {
                            "invariant": "recovery_preserves_confirmed_writes",
                            "key": key,
                            "kind": "get_failed",
                            "error": str(error),
                        }
                    )
            return {
                "schema_version": 1,
                "checked": ["recovery_preserves_confirmed_writes"],
                "unsupported": [],
                "confirmed_writes": len(confirmed),
                "issues": issues,
                "verdict": "passed" if not issues else "failed",
            }
        finally:
            await self.pool.close()
