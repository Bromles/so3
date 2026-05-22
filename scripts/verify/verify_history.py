"""History verifier for SO3 object-level correctness scenarios.

Records are sorted by ``start_monotonic_secs`` so that the verifier
reasons about *when the client issued the request*, not when the
response arrived.  For the delete-visibility invariant, concurrent
operations that overlap a DELETE are excluded: the invariant only
applies to reads that started strictly after the DELETE completed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SUPPORTED_INVARIANTS = [
    "reads_return_only_successfully_written_values",
    "head_etag_matches_successfully_written_values",
    "successful_delete_hides_prior_value_until_next_successful_put",
]

UNSUPPORTED_INVARIANTS = [
    "cas_success_requires_matching_version",
    "if_none_match_success_requires_absence",
    "same_idempotency_key_does_not_create_second_change",
]


@dataclass
class VerificationIssue:
    invariant: str
    operation_id: str | None
    key: str | None
    message: str

    def to_json(self) -> dict[str, Any]:
        return {
            "invariant": self.invariant,
            "operation_id": self.operation_id,
            "key": self.key,
            "message": self.message,
        }


@dataclass
class VerificationResult:
    verdict: str = "passed"
    checked: list[str] = field(default_factory=lambda: list(SUPPORTED_INVARIANTS))
    unsupported: list[str] = field(default_factory=lambda: list(UNSUPPORTED_INVARIANTS))
    issues: list[VerificationIssue] = field(default_factory=list)
    operation_count: int = 0

    def fail(self, issue: VerificationIssue) -> None:
        self.verdict = "failed"
        self.issues.append(issue)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "verdict": self.verdict,
            "operation_count": self.operation_count,
            "checked": self.checked,
            "unsupported": self.unsupported,
            "issues": [issue.to_json() for issue in self.issues],
        }


def load_history(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSONL at {path}:{line_number}: {error}"
                ) from error
    records.sort(
        key=lambda record: (
            float(record.get("start_monotonic_secs", 0.0)),
            str(record.get("operation_id", "")),
        )
    )
    return records


def is_success(record: dict[str, Any], operation_type: str | None = None) -> bool:
    if operation_type is not None and record.get("operation_type") != operation_type:
        return False
    return bool(record.get("success"))


def is_ambiguous(record: dict[str, Any]) -> bool:
    return not record.get("success")


def is_success_or_ambiguous(record: dict[str, Any]) -> bool:
    return bool(record.get("success")) or is_ambiguous(record)


def _start(record: dict[str, Any]) -> float:
    return float(record.get("start_monotonic_secs", 0.0))


def _end(record: dict[str, Any]) -> float:
    return float(record.get("end_monotonic_secs", 0.0))


def verify_history(records: list[dict[str, Any]]) -> VerificationResult:
    result = VerificationResult(operation_count=len(records))
    known_values_by_key: dict[str, set[str]] = {}
    known_etags_by_key: dict[str, set[str]] = {}

    for record in records:
        key = record.get("key")
        if not isinstance(key, str):
            result.fail(
                VerificationIssue(
                    invariant="history_schema",
                    operation_id=record.get("operation_id"),
                    key=None,
                    message="operation has no string key",
                )
            )
            continue

        op = record.get("operation_type")
        is_ambiguous_put = op == "PUT" and not is_success(record)
        if op == "PUT" and (is_success(record) or is_ambiguous_put):
            value_hash = record.get("input_value_hash")
            if isinstance(value_hash, str):
                known_values_by_key.setdefault(key, set()).add(value_hash)
                if is_ambiguous_put:
                    known_etags_by_key.setdefault(key, set()).add(value_hash)
            elif is_success(record):
                result.fail(
                    VerificationIssue(
                        invariant="history_schema",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message="successful PUT has no input_value_hash",
                    )
                )
            etag = record.get("etag")
            if isinstance(etag, str):
                known_etags_by_key.setdefault(key, set()).add(etag.strip('"'))

        if op == "GET" and is_success(record):
            etag = record.get("etag")
            if isinstance(etag, str):
                known_etags_by_key.setdefault(key, set()).add(etag.strip('"'))

    for record in records:
        operation_type = record.get("operation_type")
        key = record.get("key")
        if not isinstance(key, str) or operation_type not in {"GET", "HEAD"}:
            continue

        status = record.get("result_code")
        if status in {403, 404} or record.get("timeout"):
            continue
        if status != 200 or not is_success(record):
            continue

        returned_hash = record.get("returned_value_hash")
        read_start = _start(record)

        if operation_type == "GET":
            if not isinstance(returned_hash, str):
                result.fail(
                    VerificationIssue(
                        invariant="reads_return_only_successfully_written_values",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message="successful GET has no returned_value_hash",
                    )
                )
                continue
            if returned_hash not in known_values_by_key.get(key, set()):
                result.fail(
                    VerificationIssue(
                        invariant="reads_return_only_successfully_written_values",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message=f"GET returned value {returned_hash} that was never successfully written for this key",
                    )
                )
            if _read_after_delete(records, key, read_start):
                result.fail(
                    VerificationIssue(
                        invariant="successful_delete_hides_prior_value_until_next_successful_put",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message="GET returned a value after successful DELETE and before a later successful PUT",
                    )
                )

        elif operation_type == "HEAD":
            etags = known_etags_by_key.get(key)
            if not isinstance(returned_hash, str):
                if etags:
                    result.fail(
                        VerificationIssue(
                            invariant="head_etag_matches_successfully_written_values",
                            operation_id=record.get("operation_id"),
                            key=key,
                            message="successful HEAD returned no etag but key has known etags",
                        )
                    )
            elif etags and returned_hash not in etags:
                result.fail(
                    VerificationIssue(
                        invariant="head_etag_matches_successfully_written_values",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message=f"HEAD returned etag {returned_hash} not seen in any PUT or GET for this key",
                    )
                )
            if _read_after_delete(records, key, read_start):
                result.fail(
                    VerificationIssue(
                        invariant="successful_delete_hides_prior_value_until_next_successful_put",
                        operation_id=record.get("operation_id"),
                        key=key,
                        message="HEAD returned 200 after successful DELETE and before a later successful PUT",
                    )
                )

    return result


def _read_after_delete(
    records: list[dict[str, Any]], key: str, read_start: float
) -> bool:
    last_delete_end: float | None = None
    for record in records:
        if record.get("key") != key:
            continue
        if record.get("operation_type") != "DELETE":
            continue
        if not is_success_or_ambiguous(record):
            continue
        del_end = _end(record)
        if del_end < read_start:
            if last_delete_end is None or del_end > last_delete_end:
                last_delete_end = del_end

    if last_delete_end is None:
        return False

    for record in records:
        if record.get("key") != key:
            continue
        if record.get("operation_type") != "PUT":
            continue
        if not is_success_or_ambiguous(record):
            continue
        put_start = _start(record)
        if last_delete_end < put_start < read_start:
            return False

    return True


def verify_history_file(path: Path) -> dict[str, Any]:
    return verify_history(load_history(path)).to_json()
