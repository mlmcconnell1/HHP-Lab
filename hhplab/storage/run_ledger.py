"""Durable local-filesystem run events, specification locks, and live leases.

The caller supplies resolved component hashes. This module never loads recipes
or builds data. A durable start precedes work; a terminal event precedes lease
removal. Call ``RunLease.check_health`` at execution boundaries so heartbeat I/O
failures cannot be reported as successful runs.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import socket
import subprocess
import sys
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hhplab.schema.results import (
    LEDGER_SCHEMA_VERSION,
    ResultContractError,
    canonical_bytes,
    validate_ledger_event,
)

HEARTBEAT_SECONDS = 30
LEASE_EXPIRY_SECONDS = 300
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_TERMINAL_EVENTS = frozenset({"run_finished", "run_failed"})


class LedgerError(RuntimeError):
    """An event or lease could not be safely persisted or interpreted."""


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def process_identity(pid: int) -> str | None:
    """Return a process birth identity, guarding against PID reuse."""
    if sys.platform.startswith("linux"):
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            fields = stat.rsplit(")", 1)[1].split()
            if fields[0] == "Z":
                return None
            boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            return f"{boot}:{fields[19]}"
        except (OSError, IndexError):
            return None
    result = subprocess.run(
        ["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or None


def _safe_id(value: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise LedgerError(
            "run/lock ID must contain only letters, digits, dots, underscores or hyphens"
        )
    return value


def _event(kind: str, **fields: Any) -> dict[str, Any]:
    return validate_ledger_event(
        {"event": kind, "schema_version": LEDGER_SCHEMA_VERSION, "ts": utc_timestamp(), **fields}
    )


def _decode_history(raw: bytes) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parsed: list[tuple[int, dict[str, Any]]] = []
    errors: list[dict[str, Any]] = []
    offset = 0
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            errors.append({"code": "torn_line", "byte_offset": offset, "byte_length": len(line)})
        else:
            try:
                value = validate_ledger_event(json.loads(line))
                parsed.append((offset, value))
            except (ValueError, TypeError, UnicodeDecodeError, ResultContractError) as exc:
                errors.append(
                    {
                        "code": "ledger_corrupt",
                        "byte_offset": offset,
                        "byte_length": len(line) - 1,
                        "message": str(exc),
                    }
                )
        offset += len(line)
    repairs = {
        event["byte_offset"]: event["byte_length"]
        for _, event in parsed
        if event["event"] == "ledger_repair"
    }
    for error in errors:
        if repairs.get(error["byte_offset"]) == error["byte_length"]:
            error["code"] = "repaired_torn_line"
    # An isolated fragment can happen to be valid JSON; the repair still marks
    # it as uncommitted bytes rather than an authoritative event.
    events = [event for offset, event in parsed if offset not in repairs]
    return events, errors


def _write_once(fd: int, payload: bytes) -> None:
    written = os.write(fd, payload)
    os.fsync(fd)
    if written != len(payload):
        raise LedgerError(f"short ledger write ({written}/{len(payload)} bytes); inspect history")


class RunLedger:
    """Append-only run history rooted at an output directory."""

    def __init__(self, outputs_root: str | Path):
        self.root = Path(outputs_root) / "ledger"
        self.path = self.root / "runs.jsonl"

    def read(self) -> dict[str, Any]:
        try:
            with self.path.open("rb") as stream:
                fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
                raw = stream.read()
        except FileNotFoundError:
            raw = b""
        events, errors = _decode_history(raw)
        return {"events": events, "errors": errors}

    def append(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """Append one validated event under a local advisory lock and fsync.

        Short writes fail immediately. A later append isolates an incomplete
        final line without overwriting any historical bytes.
        """
        value = validate_ledger_event(event)
        for field in ("run_id", "lock_id"):
            if value.get(field) is not None:
                _safe_id(value[field])
        self.root.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+b") as stream:
            fd = stream.fileno()
            fcntl.flock(fd, fcntl.LOCK_EX)
            stream.seek(0)
            raw = stream.read()
            events, errors = _decode_history(raw)
            corrupt = [error for error in errors if error["code"] == "ledger_corrupt"]
            if corrupt:
                raise LedgerError(
                    f"ledger_corrupt: inspect {self.path} at {corrupt[0]['byte_offset']}"
                )
            self._validate_lifecycle(value, events)
            if raw and not raw.endswith(b"\n"):
                fragment_start = raw.rfind(b"\n") + 1
                repair = _event(
                    "ledger_repair",
                    byte_offset=fragment_start,
                    byte_length=len(raw) - fragment_start,
                    ledger_path=str(self.path),
                )
                _write_once(fd, b"\n" + canonical_bytes(repair) + b"\n")
            _write_once(fd, canonical_bytes(value) + b"\n")
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return value

    @staticmethod
    def _validate_lifecycle(event: Mapping[str, Any], events: list[dict[str, Any]]) -> None:
        kind = event["event"]
        if kind == "lock_created":
            if any(
                item.get("lock_id") == event["lock_id"]
                for item in events
                if item["event"] == "lock_created"
            ):
                raise LedgerError("duplicate lock_id; create a new lock identity")
        if kind not in {"run_started", *_TERMINAL_EVENTS}:
            return
        previous = [item for item in events if item.get("run_id") == event["run_id"]]
        if kind == "run_started" and previous:
            raise LedgerError("duplicate run_id; create a new run identity")
        if kind in _TERMINAL_EVENTS:
            if not any(item["event"] == "run_started" for item in previous):
                raise LedgerError("terminal event requires a durable run_started event")
            if any(item["event"] in _TERMINAL_EVENTS for item in previous):
                raise LedgerError("run already has a terminal event")

    def compare_lock(self, recipe_id: str, components: Mapping[str, str]) -> dict[str, Any]:
        history = self.read()
        if any(error["code"] == "ledger_corrupt" for error in history["errors"]):
            raise LedgerError("ledger_corrupt: cannot establish a lock comparison")
        locks = [
            event
            for event in history["events"]
            if event["event"] == "lock_created" and event["recipe_id"] == recipe_id
        ]
        if not locks:
            return {"status": "unlocked", "components": [], "lock_id": None}
        latest = locks[-1]
        previous = latest["component_hashes"]
        changed = sorted(
            key
            for key in set(previous) | set(components)
            if previous.get(key) != components.get(key)
        )
        return {
            "status": "modified" if changed else "matched",
            "components": changed,
            "lock_id": latest["lock_id"],
        }

    def create_lock(
        self,
        *,
        recipe_id: str,
        recipe_sha256: str,
        components: Mapping[str, str],
        spec_hash: str,
        export_path: str | Path | None = None,
    ) -> dict[str, Any]:
        event = _event(
            "lock_created",
            lock_id=uuid.uuid4().hex,
            recipe_id=recipe_id,
            recipe_sha256=recipe_sha256,
            spec_hash=spec_hash,
            component_hashes=dict(components),
            export_path=str(export_path) if export_path is not None else None,
        )
        if export_path is not None:
            destination = Path(export_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive creation prevents silently replacing an earlier public lock.
            with destination.open("xb") as stream:
                stream.write(canonical_bytes(event) + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
        return self.append(event)

    def start(
        self,
        *,
        recipe_id: str,
        recipe_sha256: str,
        spec_hash: str,
        components: Mapping[str, str],
        environment: Mapping[str, Any],
        run_id: str | None = None,
    ) -> RunLease:
        comparison = self.compare_lock(recipe_id, components)
        pid = os.getpid()
        identity = process_identity(pid)
        if identity is None:
            raise LedgerError("cannot establish current process identity; run cannot start")
        event = _event(
            "run_started",
            run_id=_safe_id(run_id or uuid.uuid4().hex),
            recipe_id=recipe_id,
            recipe_sha256=recipe_sha256,
            spec_hash=spec_hash,
            component_hashes=dict(components),
            lock_status={"status": comparison["status"], "components": comparison["components"]},
            lock_id=comparison["lock_id"],
            host=socket.gethostname(),
            pid=pid,
            process_start_time=identity,
            environment=dict(environment),
        )
        self.append(event)
        lease = RunLease(self, event)
        try:
            lease.start()
        except OSError as exc:
            self.fail_best_effort(event["run_id"], exc)
            raise
        return lease

    def fail_best_effort(
        self,
        run_id: str,
        error: BaseException,
        *,
        failure_bundle_path: str | None = None,
    ) -> bool:
        """Try once to record a run failure; never recurse on logging failure."""
        try:
            self.append(
                _event(
                    "run_failed",
                    run_id=run_id,
                    failure_scope="run",
                    error_class=type(error).__name__,
                    message=str(error) or type(error).__name__,
                    failure_bundle_path=failure_bundle_path,
                )
            )
        except (OSError, LedgerError, ValueError):
            return False
        return True

    def history(self, *, now: datetime | None = None) -> dict[str, Any]:
        report = self.read()
        now = now or datetime.now(UTC)
        runs: dict[str, dict[str, Any]] = {}
        for event in report["events"]:
            if event["event"] == "run_started":
                runs[event["run_id"]] = {"run_id": event["run_id"], "start": event}
            elif event["event"] in _TERMINAL_EVENTS and event["run_id"] in runs:
                runs[event["run_id"]]["terminal"] = event
        for run in runs.values():
            terminal = run.get("terminal")
            if terminal:
                run["status"] = (
                    "failed" if terminal["event"] == "run_failed" else terminal["execution_status"]
                )
            else:
                run["status"] = self._lease_status(run["start"], now)
        return {"runs": list(runs.values()), "errors": report["errors"]}

    def _lease_status(self, start: dict[str, Any], now: datetime) -> str:
        path = self.root / "running" / f"{_safe_id(start['run_id'])}.json"
        try:
            lease = json.loads(path.read_text())
            if any(
                lease[key] != start[key] for key in ("run_id", "host", "pid", "process_start_time")
            ):
                return "abandoned"
            heartbeat = datetime.fromisoformat(lease["heartbeat_ts"].replace("Z", "+00:00"))
            expired = heartbeat.tzinfo is None or (
                now - heartbeat > timedelta(seconds=LEASE_EXPIRY_SECONDS)
            )
            if expired:
                return "abandoned"
            if lease["host"] == socket.gethostname():
                if process_identity(lease["pid"]) != lease["process_start_time"]:
                    return "abandoned"
        except (OSError, ValueError, KeyError, TypeError):
            return "abandoned"
        return "running"


class RunLease:
    """Periodic atomic heartbeat for one already-started run."""

    def __init__(self, ledger: RunLedger, start: dict[str, Any]):
        self.ledger = ledger
        self.identity = {key: start[key] for key in ("run_id", "host", "pid", "process_start_time")}
        self.path = ledger.root / "running" / f"{self.identity['run_id']}.json"
        self._stop = threading.Event()
        self._error: OSError | None = None
        self._thread: threading.Thread | None = None

    def heartbeat(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        with temporary.open("xb") as stream:
            stream.write(canonical_bytes({**self.identity, "heartbeat_ts": utc_timestamp()}))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def start(self) -> None:
        self.heartbeat()
        self._thread = threading.Thread(target=self._keep_alive, daemon=True)
        self._thread.start()

    def _keep_alive(self) -> None:
        while not self._stop.wait(HEARTBEAT_SECONDS):
            try:
                self.heartbeat()
            except OSError as exc:
                self._error = exc
                return

    def check_health(self) -> None:
        if self._error is not None:
            raise LedgerError(f"lease heartbeat failed: {self._error}") from self._error

    def stop_heartbeat(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def finish(
        self,
        *,
        statuses: Mapping[str, str],
        bundle_path: str,
        manifest_sha256: str,
    ) -> None:
        self.stop_heartbeat()
        try:
            self.check_health()
            self.ledger.append(
                _event(
                    "run_finished",
                    run_id=self.identity["run_id"],
                    **statuses,
                    bundle_path=bundle_path,
                    manifest_sha256=manifest_sha256,
                )
            )
        except (OSError, LedgerError, ValueError) as exc:
            self.ledger.fail_best_effort(self.identity["run_id"], exc)
            raise
        self._remove_terminal_lease()

    def fail(self, error: BaseException, *, failure_bundle_path: str | None = None) -> bool:
        self.stop_heartbeat()
        recorded = self.ledger.fail_best_effort(
            self.identity["run_id"],
            error,
            failure_bundle_path=failure_bundle_path,
        )
        if recorded:
            self._remove_terminal_lease()
        return recorded

    def _remove_terminal_lease(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            # The durable terminal event is authoritative even if cleanup fails.
            pass
