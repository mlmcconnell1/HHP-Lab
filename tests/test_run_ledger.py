"""Ledger truth table: live lease=running; absent/expired/reused PID=abandoned;
terminal events override leases; torn tails are isolated, interior corruption
blocks append, and a failed durable start never starts a heartbeat.
"""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from hhplab.schema.results import LEDGER_SCHEMA_VERSION
from hhplab.storage import run_ledger as module
from hhplab.storage.run_ledger import LedgerError, RunLedger, utc_timestamp

DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64
COMPONENTS = {"samples": DIGEST, "analyses": DIGEST}
COMPLETE = {
    "execution_status": "complete",
    "diagnostics_status": "none",
    "findings_status": "all_evaluated",
    "distribution_status": "full",
}


def start(ledger, run_id="test-run"):
    return ledger.start(
        recipe_id="test",
        recipe_sha256=DIGEST,
        spec_hash=DIGEST,
        components=COMPONENTS,
        environment={},
        run_id=run_id,
    )


def lock_event(lock_id):
    return {
        "event": "lock_created",
        "schema_version": LEDGER_SCHEMA_VERSION,
        "ts": utc_timestamp(),
        "lock_id": lock_id,
        "recipe_id": "test",
        "recipe_sha256": DIGEST,
        "spec_hash": DIGEST,
        "component_hashes": COMPONENTS,
        "export_path": None,
    }


def test_parallel_append_is_parseable_and_preserves_every_event(tmp_path):
    ledger = RunLedger(tmp_path)
    lock_ids = [f"lock-{index}" for index in range(24)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda identifier: ledger.append(lock_event(identifier)), lock_ids))
    report = ledger.read()
    assert report["errors"] == []
    assert {event["lock_id"] for event in report["events"]} == set(lock_ids)


@pytest.mark.parametrize("fragment", [b'{"event":', b"not-json"], ids=["partial-json", "garbage"])
def test_torn_tail_is_preserved_and_isolated(tmp_path, fragment):
    ledger = RunLedger(tmp_path)
    ledger.append(lock_event("before"))
    with ledger.path.open("ab") as stream:
        stream.write(fragment)
    original = ledger.path.read_bytes()
    ledger.append(lock_event("after"))
    assert ledger.path.read_bytes().startswith(original)
    report = ledger.read()
    assert [error["code"] for error in report["errors"]] == ["repaired_torn_line"]
    assert [event["event"] for event in report["events"]] == [
        "lock_created",
        "ledger_repair",
        "lock_created",
    ]
    ledger.append(lock_event("later"))


def test_interior_corruption_is_reported_and_never_rewritten(tmp_path):
    ledger = RunLedger(tmp_path)
    ledger.append(lock_event("before"))
    with ledger.path.open("ab") as stream:
        stream.write(b"corrupt\n")
    before = ledger.path.read_bytes()
    with pytest.raises(LedgerError, match="ledger_corrupt"):
        ledger.append(lock_event("after"))
    assert ledger.path.read_bytes() == before
    assert ledger.history()["errors"][0]["code"] == "ledger_corrupt"


def test_short_write_is_a_failure_and_can_be_repaired(tmp_path, monkeypatch):
    ledger = RunLedger(tmp_path)
    original_write = os.write
    with monkeypatch.context() as patch:
        patch.setattr(module.os, "write", lambda fd, value: original_write(fd, value[:7]))
        with pytest.raises(LedgerError, match="short ledger write"):
            ledger.append(lock_event("short"))
    ledger.append(lock_event("recovered"))
    assert ledger.read()["errors"][0]["code"] == "repaired_torn_line"
    assert ledger.read()["events"][-1]["lock_id"] == "recovered"


@pytest.mark.parametrize(
    "change,expected",
    [
        ("live", "running"),
        ("expired", "abandoned"),
        ("missing", "abandoned"),
        ("reused", "abandoned"),
    ],
    ids=["live", "expired", "missing", "pid-reused"],
)
def test_lease_history_states(tmp_path, monkeypatch, change, expected):
    ledger = RunLedger(tmp_path)
    lease = start(ledger)
    lease.stop_heartbeat()
    now = datetime.now(UTC)
    if change == "expired":
        now += timedelta(seconds=module.LEASE_EXPIRY_SECONDS + 1)
    elif change == "missing":
        # Retain the fixture bytes while making the canonical lease unavailable.
        lease.path.rename(lease.path.with_suffix(".saved"))
    elif change == "reused":
        monkeypatch.setattr(module, "process_identity", lambda pid: "other-process-birth")
    assert ledger.history(now=now)["runs"][0]["status"] == expected
    assert len(ledger.read()["events"]) == 1


def test_terminal_event_overrides_expired_lease(tmp_path):
    ledger = RunLedger(tmp_path)
    lease = start(ledger)
    lease.finish(statuses=COMPLETE, bundle_path="bundle", manifest_sha256=DIGEST)
    assert not lease.path.exists()
    report = ledger.history(now=datetime.now(UTC) + timedelta(days=1))
    assert report["runs"][0]["status"] == "complete"


def test_failed_start_does_not_write_lease_or_start_work(tmp_path, monkeypatch):
    ledger = RunLedger(tmp_path)

    def fail_append(event):
        raise OSError("disk unavailable")

    monkeypatch.setattr(ledger, "append", fail_append)
    with pytest.raises(OSError, match="disk unavailable"):
        start(ledger)
    assert not (ledger.root / "running").exists()


def test_heartbeat_failure_cannot_finish_successfully(tmp_path):
    ledger = RunLedger(tmp_path)
    lease = start(ledger)
    lease._error = OSError("heartbeat disk error")
    with pytest.raises(LedgerError, match="heartbeat failed"):
        lease.finish(statuses=COMPLETE, bundle_path="bundle", manifest_sha256=DIGEST)
    assert ledger.history()["runs"][0]["status"] == "failed"
    assert ledger.read()["events"][-1]["event"] == "run_failed"


def test_best_effort_failure_attempts_only_once(tmp_path, monkeypatch):
    ledger = RunLedger(tmp_path)
    attempts = []

    def fail_append(event):
        attempts.append(event)
        raise OSError("offline")

    monkeypatch.setattr(ledger, "append", fail_append)
    assert not ledger.fail_best_effort("run", RuntimeError("failure"))
    assert len(attempts) == 1


def test_lock_changes_identify_semantic_components_and_preserve_export(tmp_path):
    ledger = RunLedger(tmp_path)
    assert ledger.compare_lock("test", COMPONENTS)["status"] == "unlocked"
    exported = tmp_path / "lock.json"
    lock = ledger.create_lock(
        recipe_id="test",
        recipe_sha256=DIGEST,
        components=COMPONENTS,
        spec_hash=DIGEST,
        export_path=exported,
    )
    assert json.loads(exported.read_text()) == lock
    assert ledger.compare_lock("test", COMPONENTS)["status"] == "matched"
    changed = ledger.compare_lock("test", {**COMPONENTS, "samples": OTHER_DIGEST})
    assert changed == {"status": "modified", "components": ["samples"], "lock_id": lock["lock_id"]}
    before = exported.read_bytes()
    with pytest.raises(FileExistsError):
        ledger.create_lock(
            recipe_id="test",
            recipe_sha256=DIGEST,
            components=COMPONENTS,
            spec_hash=DIGEST,
            export_path=exported,
        )
    assert exported.read_bytes() == before


def test_duplicate_start_and_terminal_events_are_rejected(tmp_path):
    ledger = RunLedger(tmp_path)
    lease = start(ledger)
    try:
        with pytest.raises(LedgerError, match="duplicate run_id"):
            start(ledger)
        assert lease.fail(RuntimeError("failure"))
        assert not ledger.fail_best_effort("test-run", RuntimeError("second"))
    finally:
        lease.stop_heartbeat()


def test_killed_process_is_abandoned_without_waiting_for_expiry(tmp_path):
    program = """
import sys, threading
from hhplab.storage.run_ledger import RunLedger
digest = 'a' * 64
lease = RunLedger(sys.argv[1]).start(
    recipe_id='test', recipe_sha256=digest, spec_hash=digest,
    components={'samples': digest}, environment={}, run_id='child',
)
print('ready', flush=True)
threading.Event().wait()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(tmp_path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        ledger = RunLedger(tmp_path)
        assert ledger.history()["runs"][0]["status"] == "running"
    finally:
        process.terminate()
        process.wait(timeout=10)
        process.stdout.close()
    assert ledger.history()["runs"][0]["status"] == "abandoned"
