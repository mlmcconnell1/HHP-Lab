"""Tests for the source_registry module.

Covers CRUD round-trips, change detection, history ordering, deduplication,
upstream change detection, serialization, edge cases, and provenance embedding.

Truth table for check_source_changed
-------------------------------------
| Registry state          | current_sha256 | changed | is_new |
|-------------------------|----------------|---------|--------|
| empty (no prior entry)  | any            | False   | True   |
| prior hash == current   | same           | False   | False  |
| prior hash != current   | different      | True    | False  |
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from coclab.source_registry import (
    SourceRegistryEntry,
    check_source_changed,
    detect_upstream_changes,
    get_latest_source,
    get_source_history,
    list_sources,
    register_source,
)

# ---------------------------------------------------------------------------
# Fixture constants — tests derive expectations from these
# ---------------------------------------------------------------------------

ENTRY_A = dict(
    source_type="zori",
    source_url="https://example.com/zori.csv",
    source_name="ZORI County Monthly",
    raw_sha256="aaa111",
    file_size=1000,
    local_path="data/raw/zori/zori__county__2025-01-06.csv",
    metadata={"vintage": "2025-01"},
)

ENTRY_B = dict(
    source_type="zori",
    source_url="https://example.com/zori.csv",
    source_name="ZORI County Monthly",
    raw_sha256="bbb222",
    file_size=2000,
    local_path="data/raw/zori/zori__county__2025-02-06.csv",
    metadata={"vintage": "2025-02"},
)

ENTRY_C = dict(
    source_type="boundary",
    source_url="https://example.com/boundary.zip",
    source_name="HUD CoC Boundaries",
    raw_sha256="ccc333",
    file_size=5000,
    local_path="data/raw/boundary/coc_boundaries__2025.zip",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def registry_path(tmp_path: Path) -> Path:
    """Isolated registry file inside tmp_path."""
    return tmp_path / "source_registry.parquet"


# ---------------------------------------------------------------------------
# 1. CRUD round-trip
# ---------------------------------------------------------------------------


class TestCrudRoundTrip:
    """register_source -> get_latest_source -> verify fields."""

    def test_register_and_retrieve(self, registry_path: Path) -> None:
        entry = register_source(**ENTRY_A, registry_path=registry_path)

        assert entry.source_type == ENTRY_A["source_type"]
        assert entry.source_url == ENTRY_A["source_url"]
        assert entry.raw_sha256 == ENTRY_A["raw_sha256"]
        assert entry.file_size == ENTRY_A["file_size"]
        assert entry.local_path == ENTRY_A["local_path"]
        assert entry.metadata == ENTRY_A["metadata"]

        latest = get_latest_source(
            source_type=ENTRY_A["source_type"],
            source_url=ENTRY_A["source_url"],
            registry_path=registry_path,
        )
        assert latest is not None
        assert latest.source_type == entry.source_type
        assert latest.raw_sha256 == entry.raw_sha256
        assert latest.source_name == entry.source_name

    def test_get_latest_filters_by_url(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_C, registry_path=registry_path)

        latest = get_latest_source(
            source_type="boundary",
            source_url=ENTRY_C["source_url"],
            registry_path=registry_path,
        )
        assert latest is not None
        assert latest.raw_sha256 == ENTRY_C["raw_sha256"]

    def test_get_latest_without_url_returns_most_recent(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_B, registry_path=registry_path)

        latest = get_latest_source(
            source_type="zori",
            registry_path=registry_path,
        )
        assert latest is not None
        assert latest.raw_sha256 == ENTRY_B["raw_sha256"]


# ---------------------------------------------------------------------------
# 2. Change detection
# ---------------------------------------------------------------------------


class TestChangeDetection:
    """check_source_changed: same hash → no change, different hash → change."""

    def test_same_hash_no_change(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)

        changed, details = check_source_changed(
            source_type=ENTRY_A["source_type"],
            source_url=ENTRY_A["source_url"],
            current_sha256=ENTRY_A["raw_sha256"],
            registry_path=registry_path,
        )
        assert changed is False
        assert details["is_new"] is False
        assert details["previous_sha256"] == ENTRY_A["raw_sha256"]

    def test_different_hash_change_detected(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        new_hash = "zzz999"

        changed, details = check_source_changed(
            source_type=ENTRY_A["source_type"],
            source_url=ENTRY_A["source_url"],
            current_sha256=new_hash,
            registry_path=registry_path,
        )
        assert changed is True
        assert details["is_new"] is False
        assert details["previous_sha256"] == ENTRY_A["raw_sha256"]

    def test_new_source_not_flagged_as_changed(self, registry_path: Path) -> None:
        changed, details = check_source_changed(
            source_type="pit",
            source_url="https://example.com/pit.xlsx",
            current_sha256="abc123",
            registry_path=registry_path,
        )
        assert changed is False
        assert details["is_new"] is True
        assert details["previous_sha256"] is None


# ---------------------------------------------------------------------------
# 3. History ordering
# ---------------------------------------------------------------------------


class TestHistory:
    """Multiple registrations for the same source produce ordered history."""

    def test_history_ordered_most_recent_first(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_B, registry_path=registry_path)

        history = get_source_history(
            source_type="zori",
            source_url=ENTRY_A["source_url"],
            registry_path=registry_path,
        )
        assert len(history) == 2
        assert history[0].ingested_at >= history[1].ingested_at
        assert history[0].raw_sha256 == ENTRY_B["raw_sha256"]
        assert history[1].raw_sha256 == ENTRY_A["raw_sha256"]

    def test_history_filters_by_url(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_C, registry_path=registry_path)

        history = get_source_history(
            source_type="zori",
            source_url=ENTRY_A["source_url"],
            registry_path=registry_path,
        )
        assert len(history) == 1
        assert history[0].source_type == "zori"


# ---------------------------------------------------------------------------
# 4. list_sources: deduplicated listing
# ---------------------------------------------------------------------------


class TestListSources:
    """list_sources returns only the most recent entry per (type, url) pair."""

    def test_deduplicates_by_type_url(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_B, registry_path=registry_path)
        register_source(**ENTRY_C, registry_path=registry_path)

        sources = list_sources(registry_path=registry_path)
        assert len(sources) == 2

        type_url_pairs = {(s.source_type, s.source_url) for s in sources}
        assert ("zori", ENTRY_A["source_url"]) in type_url_pairs
        assert ("boundary", ENTRY_C["source_url"]) in type_url_pairs

    def test_returns_latest_per_pair(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_B, registry_path=registry_path)

        sources = list_sources(registry_path=registry_path)
        zori = [s for s in sources if s.source_type == "zori"]
        assert len(zori) == 1
        assert zori[0].raw_sha256 == ENTRY_B["raw_sha256"]

    def test_filter_by_source_type(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_C, registry_path=registry_path)

        sources = list_sources(source_type="boundary", registry_path=registry_path)
        assert len(sources) == 1
        assert sources[0].source_type == "boundary"


# ---------------------------------------------------------------------------
# 5. detect_upstream_changes
# ---------------------------------------------------------------------------


class TestDetectUpstreamChanges:
    """Flags sources with multiple distinct hashes."""

    def test_flags_multi_hash_source(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_B, registry_path=registry_path)

        changes = detect_upstream_changes(registry_path=registry_path)
        assert len(changes) == 1
        row = changes.iloc[0]
        assert row["source_type"] == "zori"
        assert row["hash_count"] == 2
        assert row["first_hash"] == ENTRY_A["raw_sha256"]
        assert row["last_hash"] == ENTRY_B["raw_sha256"]

    def test_no_change_when_same_hash(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        register_source(**ENTRY_A, registry_path=registry_path)

        changes = detect_upstream_changes(registry_path=registry_path)
        assert changes.empty

    def test_single_entry_no_change(self, registry_path: Path) -> None:
        register_source(**ENTRY_C, registry_path=registry_path)

        changes = detect_upstream_changes(registry_path=registry_path)
        assert changes.empty


# ---------------------------------------------------------------------------
# 6. Serialization round-trip
# ---------------------------------------------------------------------------


class TestSerialization:
    """SourceRegistryEntry.to_dict() / from_dict() round-trip."""

    def test_round_trip_with_metadata(self) -> None:
        ts = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
        entry = SourceRegistryEntry(
            source_type="zori",
            source_url="https://example.com/zori.csv",
            source_name="ZORI",
            raw_sha256="abc123",
            file_size=999,
            local_path="data/raw/zori.csv",
            ingested_at=ts,
            metadata={"vintage": "2025-01", "notes": "test"},
        )

        d = entry.to_dict()
        restored = SourceRegistryEntry.from_dict(d)

        assert restored.source_type == entry.source_type
        assert restored.source_url == entry.source_url
        assert restored.source_name == entry.source_name
        assert restored.raw_sha256 == entry.raw_sha256
        assert restored.file_size == entry.file_size
        assert restored.local_path == entry.local_path
        assert restored.ingested_at == entry.ingested_at
        assert restored.metadata == entry.metadata

    def test_metadata_serialized_as_json_string(self) -> None:
        entry = SourceRegistryEntry(
            source_type="boundary",
            source_url="https://example.com",
            raw_sha256="xyz",
            ingested_at=datetime.now(UTC),
            metadata={"key": "value"},
        )
        d = entry.to_dict()
        assert isinstance(d["metadata"], str)
        assert json.loads(d["metadata"]) == {"key": "value"}

    def test_empty_metadata_round_trip(self) -> None:
        entry = SourceRegistryEntry(
            source_type="pit",
            source_url="https://example.com/pit",
            raw_sha256="hash",
            ingested_at=datetime.now(UTC),
            metadata={},
        )
        d = entry.to_dict()
        restored = SourceRegistryEntry.from_dict(d)
        assert restored.metadata == {}

    @pytest.mark.parametrize(
        "metadata_input",
        [
            pytest.param("{}", id="json-string"),
            pytest.param("", id="empty-string"),
        ],
    )
    def test_from_dict_handles_string_metadata(self, metadata_input: str) -> None:
        d = {
            "source_type": "other",
            "source_url": "https://example.com",
            "raw_sha256": "h",
            "ingested_at": datetime.now(UTC),
            "metadata": metadata_input,
        }
        entry = SourceRegistryEntry.from_dict(d)
        assert isinstance(entry.metadata, dict)


# ---------------------------------------------------------------------------
# 7. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Empty registry and missing optional fields."""

    def test_empty_registry_get_latest(self, registry_path: Path) -> None:
        result = get_latest_source(source_type="zori", registry_path=registry_path)
        assert result is None

    def test_empty_registry_history(self, registry_path: Path) -> None:
        result = get_source_history(source_type="zori", registry_path=registry_path)
        assert result == []

    def test_empty_registry_list_sources(self, registry_path: Path) -> None:
        result = list_sources(registry_path=registry_path)
        assert result == []

    def test_empty_registry_detect_upstream_changes(self, registry_path: Path) -> None:
        result = detect_upstream_changes(registry_path=registry_path)
        assert result.empty
        expected_cols = {
            "source_type",
            "source_url",
            "hash_count",
            "first_seen",
            "last_seen",
            "first_hash",
            "last_hash",
        }
        assert set(result.columns) == expected_cols

    def test_register_with_defaults(self, registry_path: Path) -> None:
        entry = register_source(
            source_type="other",
            source_url="https://example.com/other",
            raw_sha256="hash_only",
            registry_path=registry_path,
        )
        assert entry.source_name == ""
        assert entry.file_size == 0
        assert entry.local_path == ""
        assert entry.metadata == {}

    def test_get_latest_nonexistent_type(self, registry_path: Path) -> None:
        register_source(**ENTRY_A, registry_path=registry_path)
        result = get_latest_source(source_type="pit", registry_path=registry_path)
        assert result is None

    def test_local_path_accepts_path_object(self, registry_path: Path) -> None:
        entry = register_source(
            source_type="boundary",
            source_url="https://example.com",
            raw_sha256="h",
            local_path=Path("data/raw/boundary/file.zip"),
            registry_path=registry_path,
        )
        assert entry.local_path == "data/raw/boundary/file.zip"


# ---------------------------------------------------------------------------
# 8. Provenance
# ---------------------------------------------------------------------------


class TestProvenance:
    """Registry file has embedded provenance after write."""

    def test_registry_file_has_provenance(self, registry_path: Path) -> None:
        from coclab.provenance import has_provenance

        register_source(**ENTRY_A, registry_path=registry_path)
        assert registry_path.exists()
        assert has_provenance(registry_path)

    def test_provenance_includes_dataset_type(self, registry_path: Path) -> None:
        from coclab.provenance import read_provenance

        register_source(**ENTRY_A, registry_path=registry_path)
        prov = read_provenance(registry_path)
        assert prov is not None
        assert prov.extra["dataset_type"] == "source_registry"

    def test_provenance_entry_count_updates(self, registry_path: Path) -> None:
        from coclab.provenance import read_provenance

        register_source(**ENTRY_A, registry_path=registry_path)
        prov1 = read_provenance(registry_path)
        assert prov1 is not None
        assert prov1.extra["entry_count"] == 1

        register_source(**ENTRY_B, registry_path=registry_path)
        prov2 = read_provenance(registry_path)
        assert prov2 is not None
        assert prov2.extra["entry_count"] == 2
