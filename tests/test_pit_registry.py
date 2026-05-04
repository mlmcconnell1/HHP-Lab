"""Tests for the PIT registry module."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from hhplab.pit.pit_registry import (
    PitRegistryEntry,
    get_pit_path,
    latest_pit_year,
    list_pit_years,
    register_pit_year,
)


@pytest.fixture
def temp_registry(tmp_path):
    """Create a temporary registry path."""
    return tmp_path / "pit" / "test_pit_registry.parquet"


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample parquet file for testing."""
    path = tmp_path / "sample_pit.parquet"
    path.write_bytes(b"fake parquet content for testing")
    return path


class TestPitRegistryEntry:
    """Tests for PitRegistryEntry dataclass."""

    def test_to_dict(self):
        entry = PitRegistryEntry(
            pit_year=2023,
            source="hud_exchange",
            ingested_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            path=Path("data/curated/pit/pit__2023.parquet"),
            row_count=400,
            hash_of_file="abc123",
        )
        d = entry.to_dict()
        assert d["pit_year"] == 2023
        assert d["source"] == "hud_exchange"
        assert d["ingested_at"] == "2025-01-01T12:00:00+00:00"
        assert d["path"] == "data/curated/pit/pit__2023.parquet"
        assert d["row_count"] == 400
        assert d["hash_of_file"] == "abc123"

    def test_from_dict(self):
        d = {
            "pit_year": 2024,
            "source": "hud_exchange",
            "ingested_at": "2024-06-15T10:30:00+00:00",
            "path": "data/curated/pit/pit__2024.parquet",
            "row_count": 395,
            "hash_of_file": "def456",
        }
        entry = PitRegistryEntry.from_dict(d)
        assert entry.pit_year == 2024
        assert entry.source == "hud_exchange"
        assert entry.path == Path("data/curated/pit/pit__2024.parquet")
        assert entry.row_count == 395


class TestRegisterPitYear:
    """Tests for register_pit_year function."""

    def test_register_new_pit_year(self, temp_registry, sample_parquet):
        entry = register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            registry_path=temp_registry,
        )
        assert entry.pit_year == 2023
        assert entry.source == "hud_exchange"
        assert entry.row_count == 400
        assert entry.hash_of_file is not None
        assert temp_registry.exists()

    def test_register_multiple_pit_years(self, temp_registry, sample_parquet):
        register_pit_year(
            pit_year=2022,
            source="hud_exchange",
            path=sample_parquet,
            row_count=390,
            registry_path=temp_registry,
        )
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            registry_path=temp_registry,
        )
        entries = list_pit_years(registry_path=temp_registry)
        assert len(entries) == 2
        years = {e.pit_year for e in entries}
        assert years == {2022, 2023}

    def test_idempotent_same_hash(self, temp_registry, sample_parquet):
        ingested_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            ingested_at=ingested_at,
            registry_path=temp_registry,
        )
        # Register again with same content
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            ingested_at=datetime(2025, 2, 1, 12, 0, 0, tzinfo=UTC),
            registry_path=temp_registry,
        )
        entries = list_pit_years(registry_path=temp_registry)
        assert len(entries) == 1


class TestListPitYears:
    """Tests for list_pit_years function."""

    def test_empty_registry(self, temp_registry):
        entries = list_pit_years(registry_path=temp_registry)
        assert entries == []


class TestGetPitPath:
    """Tests for get_pit_path function."""

    def test_empty_registry(self, temp_registry):
        result = get_pit_path(2023, registry_path=temp_registry)
        assert result is None

    def test_get_existing_path(self, temp_registry, sample_parquet):
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            registry_path=temp_registry,
        )
        result = get_pit_path(2023, registry_path=temp_registry)
        assert result == sample_parquet


class TestLatestPitYear:
    """Tests for latest_pit_year function."""

    def test_empty_registry(self, temp_registry):
        result = latest_pit_year(registry_path=temp_registry)
        assert result is None

    def test_returns_highest_year(self, temp_registry, sample_parquet):
        register_pit_year(
            pit_year=2021,
            source="hud_exchange",
            path=sample_parquet,
            row_count=380,
            registry_path=temp_registry,
        )
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=sample_parquet,
            row_count=400,
            registry_path=temp_registry,
        )
        register_pit_year(
            pit_year=2022,
            source="hud_exchange",
            path=sample_parquet,
            row_count=390,
            registry_path=temp_registry,
        )
        result = latest_pit_year(registry_path=temp_registry)
        assert result == 2023
