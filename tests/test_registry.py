"""Tests for the boundary registry module."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from coclab.registry import RegistryEntry, latest_vintage, list_vintages, register_vintage


@pytest.fixture
def temp_registry(tmp_path):
    """Create a temporary registry path."""
    return tmp_path / "test_registry.parquet"


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample parquet file for testing."""
    path = tmp_path / "sample.parquet"
    path.write_bytes(b"fake parquet content for testing")
    return path


class TestRegistryEntry:
    """Tests for RegistryEntry dataclass."""

    def test_to_dict(self):
        entry = RegistryEntry(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            ingested_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            path=Path("data/curated/coc_boundaries__2025.parquet"),
            feature_count=400,
            hash_of_file="abc123",
        )
        d = entry.to_dict()
        assert d["boundary_vintage"] == "2025"
        assert d["source"] == "hud_exchange_gis_tools"
        assert d["ingested_at"] == "2025-01-01T12:00:00+00:00"
        assert d["path"] == "data/curated/coc_boundaries__2025.parquet"
        assert d["feature_count"] == 400
        assert d["hash_of_file"] == "abc123"

    def test_from_dict(self):
        d = {
            "boundary_vintage": "2024",
            "source": "hud_opendata_arcgis",
            "ingested_at": "2024-06-15T10:30:00+00:00",
            "path": "data/curated/coc_boundaries__2024.parquet",
            "feature_count": 395,
            "hash_of_file": "def456",
        }
        entry = RegistryEntry.from_dict(d)
        assert entry.boundary_vintage == "2024"
        assert entry.source == "hud_opendata_arcgis"
        assert entry.ingested_at == datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        assert entry.path == Path("data/curated/coc_boundaries__2024.parquet")
        assert entry.feature_count == 395
        assert entry.hash_of_file == "def456"


class TestRegisterVintage:
    """Tests for register_vintage function."""

    def test_register_new_vintage(self, temp_registry, sample_parquet):
        entry = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
        )
        assert entry.boundary_vintage == "2025"
        assert entry.source == "hud_exchange_gis_tools"
        assert entry.feature_count == 400
        assert entry.hash_of_file is not None
        assert temp_registry.exists()

    def test_register_multiple_vintages(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=390,
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
        )
        entries = list_vintages(registry_path=temp_registry)
        assert len(entries) == 2
        vintages = {e.boundary_vintage for e in entries}
        assert vintages == {"2024", "2025"}

    def test_idempotent_same_hash(self, temp_registry, sample_parquet):
        ingested_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        entry1 = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=ingested_at,
            registry_path=temp_registry,
        )
        # Register again with same content
        entry2 = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 2, 1, 12, 0, 0, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        entries = list_vintages(registry_path=temp_registry)
        assert len(entries) == 1
        # Original timestamp preserved
        assert entries[0].ingested_at == ingested_at

    def test_update_on_hash_change(self, temp_registry, tmp_path):
        # Create two different files
        file1 = tmp_path / "file1.parquet"
        file1.write_bytes(b"content version 1")
        file2 = tmp_path / "file2.parquet"
        file2.write_bytes(b"content version 2 - different")

        entry1 = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=file1,
            feature_count=400,
            registry_path=temp_registry,
        )
        hash1 = entry1.hash_of_file

        entry2 = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=file2,
            feature_count=405,
            registry_path=temp_registry,
        )
        hash2 = entry2.hash_of_file

        assert hash1 != hash2
        entries = list_vintages(registry_path=temp_registry)
        assert len(entries) == 1
        assert entries[0].hash_of_file == hash2
        assert entries[0].feature_count == 405


class TestListVintages:
    """Tests for list_vintages function."""

    def test_empty_registry(self, temp_registry):
        entries = list_vintages(registry_path=temp_registry)
        assert entries == []

    def test_sorted_by_ingested_at_descending(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2023",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=380,
            ingested_at=datetime(2023, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        entries = list_vintages(registry_path=temp_registry)
        assert [e.boundary_vintage for e in entries] == ["2025", "2024", "2023"]


class TestLatestVintage:
    """Tests for latest_vintage function."""

    def test_empty_registry(self, temp_registry):
        result = latest_vintage(registry_path=temp_registry)
        assert result is None

    def test_hud_exchange_prefers_highest_year(self, temp_registry, sample_parquet):
        # Register older year but more recent ingestion
        register_vintage(
            boundary_vintage="2023",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=380,
            ingested_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        result = latest_vintage(
            source="hud_exchange_gis_tools",
            registry_path=temp_registry,
        )
        assert result == "2025"

    def test_opendata_prefers_recent_ingestion(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="HUDOpenData_2024-01-15",
            source="hud_opendata_arcgis",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="HUDOpenData_2024-06-01",
            source="hud_opendata_arcgis",
            path=sample_parquet,
            feature_count=395,
            ingested_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        result = latest_vintage(
            source="hud_opendata_arcgis",
            registry_path=temp_registry,
        )
        assert result == "HUDOpenData_2024-06-01"

    def test_no_source_prefers_hud_exchange_year(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="HUDOpenData_2025-06-01",
            source="hud_opendata_arcgis",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        result = latest_vintage(registry_path=temp_registry)
        # Should prefer hud_exchange with highest year
        assert result == "2025"

    def test_fallback_to_ingested_at_when_no_year(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="snapshot_a",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        register_vintage(
            boundary_vintage="snapshot_b",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
            registry_path=temp_registry,
        )
        result = latest_vintage(
            source="hud_exchange_gis_tools",
            registry_path=temp_registry,
        )
        # Fallback to most recent ingested_at when no year parseable
        assert result == "snapshot_b"

    def test_source_filter_returns_none_if_not_found(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange_gis_tools",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
        )
        result = latest_vintage(
            source="hud_opendata_arcgis",
            registry_path=temp_registry,
        )
        assert result is None
