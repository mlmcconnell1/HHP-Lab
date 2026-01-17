"""Tests for the boundary registry module."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from coclab.registry import (
    RegistryEntry,
    check_registry_health,
    latest_vintage,
    list_boundaries,
    register_vintage,
)
from coclab.registry.registry import _is_temp_path


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
            source="hud_exchange",
            ingested_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
            path=Path("data/curated/coc_boundaries/coc_boundaries__2025.parquet"),
            feature_count=400,
            hash_of_file="abc123",
        )
        d = entry.to_dict()
        assert d["boundary_vintage"] == "2025"
        assert d["source"] == "hud_exchange"
        assert d["ingested_at"] == "2025-01-01T12:00:00+00:00"
        assert d["path"] == "data/curated/coc_boundaries/coc_boundaries__2025.parquet"
        assert d["feature_count"] == 400
        assert d["hash_of_file"] == "abc123"

    def test_from_dict(self):
        d = {
            "boundary_vintage": "2024",
            "source": "hud_opendata",
            "ingested_at": "2024-06-15T10:30:00+00:00",
            "path": "data/curated/coc_boundaries/coc_boundaries__2024.parquet",
            "feature_count": 395,
            "hash_of_file": "def456",
        }
        entry = RegistryEntry.from_dict(d)
        assert entry.boundary_vintage == "2024"
        assert entry.source == "hud_opendata"
        assert entry.ingested_at == datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)
        assert entry.path == Path("data/curated/coc_boundaries/coc_boundaries__2024.parquet")
        assert entry.feature_count == 395
        assert entry.hash_of_file == "def456"


class TestRegisterVintage:
    """Tests for register_vintage function."""

    def test_register_new_vintage(self, temp_registry, sample_parquet):
        entry = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        assert entry.boundary_vintage == "2025"
        assert entry.source == "hud_exchange"
        assert entry.feature_count == 400
        assert entry.hash_of_file is not None
        assert temp_registry.exists()

    def test_register_multiple_vintages(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=390,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        entries = list_boundaries(registry_path=temp_registry)
        assert len(entries) == 2
        vintages = {e.boundary_vintage for e in entries}
        assert vintages == {"2024", "2025"}

    def test_idempotent_same_hash(self, temp_registry, sample_parquet):
        ingested_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=ingested_at,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        # Register again with same content
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 2, 1, 12, 0, 0, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        entries = list_boundaries(registry_path=temp_registry)
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
            source="hud_exchange",
            path=file1,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        hash1 = entry1.hash_of_file

        entry2 = register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=file2,
            feature_count=405,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        hash2 = entry2.hash_of_file

        assert hash1 != hash2
        entries = list_boundaries(registry_path=temp_registry)
        assert len(entries) == 1
        assert entries[0].hash_of_file == hash2
        assert entries[0].feature_count == 405


class TestListVintages:
    """Tests for list_boundaries function."""

    def test_empty_registry(self, temp_registry):
        entries = list_boundaries(registry_path=temp_registry)
        assert entries == []

    def test_sorted_by_ingested_at_descending(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2023",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=380,
            ingested_at=datetime(2023, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        entries = list_boundaries(registry_path=temp_registry)
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
            source="hud_exchange",
            path=sample_parquet,
            feature_count=380,
            ingested_at=datetime(2025, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        result = latest_vintage(
            source="hud_exchange",
            registry_path=temp_registry,
        )
        assert result == "2025"

    def test_opendata_prefers_recent_ingestion(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="HUDOpenData_2024-01-15",
            source="hud_opendata",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 15, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="HUDOpenData_2024-06-01",
            source="hud_opendata",
            path=sample_parquet,
            feature_count=395,
            ingested_at=datetime(2024, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        result = latest_vintage(
            source="hud_opendata",
            registry_path=temp_registry,
        )
        assert result == "HUDOpenData_2024-06-01"

    def test_no_source_prefers_hud_exchange_year(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="HUDOpenData_2025-06-01",
            source="hud_opendata",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2024",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        result = latest_vintage(registry_path=temp_registry)
        # Should prefer hud_exchange with highest year
        assert result == "2025"

    def test_fallback_to_ingested_at_when_no_year(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="snapshot_a",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=390,
            ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        register_vintage(
            boundary_vintage="snapshot_b",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            ingested_at=datetime(2025, 6, 1, tzinfo=UTC),
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        result = latest_vintage(
            source="hud_exchange",
            registry_path=temp_registry,
        )
        # Fallback to most recent ingested_at when no year parseable
        assert result == "snapshot_b"

    def test_source_filter_returns_none_if_not_found(self, temp_registry, sample_parquet):
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        result = latest_vintage(
            source="hud_opendata",
            registry_path=temp_registry,
        )
        assert result is None


class TestTempPathValidation:
    """Tests for temp directory path validation."""

    def test_is_temp_path_var_folders(self):
        """Test detection of macOS temp directories."""
        assert _is_temp_path("/var/folders/ab/cd123/T/tmpXXXXX/file.parquet")
        assert _is_temp_path("/var/folders/zz/zzxyz/file.parquet")

    def test_is_temp_path_tmp(self):
        """Test detection of /tmp paths."""
        assert _is_temp_path("/tmp/tmpXXXXX/file.parquet")
        assert _is_temp_path("/tmp/myfile.parquet")

    def test_is_temp_path_temp(self):
        """Test detection of /temp paths."""
        assert _is_temp_path("/temp/file.parquet")
        # Windows paths with forward slashes are detected
        assert _is_temp_path("C:/temp/file.parquet")

    def test_is_temp_path_system_tempdir(self):
        """Test detection of system temp directory."""
        system_temp = tempfile.gettempdir()
        assert _is_temp_path(f"{system_temp}/file.parquet")

    def test_is_temp_path_normal_paths(self):
        """Test that normal paths are not flagged as temp."""
        assert not _is_temp_path("/Users/matt/data/file.parquet")
        assert not _is_temp_path("data/curated/coc_boundaries/boundaries__2025.parquet")
        assert not _is_temp_path("/home/user/project/data/file.parquet")

    def test_register_vintage_rejects_temp_path(self, temp_registry):
        """Test that register_vintage raises error for temp paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "boundaries.parquet"
            temp_file.write_bytes(b"fake content")

            with pytest.raises(ValueError, match="temporary directory"):
                register_vintage(
                    boundary_vintage="2025",
                    source="hud_exchange",
                    path=temp_file,
                    feature_count=400,
                    registry_path=temp_registry,
                )


class TestRegistryHealthCheck:
    """Tests for registry health check functionality."""

    def test_empty_registry_is_healthy(self, temp_registry):
        """Test that an empty registry is considered healthy."""
        report = check_registry_health(registry_path=temp_registry)
        assert report.is_healthy
        assert len(report.issues) == 0

    def test_healthy_registry(self, temp_registry, sample_parquet):
        """Test that a properly configured registry is healthy."""
        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=sample_parquet,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )
        # Skip temp check since test fixtures use temp directories
        report = check_registry_health(registry_path=temp_registry, _skip_temp_check=True)
        assert report.is_healthy
        assert len(report.issues) == 0

    def test_detects_missing_files(self, temp_registry, tmp_path):
        """Test detection of missing boundary files."""
        # Create a file, register it, then delete it
        temp_file = tmp_path / "boundaries.parquet"
        temp_file.write_bytes(b"fake content")

        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=temp_file,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )

        # Delete the file
        temp_file.unlink()

        # Skip temp check to test MISSING_FILE detection
        report = check_registry_health(registry_path=temp_registry, _skip_temp_check=True)
        assert not report.is_healthy
        assert len(report.issues) == 1
        assert report.issues[0].issue_type == "MISSING_FILE"
        assert report.issues[0].vintage == "2025"

    def test_report_string_representation(self, temp_registry, tmp_path):
        """Test the string representation of the health report."""
        # Create a file, register it, then delete it
        temp_file = tmp_path / "boundaries.parquet"
        temp_file.write_bytes(b"fake content")

        register_vintage(
            boundary_vintage="2025",
            source="hud_exchange",
            path=temp_file,
            feature_count=400,
            registry_path=temp_registry,
            _allow_temp_path=True,
        )

        temp_file.unlink()

        # Skip temp check to test MISSING_FILE string representation
        report = check_registry_health(registry_path=temp_registry, _skip_temp_check=True)
        report_str = str(report)

        assert "MISSING_FILE" in report_str
        assert "2025" in report_str
        assert "hud_exchange" in report_str

    def test_healthy_report_string(self, temp_registry):
        """Test string representation of healthy report."""
        report = check_registry_health(registry_path=temp_registry)
        assert "healthy" in str(report).lower()
        assert "no issues" in str(report).lower()
