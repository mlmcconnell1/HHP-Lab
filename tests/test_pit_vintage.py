"""Tests for PIT vintage registry CRUD and parser dataclass coverage.

Covers:
- register_pit_vintage / list_pit_vintages / get_pit_vintage_path
- PITParseResult.duplicates_dropped field
- PITVintageParseResult.years_failed field
- write_pit_parquet provenance metadata for single- and multi-year DataFrames
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from hhplab.pit.ingest.parser import (
    PITParseResult,
    PITVintageParseResult,
    write_pit_parquet,
)
from hhplab.pit.pit_registry import (
    PitVintageRegistryEntry,
    get_pit_vintage_path,
    list_pit_vintages,
    register_pit_vintage,
)
from hhplab.provenance import PROVENANCE_KEY, ProvenanceBlock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pit_df(
    coc_ids: list[str] | None = None,
    totals: list[int] | None = None,
    year: int = 2024,
) -> pd.DataFrame:
    """Create a minimal canonical PIT DataFrame for testing."""
    if coc_ids is None:
        coc_ids = ["CO-500", "CA-600"]
    if totals is None:
        totals = [1000, 50000]
    n = len(coc_ids)
    return pd.DataFrame(
        {
            "pit_year": [year] * n,
            "coc_id": coc_ids,
            "pit_total": totals[:n],
            "pit_sheltered": [int(t * 0.7) for t in totals[:n]],
            "pit_unsheltered": [int(t * 0.3) for t in totals[:n]],
            "data_source": ["hud_exchange"] * n,
            "source_ref": ["https://example.com"] * n,
            "ingested_at": [datetime.now(UTC)] * n,
            "notes": [None] * n,
        }
    )


def _dummy_parquet(tmp_path: Path, name: str = "dummy.parquet") -> Path:
    """Write a tiny parquet file and return its path (for hash computation)."""
    p = tmp_path / name
    pd.DataFrame({"x": [1]}).to_parquet(p, index=False)
    return p


# ---------------------------------------------------------------------------
# 1. Vintage registry CRUD
# ---------------------------------------------------------------------------


class TestVintageRegistryCRUD:
    """register_pit_vintage, list_pit_vintages, get_pit_vintage_path."""

    def test_register_and_list(self, tmp_path: Path):
        reg = tmp_path / "vintage_registry.parquet"
        pq_file = _dummy_parquet(tmp_path)

        entry = register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_file,
            row_count=400,
            years_included=[2007, 2020, 2024],
            registry_path=reg,
        )

        assert isinstance(entry, PitVintageRegistryEntry)
        assert entry.vintage == 2024
        assert entry.years_included == [2007, 2020, 2024]

        entries = list_pit_vintages(registry_path=reg)
        assert len(entries) == 1
        assert entries[0].vintage == 2024
        assert entries[0].row_count == 400

    def test_get_vintage_path_returns_correct_path(self, tmp_path: Path):
        reg = tmp_path / "vintage_registry.parquet"
        pq_file = _dummy_parquet(tmp_path)

        register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_file,
            row_count=100,
            years_included=[2024],
            registry_path=reg,
        )

        result = get_pit_vintage_path(2024, registry_path=reg)
        assert result == pq_file

    def test_get_vintage_path_missing_returns_none(self, tmp_path: Path):
        reg = tmp_path / "vintage_registry.parquet"
        assert get_pit_vintage_path(2099, registry_path=reg) is None

    def test_multiple_vintages(self, tmp_path: Path):
        reg = tmp_path / "vintage_registry.parquet"
        pq_a = _dummy_parquet(tmp_path, "a.parquet")
        pq_b = _dummy_parquet(tmp_path, "b.parquet")

        register_pit_vintage(
            vintage=2023,
            source="hud_user",
            path=pq_a,
            row_count=300,
            years_included=[2007, 2023],
            registry_path=reg,
        )
        register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_b,
            row_count=400,
            years_included=[2007, 2024],
            registry_path=reg,
        )

        entries = list_pit_vintages(registry_path=reg)
        assert len(entries) == 2
        vintages = {e.vintage for e in entries}
        assert vintages == {2023, 2024}

    def test_idempotent_same_hash(self, tmp_path: Path):
        """Re-registering with same hash returns existing entry without doubling."""
        reg = tmp_path / "vintage_registry.parquet"
        pq_file = _dummy_parquet(tmp_path)

        entry1 = register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_file,
            row_count=100,
            years_included=[2024],
            registry_path=reg,
        )
        entry2 = register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_file,
            row_count=100,
            years_included=[2024],
            registry_path=reg,
        )

        assert entry1.hash_of_file == entry2.hash_of_file
        assert len(list_pit_vintages(registry_path=reg)) == 1

    def test_years_included_sorted(self, tmp_path: Path):
        """years_included is stored in sorted order regardless of input."""
        reg = tmp_path / "vintage_registry.parquet"
        pq_file = _dummy_parquet(tmp_path)

        entry = register_pit_vintage(
            vintage=2024,
            source="hud_user",
            path=pq_file,
            row_count=100,
            years_included=[2024, 2007, 2015],
            registry_path=reg,
        )
        assert entry.years_included == [2007, 2015, 2024]


# ---------------------------------------------------------------------------
# 2. PITParseResult.duplicates_dropped
# ---------------------------------------------------------------------------


class TestPITParseResultDuplicatesDropped:
    def test_default_is_empty_list(self):
        result = PITParseResult(df=pd.DataFrame())
        assert result.duplicates_dropped == []
        assert isinstance(result.duplicates_dropped, list)

    def test_populated_duplicates_dropped(self):
        result = PITParseResult(
            df=pd.DataFrame(),
            duplicates_dropped=["CO-500", "CA-600"],
        )
        assert result.duplicates_dropped == ["CO-500", "CA-600"]

    def test_no_duplicates_empty(self):
        """When constructed without duplicates, field is present and empty."""
        result = PITParseResult(
            df=pd.DataFrame(),
            cross_state_mappings={},
            rows_read=10,
            rows_skipped=0,
        )
        assert result.duplicates_dropped == []


# ---------------------------------------------------------------------------
# 3. PITVintageParseResult.years_failed
# ---------------------------------------------------------------------------


class TestPITVintageParseResultYearsFailed:
    def test_default_is_empty_list(self):
        result = PITVintageParseResult(df=pd.DataFrame(), vintage=2024)
        assert result.years_failed == []
        assert isinstance(result.years_failed, list)

    def test_populated_years_failed(self):
        result = PITVintageParseResult(
            df=pd.DataFrame(),
            vintage=2024,
            years_parsed=[2020, 2021, 2023],
            years_failed=[2022],
        )
        assert result.years_failed == [2022]
        assert isinstance(result.years_failed, list)

    def test_years_failed_with_multiple(self):
        result = PITVintageParseResult(
            df=pd.DataFrame(),
            vintage=2024,
            years_parsed=[2020],
            years_failed=[2021, 2022, 2023],
        )
        assert len(result.years_failed) == 3
        assert result.years_failed == [2021, 2022, 2023]

    def test_all_fields_present(self):
        """Verify all fields on PITVintageParseResult are accessible."""
        result = PITVintageParseResult(
            df=pd.DataFrame(),
            vintage=2024,
            years_parsed=[2024],
            years_failed=[],
            cross_state_mappings={"MO-604a": "MO-604"},
            total_rows_read=500,
            total_rows_skipped=3,
        )
        assert result.vintage == 2024
        assert result.years_parsed == [2024]
        assert result.years_failed == []
        assert result.cross_state_mappings == {"MO-604a": "MO-604"}
        assert result.total_rows_read == 500
        assert result.total_rows_skipped == 3


# ---------------------------------------------------------------------------
# 4. write_pit_parquet provenance
# ---------------------------------------------------------------------------


class TestWritePitParquetProvenance:
    def test_single_year_provenance(self, tmp_path: Path):
        df = _make_pit_df(year=2024)
        out = tmp_path / "pit_2024.parquet"

        write_pit_parquet(df, out)

        meta = pq.read_schema(out).metadata
        assert PROVENANCE_KEY in meta

        prov = ProvenanceBlock.from_json(meta[PROVENANCE_KEY].decode("utf-8"))
        assert prov.extra["pit_year"] == 2024
        assert prov.extra["years_included"] == [2024]
        assert prov.extra["row_count"] == len(df)

    def test_multi_year_provenance(self, tmp_path: Path):
        df_2023 = _make_pit_df(year=2023)
        df_2024 = _make_pit_df(year=2024)
        combined = pd.concat([df_2023, df_2024], ignore_index=True)
        out = tmp_path / "pit_vintage.parquet"

        write_pit_parquet(combined, out)

        meta = pq.read_schema(out).metadata
        prov = ProvenanceBlock.from_json(meta[PROVENANCE_KEY].decode("utf-8"))
        assert prov.extra["years_included"] == [2023, 2024]
        # pit_year is None when multiple years present
        assert prov.extra["pit_year"] is None

    def test_provenance_includes_parse_stats(self, tmp_path: Path):
        df = _make_pit_df()
        out = tmp_path / "pit_stats.parquet"

        write_pit_parquet(df, out, rows_read=500, rows_skipped=10)

        meta = pq.read_schema(out).metadata
        prov = ProvenanceBlock.from_json(meta[PROVENANCE_KEY].decode("utf-8"))
        assert prov.extra["rows_read"] == 500
        assert prov.extra["rows_skipped"] == 10

    def test_provenance_cross_state_mappings(self, tmp_path: Path):
        df = _make_pit_df()
        out = tmp_path / "pit_xstate.parquet"
        mappings = {"MO-604a": "MO-604"}

        write_pit_parquet(df, out, cross_state_mappings=mappings)

        meta = pq.read_schema(out).metadata
        prov = ProvenanceBlock.from_json(meta[PROVENANCE_KEY].decode("utf-8"))
        assert prov.extra["cross_state_mappings"] == {"MO-604a": "MO-604"}
