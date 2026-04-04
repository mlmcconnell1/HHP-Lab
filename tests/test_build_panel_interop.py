"""Regression tests for panel assemble internals with aggregate outputs.

Tests that panel loader functions can discover canonical PIT and
measures files.

Covers coclab-26oa. Updated by coclab-gude.6 to remove build-scoped
PIT fallback (the @B suffix path is no longer supported).
"""


import pandas as pd

from coclab.panel.assemble import _load_acs_measures, _load_pit_for_year


class TestPitCanonicalFilenames:
    """Panel loader should discover canonical PIT files."""

    def test_loads_pit_canonical(self, tmp_path):
        """pit__P2024.parquet should be found when pit_dir is provided."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        df = pd.DataFrame({
            "coc_id": ["CO-500", "CO-501"],
            "pit_total": [100, 200],
            "pit_sheltered": [60, 120],
            "pit_unsheltered": [40, 80],
            "pit_year": [2024, 2024],
        })
        df.to_parquet(pit_dir / "pit__P2024.parquet")

        result = _load_pit_for_year(2024, pit_dir=pit_dir)

        assert not result.empty
        assert len(result) == 2
        assert set(result["coc_id"]) == {"CO-500", "CO-501"}

    def test_build_scoped_not_loaded(self, tmp_path):
        """pit__P2024@B2024.parquet should NOT be loaded (deprecated path)."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "pit_total": [100],
        })
        df.to_parquet(pit_dir / "pit__P2024@B2024.parquet")

        result = _load_pit_for_year(2024, pit_dir=pit_dir)
        assert result.empty

    def test_missing_pit_returns_empty(self, tmp_path):
        """Missing PIT file returns empty DataFrame."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()

        result = _load_pit_for_year(2024, pit_dir=pit_dir)
        assert result.empty


class TestMeasuresBoundaryFallback:
    """Panel loader should fall back to boundary-matching measures files."""

    def test_exact_acs_match(self, tmp_path):
        """Exact ACS vintage match should be found."""
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "total_population": [50000],
            "coverage_ratio": [0.95],
        })
        df.to_parquet(measures_dir / "measures__A2023@B2024.parquet")

        # _load_acs_measures(boundary_vintage, acs_vintage, weighting, measures_dir)
        result, _ = _load_acs_measures("2024", "2023", "area", measures_dir)

        assert not result.empty
        assert result["coc_id"].iloc[0] == "CO-500"

    def test_wrong_acs_vintage_returns_empty(self, tmp_path):
        """Regression for coclab-s4hw: a file with a different ACS vintage
        must NOT be silently loaded as a fallback."""
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        # File has A2024, but panel asks for A2023 — boundary is 2024
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "total_population": [50000],
            "coverage_ratio": [0.95],
        })
        df.to_parquet(
            measures_dir / "measures__A2024@B2024xT2020.parquet"
        )

        # boundary_vintage=2024, acs_vintage=2023 (not in any filename)
        result, _ = _load_acs_measures("2024", "2023", "area", measures_dir)

        assert result.empty

    def test_no_match_returns_empty(self, tmp_path):
        """No matching file returns empty DataFrame."""
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        # File for different boundary (B2025, not B2024)
        df = pd.DataFrame({
            "coc_id": ["CO-500"],
            "total_population": [50000],
            "coverage_ratio": [0.95],
        })
        df.to_parquet(measures_dir / "measures__A2023@B2025.parquet")

        # boundary_vintage=2024, acs_vintage=2023 — no B2024 file exists
        result, _ = _load_acs_measures("2024", "2023", "area", measures_dir)

        assert result.empty
