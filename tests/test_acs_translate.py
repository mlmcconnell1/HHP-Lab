"""Tests for ACS tract vintage translation."""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.acs.translate import (
    TranslationStats,
    default_tract_vintage_for_acs,
    get_source_tract_vintage,
    needs_translation,
    translate_acs_to_target_vintage,
    translate_tracts_2010_to_2020,
)
from coclab.census.ingest.tract_relationship import TractRelationshipNotFoundError


class TestGetSourceTractVintage:
    """Tests for get_source_tract_vintage function."""

    def test_pre_2020_acs_uses_2010_geography(self):
        """ACS with end year before 2020 uses 2010 tract geography."""
        assert get_source_tract_vintage("2015-2019") == 2010
        assert get_source_tract_vintage("2014-2018") == 2010
        assert get_source_tract_vintage("2010-2014") == 2010
        assert get_source_tract_vintage("2007-2011") == 2010

    def test_2020_plus_acs_uses_2020_geography(self):
        """ACS with end year 2020 or later uses 2020 tract geography."""
        assert get_source_tract_vintage("2016-2020") == 2020
        assert get_source_tract_vintage("2017-2021") == 2020
        assert get_source_tract_vintage("2019-2023") == 2020
        assert get_source_tract_vintage("2020-2024") == 2020

    def test_single_year_format(self):
        """Single year format also works."""
        assert get_source_tract_vintage("2019") == 2010
        assert get_source_tract_vintage("2020") == 2020
        assert get_source_tract_vintage("2023") == 2020
        assert get_source_tract_vintage("2030") == 2030


class TestDefaultTractVintageForAcs:
    """Tests for default tract vintage mapping from ACS end years."""

    @pytest.mark.parametrize(
        ("acs_vintage", "expected"),
        [
            ("2007-2010", 2010),
            ("2016-2019", 2010),
            ("2017-2020", 2020),
            ("2021-2024", 2020),
            ("2026-2030", 2030),
        ],
    )
    def test_defaults_to_most_recent_decennial(self, acs_vintage, expected):
        assert default_tract_vintage_for_acs(acs_vintage) == expected


class TestNeedsTranslation:
    """Tests for needs_translation function."""

    def test_2010_source_to_2020_target_needs_translation(self):
        """ACS using 2010 geography targeting 2020+ vintage needs translation."""
        assert needs_translation("2015-2019", 2023) is True
        assert needs_translation("2015-2019", "2023") is True
        assert needs_translation("2010-2014", 2020) is True

    def test_2020_source_to_2020_target_no_translation(self):
        """ACS using 2020 geography targeting 2020+ vintage doesn't need translation."""
        assert needs_translation("2019-2023", 2023) is False
        assert needs_translation("2016-2020", 2023) is False
        assert needs_translation("2020-2024", 2024) is False

    def test_2010_source_to_2010_target_no_translation(self):
        """ACS using 2010 geography targeting pre-2020 vintage doesn't need translation."""
        # This is a theoretical case - we don't have pre-2020 target vintages
        assert needs_translation("2015-2019", 2019) is False

    def test_boundary_case_2020(self):
        """Test boundary case at year 2020."""
        # ACS 2020 (2016-2020) uses 2020 geography
        assert needs_translation("2016-2020", 2020) is False
        # ACS 2019 (2015-2019) uses 2010 geography
        assert needs_translation("2015-2019", 2020) is True


class TestTranslationStats:
    """Tests for TranslationStats dataclass."""

    def test_str_representation(self):
        """Test string representation of TranslationStats."""
        stats = TranslationStats(
            input_tracts=1000,
            output_tracts=1050,
            matched_tracts=990,
            unmatched_tracts=10,
            match_rate=0.99,
            input_population=1000000,
            output_population=999000,
            population_delta_pct=-0.1,
        )
        s = str(stats)
        assert "input_tracts=1,000" in s
        assert "output_tracts=1,050" in s
        assert "99.0%" in s


class TestTranslateTracts2010To2020:
    """Tests for translate_tracts_2010_to_2020 function."""

    @pytest.fixture
    def mock_relationship_file(self, tmp_path, monkeypatch):
        """Create a mock tract relationship file."""
        # Create relationship data
        rel_df = pd.DataFrame(
            {
                "tract_geoid_2010": [
                    "01001020100",  # Maps to one 2020 tract
                    "01001020200",  # Splits into two 2020 tracts
                    "01001020200",
                    "01001020300",  # Merges with another 2010 tract
                    "01001020400",  # Merges with 020300
                ],
                "tract_geoid_2020": [
                    "01001020101",  # Single mapping
                    "01001020201",  # Half of 020200
                    "01001020202",  # Other half of 020200
                    "01001020301",  # Merge target
                    "01001020301",  # Same merge target
                ],
                "area_2010_to_2020_weight": [
                    1.0,  # Full mapping
                    0.6,  # 60% goes to 020201
                    0.4,  # 40% goes to 020202
                    0.7,  # 70% contribution from 020300
                    0.3,  # 30% contribution from 020400
                ],
                "area_2020_to_2010_weight": [1.0, 1.0, 1.0, 0.5, 0.5],
            }
        )

        # Save to parquet in expected location
        output_dir = tmp_path / "data" / "curated" / "census"
        output_dir.mkdir(parents=True, exist_ok=True)
        rel_path = output_dir / "tract_relationship__T2010xT2020.parquet"
        rel_df.to_parquet(rel_path)

        # Monkeypatch the load function
        def mock_load():
            return pd.read_parquet(rel_path)

        monkeypatch.setattr(
            "coclab.acs.translate.load_tract_relationship",
            mock_load,
        )

        return rel_df

    def test_single_tract_mapping(self, mock_relationship_file):
        """Test translation of a tract that maps 1:1."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [1000],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        assert len(result) == 1
        assert result.iloc[0]["tract_geoid"] == "01001020101"
        assert result.iloc[0]["total_population"] == 1000
        assert stats.match_rate == 1.0

    def test_tract_split(self, mock_relationship_file):
        """Test translation of a tract that splits into multiple 2020 tracts."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020200"],
                "total_population": [1000],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        # Should produce two rows (one for each 2020 tract)
        assert len(result) == 2

        # Check population was split proportionally
        result = result.sort_values("tract_geoid").reset_index(drop=True)
        assert result.iloc[0]["tract_geoid"] == "01001020201"
        assert result.iloc[0]["total_population"] == pytest.approx(600, rel=0.01)
        assert result.iloc[1]["tract_geoid"] == "01001020202"
        assert result.iloc[1]["total_population"] == pytest.approx(400, rel=0.01)

        # Total population should be preserved
        assert result["total_population"].sum() == pytest.approx(1000, rel=0.01)

    def test_tract_merge(self, mock_relationship_file):
        """Test translation of multiple tracts that merge into one 2020 tract."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020300", "01001020400"],
                "total_population": [1000, 500],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        # Should produce one row (merged)
        assert len(result) == 1
        assert result.iloc[0]["tract_geoid"] == "01001020301"

        # Population should be sum of weighted contributions
        # 1000 * 0.7 + 500 * 0.3 = 700 + 150 = 850
        assert result.iloc[0]["total_population"] == pytest.approx(850, rel=0.01)

    def test_unmatched_tracts_reported(self, mock_relationship_file):
        """Test that unmatched tracts are counted in stats."""
        df = pd.DataFrame(
            {
                "tract_geoid": [
                    "01001020100",  # Matched
                    "99999999999",  # Not in relationship file
                ],
                "total_population": [1000, 500],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        assert stats.input_tracts == 2
        assert stats.matched_tracts == 1
        assert stats.unmatched_tracts == 1
        assert stats.match_rate == 0.5

    def test_preserves_metadata_columns(self, mock_relationship_file):
        """Test that metadata columns are preserved."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [1000],
                "acs_vintage": ["2015-2019"],
                "tract_vintage": ["2019"],  # Will be updated by caller
                "data_source": ["acs_5yr"],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        assert "acs_vintage" in result.columns
        assert result.iloc[0]["acs_vintage"] == "2015-2019"
        assert result.iloc[0]["data_source"] == "acs_5yr"

    def test_moe_propagation(self, mock_relationship_file):
        """Test that margin of error is properly propagated."""
        # For a split: MOE = sqrt(sum(weight^2 * moe^2))
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020200"],
                "total_population": [1000],
                "moe_total_population": [100.0],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        assert len(result) == 2
        assert "moe_total_population" in result.columns

        # For 60% weight: sqrt(0.6^2 * 100^2) = 60
        # For 40% weight: sqrt(0.4^2 * 100^2) = 40
        result = result.sort_values("tract_geoid").reset_index(drop=True)
        assert result.iloc[0]["moe_total_population"] == pytest.approx(60, rel=0.01)
        assert result.iloc[1]["moe_total_population"] == pytest.approx(40, rel=0.01)

    def test_missing_geoid_column_raises(self, mock_relationship_file):
        """Test that missing geoid column raises ValueError."""
        df = pd.DataFrame(
            {
                "wrong_column": ["01001020100"],
                "total_population": [1000],
            }
        )

        with pytest.raises(ValueError, match="Missing required column: tract_geoid"):
            translate_tracts_2010_to_2020(df)

    def test_missing_population_column_raises(self, mock_relationship_file):
        """Test that missing population column raises ValueError."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "wrong_column": [1000],
            }
        )

        with pytest.raises(ValueError, match="Missing required column: total_population"):
            translate_tracts_2010_to_2020(df)


class TestTranslateAcsToTargetVintage:
    """Tests for translate_acs_to_target_vintage function."""

    @pytest.fixture
    def mock_relationship_file(self, tmp_path, monkeypatch):
        """Create a mock tract relationship file."""
        rel_df = pd.DataFrame(
            {
                "tract_geoid_2010": ["01001020100"],
                "tract_geoid_2020": ["01001020101"],
                "area_2010_to_2020_weight": [1.0],
                "area_2020_to_2010_weight": [1.0],
            }
        )

        output_dir = tmp_path / "data" / "curated" / "census"
        output_dir.mkdir(parents=True, exist_ok=True)
        rel_path = output_dir / "tract_relationship__T2010xT2020.parquet"
        rel_df.to_parquet(rel_path)

        def mock_load():
            return pd.read_parquet(rel_path)

        monkeypatch.setattr(
            "coclab.acs.translate.load_tract_relationship",
            mock_load,
        )

        return rel_df

    def test_no_translation_when_not_needed(self, mock_relationship_file):
        """Test that no translation occurs for 2020+ ACS data."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [1000],
            }
        )

        result, stats = translate_acs_to_target_vintage(
            df,
            acs_vintage="2019-2023",  # Uses 2020 geography
            target_tract_vintage=2023,
        )

        assert stats is None  # No translation performed
        assert result is df  # Same object returned

    def test_translation_when_needed(self, mock_relationship_file):
        """Test that translation occurs for pre-2020 ACS data."""
        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [1000],
            }
        )

        result, stats = translate_acs_to_target_vintage(
            df,
            acs_vintage="2015-2019",  # Uses 2010 geography
            target_tract_vintage=2023,
        )

        assert stats is not None
        assert stats.input_tracts == 1
        assert result.iloc[0]["tract_geoid"] == "01001020101"

    def test_raises_when_relationship_file_missing(self, monkeypatch):
        """Test that TractRelationshipNotFoundError is raised when file is missing."""

        def mock_load():
            raise TractRelationshipNotFoundError()

        monkeypatch.setattr(
            "coclab.acs.translate.load_tract_relationship",
            mock_load,
        )

        df = pd.DataFrame(
            {
                "tract_geoid": ["01001020100"],
                "total_population": [1000],
            }
        )

        with pytest.raises(TractRelationshipNotFoundError):
            translate_acs_to_target_vintage(
                df,
                acs_vintage="2015-2019",
                target_tract_vintage=2023,
            )


class TestIntegrationWithRealRelationshipFile:
    """Integration tests using the real tract relationship file.

    These tests are skipped if the relationship file hasn't been ingested.
    """

    def _relationship_file_available(self) -> bool:
        """Check if the relationship file is available."""
        try:
            from coclab.census.ingest.tract_relationship import get_tract_relationship_path

            get_tract_relationship_path()
            return True
        except TractRelationshipNotFoundError:
            return False

    def test_real_translation_preserves_population(self):
        """Test that real translation preserves total population within tolerance."""
        if not self._relationship_file_available():
            pytest.skip("Tract relationship file not available")

        # Create sample data with real 2010 tract GEOIDs from Denver
        df = pd.DataFrame(
            {
                "tract_geoid": [
                    "08031000102",  # Denver 2010 tracts
                    "08031000201",
                    "08031000202",
                ],
                "total_population": [5000, 3000, 4000],
            }
        )

        result, stats = translate_tracts_2010_to_2020(df)

        # Population should be preserved within a small tolerance
        input_pop = df["total_population"].sum()
        output_pop = result["total_population"].sum()
        pct_diff = abs(output_pop - input_pop) / input_pop * 100

        # Allow up to 1% difference due to rounding and unmatched tracts
        assert pct_diff < 1.0, f"Population changed by {pct_diff:.2f}%"
