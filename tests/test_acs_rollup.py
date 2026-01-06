"""Tests for ACS tract-to-CoC population rollup engine."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from coclab.acs.rollup import (
    build_coc_population_rollup,
    get_crosswalk_path,
    get_output_path,
    get_tract_population_path,
    rollup_tract_population,
)
from coclab.provenance import read_provenance


# Test fixtures
@pytest.fixture
def sample_tract_population():
    """Sample tract population data for testing."""
    return pd.DataFrame({
        "tract_geoid": [
            "08031001000",  # Full tract in CO-500
            "08031001100",  # Partial tract in CO-500 and CO-501
            "08031001200",  # Full tract in CO-501
            "08031001300",  # Full tract in CO-500
        ],
        "total_population": [5000, 3000, 4000, 2000],
        "acs_vintage": ["2019-2023"] * 4,
        "tract_vintage": ["2023"] * 4,
    })


@pytest.fixture
def sample_crosswalk():
    """Sample crosswalk data for testing."""
    return pd.DataFrame({
        "coc_id": [
            "CO-500",  # tract 001000, full
            "CO-500",  # tract 001100, partial (60%)
            "CO-501",  # tract 001100, partial (40%)
            "CO-501",  # tract 001200, full
            "CO-500",  # tract 001300, full
        ],
        "tract_geoid": [
            "08031001000",
            "08031001100",
            "08031001100",
            "08031001200",
            "08031001300",
        ],
        "area_share": [1.0, 0.6, 0.4, 1.0, 1.0],
        "intersection_area": [100.0, 60.0, 40.0, 100.0, 100.0],
        "boundary_vintage": ["2025"] * 5,
        "tract_vintage": ["2023"] * 5,
    })


class TestRollupTractPopulation:
    """Tests for rollup_tract_population function."""

    def test_aggregation_correctness(self, sample_tract_population, sample_crosswalk):
        """Test that population aggregation is computed correctly."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        # CO-500: 5000*1.0 + 3000*0.6 + 2000*1.0 = 5000 + 1800 + 2000 = 8800
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["coc_population"] == pytest.approx(8800.0)

        # CO-501: 3000*0.4 + 4000*1.0 = 1200 + 4000 = 5200
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["coc_population"] == pytest.approx(5200.0)

    def test_coverage_ratio_computation(self, sample_tract_population, sample_crosswalk):
        """Test that coverage_ratio is computed correctly."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        # coverage_ratio = sum(coc_share) for tracts with population data
        # Since all tracts have population data, coverage_ratio should be 1.0
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["coverage_ratio"] == pytest.approx(1.0)

        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["coverage_ratio"] == pytest.approx(1.0)

    def test_max_tract_contribution_computation(self, sample_tract_population, sample_crosswalk):
        """Test that max_tract_contribution is computed correctly."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        # CO-500: max(5000*1.0, 3000*0.6, 2000*1.0) = max(5000, 1800, 2000) = 5000
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["max_tract_contribution"] == pytest.approx(5000.0)

        # CO-501: max(3000*0.4, 4000*1.0) = max(1200, 4000) = 4000
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["max_tract_contribution"] == pytest.approx(4000.0)

    def test_tract_count_computation(self, sample_tract_population, sample_crosswalk):
        """Test that tract_count is computed correctly."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        # CO-500: 3 tracts (001000, 001100, 001300)
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["tract_count"] == 3

        # CO-501: 2 tracts (001100, 001200)
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["tract_count"] == 2

    def test_weighting_method_recorded(self, sample_tract_population, sample_crosswalk):
        """Test that weighting method is correctly recorded."""
        # Area weighting
        result_area = rollup_tract_population(
            sample_tract_population, sample_crosswalk, weighting="area"
        )
        assert all(result_area["weighting_method"] == "area")

        # Population mass weighting
        result_pop = rollup_tract_population(
            sample_tract_population, sample_crosswalk, weighting="population_mass"
        )
        assert all(result_pop["weighting_method"] == "population_mass")

    def test_invalid_weighting_raises(self, sample_tract_population, sample_crosswalk):
        """Test that invalid weighting method raises ValueError."""
        with pytest.raises(ValueError, match="weighting must be"):
            rollup_tract_population(
                sample_tract_population, sample_crosswalk, weighting="invalid"
            )

    def test_output_schema(self, sample_tract_population, sample_crosswalk):
        """Test that output has correct schema."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        expected_columns = {
            "coc_id",
            "weighting_method",
            "coc_population",
            "coverage_ratio",
            "max_tract_contribution",
            "tract_count",
        }
        assert set(result.columns) == expected_columns

    def test_column_types(self, sample_tract_population, sample_crosswalk):
        """Test that output columns have correct types."""
        result = rollup_tract_population(sample_tract_population, sample_crosswalk)

        assert result["coc_id"].dtype == object  # str
        assert result["weighting_method"].dtype == object  # str
        assert result["coc_population"].dtype == float
        assert result["coverage_ratio"].dtype == float
        assert result["max_tract_contribution"].dtype == float
        assert result["tract_count"].dtype in (int, "int64", "int32")


class TestMissingDataEdgeCases:
    """Tests for edge cases with missing data."""

    def test_missing_tract_in_crosswalk(self):
        """Test handling when crosswalk has tracts not in population data."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000"],
            "total_population": [5000],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500"],
            "tract_geoid": ["08031001000", "08031009999"],  # 009999 not in tract_pop
            "area_share": [1.0, 0.5],
            "intersection_area": [100.0, 50.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        # Should only count tract with population data
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["coc_population"] == pytest.approx(5000.0)
        # Coverage ratio = coc_share for tract with data = 100/(100+50) = 0.667
        assert co500["coverage_ratio"] == pytest.approx(100.0 / 150.0)
        # Only 1 tract with non-zero contribution
        assert co500["tract_count"] == 1

    def test_tract_with_na_population(self):
        """Test handling when tract has NA population value."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000", "08031001100"],
            "total_population": [5000, pd.NA],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500"],
            "tract_geoid": ["08031001000", "08031001100"],
            "area_share": [1.0, 0.5],
            "intersection_area": [100.0, 50.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        # Only tract 001000 contributes
        assert co500["coc_population"] == pytest.approx(5000.0)
        # Coverage = coc_share for tract with data = 100/(100+50) = 0.667
        assert co500["coverage_ratio"] == pytest.approx(100.0 / 150.0)

    def test_tract_with_zero_population(self):
        """Test handling when tract has zero population."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000", "08031001100"],
            "total_population": [5000, 0],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500"],
            "tract_geoid": ["08031001000", "08031001100"],
            "area_share": [1.0, 1.0],
            "intersection_area": [100.0, 100.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["coc_population"] == pytest.approx(5000.0)
        # Coverage includes both tracts (both have non-NA population)
        # coc_share = 100/200 + 100/200 = 1.0
        assert co500["coverage_ratio"] == pytest.approx(1.0)
        # Only 1 tract with non-zero contribution
        assert co500["tract_count"] == 1

    def test_empty_coc_no_tracts(self):
        """Test that CoC with no matching tracts returns zero population."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000"],
            "total_population": [5000],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500", "CO-501"],
            "tract_geoid": ["08031001000", "08031009999"],  # CO-501 has no matching tract
            "area_share": [1.0, 1.0],
            "intersection_area": [100.0, 100.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        # CO-501 should have zero population
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["coc_population"] == pytest.approx(0.0)
        assert co501["tract_count"] == 0


class TestInputValidation:
    """Tests for input validation."""

    def test_missing_tract_geoid_column(self):
        """Test that missing tract_geoid column raises error."""
        tract_pop = pd.DataFrame({
            "GEOID": ["08031001000"],  # Wrong column name
            "total_population": [5000],
        })
        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500"],
            "tract_geoid": ["08031001000"],
            "area_share": [1.0],
            "intersection_area": [100.0],
        })

        with pytest.raises(ValueError, match="tract_geoid"):
            rollup_tract_population(tract_pop, crosswalk)

    def test_missing_total_population_column(self):
        """Test that missing total_population column raises error."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000"],
            "population": [5000],  # Wrong column name
        })
        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500"],
            "tract_geoid": ["08031001000"],
            "area_share": [1.0],
            "intersection_area": [100.0],
        })

        with pytest.raises(ValueError, match="total_population"):
            rollup_tract_population(tract_pop, crosswalk)

    def test_missing_coc_id_column(self):
        """Test that missing coc_id column raises error."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000"],
            "total_population": [5000],
        })
        crosswalk = pd.DataFrame({
            "coc_number": ["CO-500"],  # Wrong column name
            "tract_geoid": ["08031001000"],
            "area_share": [1.0],
        })

        with pytest.raises(ValueError, match="coc_id"):
            rollup_tract_population(tract_pop, crosswalk)

    def test_missing_area_share_column(self):
        """Test that missing area_share column raises error."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["08031001000"],
            "total_population": [5000],
        })
        crosswalk = pd.DataFrame({
            "coc_id": ["CO-500"],
            "tract_geoid": ["08031001000"],
            "weight": [1.0],  # Wrong column name
        })

        with pytest.raises(ValueError, match="area_share"):
            rollup_tract_population(tract_pop, crosswalk)


class TestPathHelpers:
    """Tests for path helper functions."""

    def test_get_tract_population_path_default(self):
        """Test default tract population path."""
        path = get_tract_population_path("2019-2023", "2023")
        assert path == Path("data/curated/acs/tract_population__2019-2023__2023.parquet")

    def test_get_tract_population_path_custom(self):
        """Test custom tract population path."""
        path = get_tract_population_path("2019-2023", "2023", base_dir="/tmp/test")
        assert path == Path("/tmp/test/tract_population__2019-2023__2023.parquet")

    def test_get_crosswalk_path_default(self):
        """Test default crosswalk path."""
        path = get_crosswalk_path("2025", "2023")
        assert path == Path("data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet")

    def test_get_crosswalk_path_custom(self):
        """Test custom crosswalk path."""
        path = get_crosswalk_path("2025", "2023", base_dir="/tmp/xwalks")
        assert path == Path("/tmp/xwalks/coc_tract_xwalk__2025__2023.parquet")

    def test_get_output_path_default(self):
        """Test default output path."""
        path = get_output_path("2025", "2019-2023", "2023", "area")
        assert path == Path(
            "data/curated/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet"
        )

    def test_get_output_path_population_mass(self):
        """Test output path with population_mass weighting."""
        path = get_output_path("2025", "2019-2023", "2023", "population_mass")
        assert path == Path(
            "data/curated/acs/coc_population_rollup__2025__2019-2023__2023__population_mass.parquet"
        )

    def test_get_output_path_custom(self):
        """Test custom output path."""
        path = get_output_path("2025", "2019-2023", "2023", "area", base_dir="/tmp/out")
        assert path == Path(
            "/tmp/out/coc_population_rollup__2025__2019-2023__2023__area.parquet"
        )


class TestBuildCocPopulationRollup:
    """Tests for build_coc_population_rollup function."""

    def test_creates_output_file(self, sample_tract_population, sample_crosswalk, tmp_path):
        """Test that build creates the output parquet file."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        assert output_path.exists()
        assert output_path.suffix == ".parquet"

    def test_output_schema(self, sample_tract_population, sample_crosswalk, tmp_path):
        """Test that output file has correct schema."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        # Read and check schema
        df = pd.read_parquet(output_path)
        expected_columns = [
            "coc_id",
            "boundary_vintage",
            "acs_vintage",
            "tract_vintage",
            "weighting_method",
            "coc_population",
            "coverage_ratio",
            "max_tract_contribution",
            "tract_count",
        ]
        assert list(df.columns) == expected_columns

    def test_vintage_columns_populated(self, sample_tract_population, sample_crosswalk, tmp_path):
        """Test that vintage columns are correctly populated."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        df = pd.read_parquet(output_path)
        assert all(df["boundary_vintage"] == "2025")
        assert all(df["acs_vintage"] == "2019-2023")
        assert all(df["tract_vintage"] == "2023")

    def test_includes_provenance_metadata(
        self, sample_tract_population, sample_crosswalk, tmp_path
    ):
        """Test that output file includes provenance metadata."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        # Read provenance
        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.boundary_vintage == "2025"
        assert provenance.acs_vintage == "2019-2023"
        assert provenance.tract_vintage == "2023"
        assert provenance.weighting == "area"
        assert provenance.extra.get("dataset") == "coc_population_rollup"

    def test_uses_cache_when_exists(self, sample_tract_population, sample_crosswalk, tmp_path):
        """Test that cached file is used when it exists."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup first time
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        # Get modification time
        mtime1 = output_path.stat().st_mtime

        # Build again without force - should use cache
        output_path2 = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        mtime2 = output_path2.stat().st_mtime
        assert mtime1 == mtime2  # File wasn't modified

    def test_force_rebuild(self, sample_tract_population, sample_crosswalk, tmp_path):
        """Test that force=True rebuilds even with cache."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        xwalk_dir = tmp_path / "xwalks"
        acs_dir.mkdir()
        xwalk_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"

        sample_tract_population.to_parquet(tract_pop_path)
        sample_crosswalk.to_parquet(xwalk_path)

        # Build rollup first time
        output_path = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        # Get modification time
        mtime1 = output_path.stat().st_mtime

        # Wait a tiny bit to ensure different mtime
        import time
        time.sleep(0.1)

        # Build with force=True
        output_path2 = build_coc_population_rollup(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            force=True,
            acs_dir=acs_dir,
            xwalk_dir=xwalk_dir,
            output_dir=acs_dir,
        )

        mtime2 = output_path2.stat().st_mtime
        assert mtime2 > mtime1  # File was rebuilt

    def test_missing_tract_population_file_raises(self, sample_crosswalk, tmp_path):
        """Test that missing tract population file raises FileNotFoundError."""
        xwalk_dir = tmp_path / "xwalks"
        xwalk_dir.mkdir()

        xwalk_path = xwalk_dir / "coc_tract_xwalk__2025__2023.parquet"
        sample_crosswalk.to_parquet(xwalk_path)

        with pytest.raises(FileNotFoundError, match="Tract population file not found"):
            build_coc_population_rollup(
                boundary_vintage="2025",
                acs_vintage="2019-2023",
                tract_vintage="2023",
                acs_dir=tmp_path / "acs",
                xwalk_dir=xwalk_dir,
                output_dir=tmp_path,  # Avoid using cached output
            )

    def test_missing_crosswalk_file_raises(self, sample_tract_population, tmp_path):
        """Test that missing crosswalk file raises FileNotFoundError."""
        acs_dir = tmp_path / "acs"
        acs_dir.mkdir()

        tract_pop_path = acs_dir / "tract_population__2019-2023__2023.parquet"
        sample_tract_population.to_parquet(tract_pop_path)

        with pytest.raises(FileNotFoundError, match="Crosswalk file not found"):
            build_coc_population_rollup(
                boundary_vintage="2025",
                acs_vintage="2019-2023",
                tract_vintage="2023",
                acs_dir=acs_dir,
                xwalk_dir=tmp_path / "xwalks",
                output_dir=tmp_path,  # Avoid using cached output
            )


class TestAggregationMathematicalProperties:
    """Tests for mathematical properties of aggregation."""

    def test_total_population_conservation(self):
        """Test total population is conserved when tracts fully belong to one CoC."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["001", "002", "003"],
            "total_population": [1000, 2000, 3000],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["A", "A", "B"],
            "tract_geoid": ["001", "002", "003"],
            "area_share": [1.0, 1.0, 1.0],
            "intersection_area": [100.0, 100.0, 100.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        # Total population should equal sum of tract populations
        total_coc_pop = result["coc_population"].sum()
        total_tract_pop = tract_pop["total_population"].sum()
        assert total_coc_pop == pytest.approx(total_tract_pop)

    def test_partial_overlap_reduces_total(self):
        """Test that partial overlap reduces attributed population correctly."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["001"],
            "total_population": [1000],
        })

        crosswalk = pd.DataFrame({
            "coc_id": ["A", "B"],
            "tract_geoid": ["001", "001"],
            "area_share": [0.7, 0.3],  # Tract split between two CoCs
            "intersection_area": [70.0, 30.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        # CoC A gets 70%, CoC B gets 30%
        coc_a = result[result["coc_id"] == "A"].iloc[0]
        coc_b = result[result["coc_id"] == "B"].iloc[0]

        assert coc_a["coc_population"] == pytest.approx(700.0)
        assert coc_b["coc_population"] == pytest.approx(300.0)

        # Total should still equal original tract population
        total = result["coc_population"].sum()
        assert total == pytest.approx(1000.0)

    def test_area_share_greater_than_one_allowed(self):
        """Test that area_share > 1 is allowed (for overlapping boundaries)."""
        tract_pop = pd.DataFrame({
            "tract_geoid": ["001"],
            "total_population": [1000],
        })

        # In real data, a tract might be counted in multiple overlapping regions
        crosswalk = pd.DataFrame({
            "coc_id": ["A", "B"],
            "tract_geoid": ["001", "001"],
            "area_share": [1.0, 1.0],  # Same tract fully in both (overlapping)
            "intersection_area": [100.0, 100.0],
        })

        result = rollup_tract_population(tract_pop, crosswalk)

        # Both CoCs get full population (overlap scenario)
        coc_a = result[result["coc_id"] == "A"].iloc[0]
        coc_b = result[result["coc_id"] == "B"].iloc[0]

        assert coc_a["coc_population"] == pytest.approx(1000.0)
        assert coc_b["coc_population"] == pytest.approx(1000.0)
