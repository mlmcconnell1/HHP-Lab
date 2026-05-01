"""Characterization and validation tests for panel output (legacy build path).

.. deprecated::
    These tests exercise the legacy ``build_panel`` function.  Panel
    output contract validation through the recipe executor is covered in
    ``test_recipe.py`` and ``test_recipe_panel_policies.py``.

These tests assert structural invariants, dtype contracts, value domain
constraints, cross-column consistency, uniqueness guarantees, round-trip
fidelity, and statistical fingerprints on the output of build_panel().

Unlike the functional tests in test_panel_assemble.py (which test
individual functions with targeted inputs), these tests treat the panel
as a data product and validate its contract with downstream consumers.

Fixture design
--------------
All fixture data is defined declaratively in the FIXTURE_* constants below.
Golden-value tests derive their expectations from these constants, so
modifying a fixture value automatically updates the expected outcomes.
An agent changing fixture data does NOT need to hand-recompute golden values.

ZORI eligibility truth table (threshold = 0.90):

    CoC     | ZORI  | Coverage       | Eligible? | Exclusion reason
    --------|-------|----------------|-----------|------------------
    CO-500  | 1500  | 0.95 (all yrs) | Yes       | —
    CA-600  | 2400  | 0.98 (all yrs) | Yes       | —
    NY-501  | 2800  | 0.85-0.87      | No        | low_coverage
    TX-500  | null  | 0.00           | No        | missing (null zori)

Beads:
- coclab-11w1: Panel output dtype contract tests
- coclab-1c8l: Panel output domain/value range validation tests
- coclab-5mzo: Panel output cross-column consistency tests
- coclab-tsnq: Panel uniqueness and mandatory non-null tests
- coclab-vn1l: Parquet round-trip dtype fidelity tests
- coclab-3qyl: Panel statistical fingerprint tests
"""

from __future__ import annotations

import re

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

pytestmark = pytest.mark.legacy_build_path

from hhplab.panel.assemble import (
    PANEL_COLUMNS,
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    build_panel,
    save_panel,
)

# ============================================================================
# Fixture constants — single source of truth for all test expectations
# ============================================================================

COC_IDS = ["CO-500", "CA-600", "NY-501", "TX-500"]
YEARS = [2022, 2023, 2024]
ZORI_MIN_COVERAGE = 0.90

# PIT data per CoC.  Key = coc_id, value = {year: pit_total}.
# CO-500 has a year-varying total; others are constant across years.
FIXTURE_PIT = {
    "CO-500": {y: 1200 + (y - YEARS[0]) * 50 for y in YEARS},
    "CA-600": {y: 45000 for y in YEARS},
    "NY-501": {y: 75000 for y in YEARS},
    "TX-500": {y: 3000 for y in YEARS},
}

# ACS coverage ratios per CoC (constant across years in fixture).
FIXTURE_ACS_COVERAGE = {"CO-500": 0.95, "CA-600": 0.98, "NY-501": 0.99, "TX-500": 0.92}

# ACS median household income per CoC (constant across years in fixture).
FIXTURE_ACS_INCOME = {"CO-500": 65000, "CA-600": 75000, "NY-501": 85000, "TX-500": 55000}

# ZORI fixture: (zori_coc, base_coverage).
# NY-501 coverage increases slightly per year but stays below threshold.
# TX-500 has null ZORI and zero coverage.
FIXTURE_ZORI = {
    "CO-500": {"zori_coc": 1500.0, "base_coverage": 0.95},
    "CA-600": {"zori_coc": 2400.0, "base_coverage": 0.98},
    "NY-501": {"zori_coc": 2800.0, "base_coverage": 0.85},  # +0.01/yr
    "TX-500": {"zori_coc": None, "base_coverage": 0.0},
}

# Derived: which CoCs are ZORI-eligible at the default threshold?
ZORI_ELIGIBLE_COCS = {
    coc for coc, v in FIXTURE_ZORI.items()
    if v["zori_coc"] is not None and v["base_coverage"] >= ZORI_MIN_COVERAGE
}
ZORI_INELIGIBLE_COCS = set(COC_IDS) - ZORI_ELIGIBLE_COCS

# Columns whose in-memory dtype is object but normalizes through parquet.
# rent_to_income: compute_rent_to_income() inits with None -> object,
#   parquet coerces to float64.
# zori_excluded_reason: mixed None/str -> parquet may normalize.
# When adding a new column with this behavior, add it here so the
# round-trip test doesn't produce a confusing failure.
PARQUET_NORMALIZE_OK = {"rent_to_income", "zori_excluded_reason"}

FIXTURE_COC_AREA_SQ_KM = {
    "CO-500": 100.0,
    "CA-600": 400.0,
    "NY-501": 225.0,
    "TX-500": 625.0,
}


# ============================================================================
# Shared Fixtures
# ============================================================================


@pytest.fixture
def panel_data_dirs(tmp_path):
    """Create a complete set of PIT, ACS, and ZORI test data.

    Produces a balanced panel: len(COC_IDS) x len(YEARS) rows.
    See FIXTURE_* constants and the truth table in the module docstring
    for the eligibility design.
    """
    pit_dir = tmp_path / "pit"
    pit_dir.mkdir()
    measures_dir = tmp_path / "measures"
    measures_dir.mkdir()
    rents_dir = tmp_path / "rents"
    rents_dir.mkdir()
    boundaries_dir = tmp_path / "coc_boundaries"
    boundaries_dir.mkdir()

    # --- PIT data ---
    for year in YEARS:
        df_pit = pd.DataFrame({
            "coc_id": COC_IDS,
            "pit_total": [FIXTURE_PIT[c][year] for c in COC_IDS],
            "pit_sheltered": [800, 30000, 55000, 2000],
            "pit_unsheltered": [
                400 + (year - YEARS[0]) * 50, 15000, 20000, 1000,
            ],
            "pit_year": [year] * len(COC_IDS),
        })
        df_pit.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

    # --- ACS measures (one file per year, keyed to default policy) ---
    for acs_year in [y - 1 for y in YEARS]:
        boundary_year = acs_year + 1
        df_acs = pd.DataFrame({
            "coc_id": COC_IDS,
            "total_population": [500000, 10000000, 8000000, 2000000],
            "adult_population": [400000, 8000000, 6400000, 1600000],
            "population_below_poverty": [50000, 1500000, 1200000, 300000],
            "median_household_income": [FIXTURE_ACS_INCOME[c] for c in COC_IDS],
            "median_gross_rent": [1200, 1800, 2200, 1000],
            "coverage_ratio": [FIXTURE_ACS_COVERAGE[c] for c in COC_IDS],
            "weighting_method": ["population"] * len(COC_IDS),
        })
        df_acs.to_parquet(
            measures_dir / f"coc_measures__{boundary_year}__{acs_year}.parquet",
            index=False,
        )
        side_lengths_m = {
            coc_id: (FIXTURE_COC_AREA_SQ_KM[coc_id] * 1_000_000.0) ** 0.5
            for coc_id in COC_IDS
        }
        gpd.GeoDataFrame(
            {"coc_id": COC_IDS},
            geometry=[
                box(0, 0, side_lengths_m[coc_id], side_lengths_m[coc_id])
                for coc_id in COC_IDS
            ],
            crs="ESRI:102003",
        ).to_parquet(
            boundaries_dir / f"coc__B{boundary_year}.parquet",
            index=False,
        )

    # --- ZORI yearly data ---
    zori_rows = []
    for year in YEARS:
        for coc_id in COC_IDS:
            z = FIXTURE_ZORI[coc_id]
            coverage = z["base_coverage"]
            # NY-501 gets +0.01 per year offset
            if coc_id == "NY-501":
                coverage += (year - YEARS[0]) * 0.01
            zori_rows.append({
                "coc_id": coc_id,
                "year": year,
                "zori_coc": z["zori_coc"],
                "coverage_ratio": coverage,
                "max_geo_contribution": 0.40 if z["zori_coc"] is not None else None,
                "method": "pit_january",
                "geo_count": 5 if z["zori_coc"] is not None else 0,
            })
    pd.DataFrame(zori_rows).to_parquet(
        rents_dir / "coc_zori_yearly__test.parquet", index=False,
    )

    return {
        "pit_dir": pit_dir,
        "measures_dir": measures_dir,
        "rents_dir": rents_dir,
        "boundaries_dir": boundaries_dir,
        "zori_path": rents_dir / "coc_zori_yearly__test.parquet",
    }


@pytest.fixture
def base_panel(panel_data_dirs):
    """Build a panel without ZORI."""
    return build_panel(
        YEARS[0],
        YEARS[-1],
        pit_dir=panel_data_dirs["pit_dir"],
        measures_dir=panel_data_dirs["measures_dir"],
    )


@pytest.fixture
def zori_panel(panel_data_dirs):
    """Build a panel with ZORI integration."""
    return build_panel(
        YEARS[0],
        YEARS[-1],
        pit_dir=panel_data_dirs["pit_dir"],
        measures_dir=panel_data_dirs["measures_dir"],
        include_zori=True,
        zori_yearly_path=panel_data_dirs["zori_path"],
        rents_dir=panel_data_dirs["rents_dir"],
        zori_min_coverage=ZORI_MIN_COVERAGE,
    )


# ============================================================================
# coclab-11w1: Panel output dtype contract tests
# ============================================================================

# Columns with a single guaranteed dtype regardless of null presence.
STRICT_DTYPE_MAP = {
    "coc_id": "object",
    "year": "int64",
    "pit_total": "int64",
    "pit_sheltered": "Int64",
    "pit_unsheltered": "Int64",
    "boundary_vintage_used": "object",
    "acs5_vintage_used": "object",
    "tract_vintage_used": "string",
    "alignment_type": "string",
    "weighting_method": "object",
    "boundary_changed": "bool",
    "source": "object",
}

# ACS numeric columns may be int64 or float64 depending on null presence.
NUMERIC_COLUMNS = [
    "total_population",
    "population_density_per_sq_km",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "coverage_ratio",
]


class TestPanelOutputDtypes:
    """Assert the dtype contract of build_panel() output."""

    @pytest.mark.parametrize("col,expected_dtype", list(STRICT_DTYPE_MAP.items()))
    def test_base_panel_strict_dtype(self, base_panel, col, expected_dtype):
        """Column has its contracted dtype."""
        assert col in base_panel.columns, f"Missing column: {col}"
        actual = str(base_panel[col].dtype)
        assert actual == expected_dtype

    @pytest.mark.parametrize("col", NUMERIC_COLUMNS)
    def test_base_panel_numeric_dtype(self, base_panel, col):
        """ACS numeric column is int64 or float64."""
        assert col in base_panel.columns, f"Missing column: {col}"
        assert base_panel[col].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize("col,expected_dtype", list(STRICT_DTYPE_MAP.items()))
    def test_zori_panel_strict_dtype(self, zori_panel, col, expected_dtype):
        """ZORI panel preserves strict column dtype."""
        if col not in zori_panel.columns:
            pytest.skip(f"{col} not in ZORI panel")
        actual = str(zori_panel[col].dtype)
        assert actual == expected_dtype

    @pytest.mark.parametrize("col", NUMERIC_COLUMNS)
    def test_zori_panel_numeric_dtype(self, zori_panel, col):
        """ZORI panel preserves numeric column dtype family."""
        if col not in zori_panel.columns:
            pytest.skip(f"{col} not in ZORI panel")
        assert zori_panel[col].dtype in [np.int64, np.float64]

    @pytest.mark.parametrize(
        "col,expected_dtype",
        [
            ("zori_coc", np.float64),
            ("zori_coverage_ratio", np.float64),
            ("zori_is_eligible", bool),
            ("zori_min_coverage", np.float64),
        ],
    )
    def test_zori_specific_dtype(self, zori_panel, col, expected_dtype):
        """ZORI-specific columns have expected dtypes."""
        assert zori_panel[col].dtype == expected_dtype

    def test_base_panel_has_exactly_canonical_columns(self, base_panel):
        """Base panel has exactly the canonical column set, no extras."""
        assert set(base_panel.columns) == set(PANEL_COLUMNS)

    def test_zori_panel_has_expected_column_superset(self, zori_panel):
        """ZORI panel has base columns plus ZORI and provenance columns."""
        expected = set(PANEL_COLUMNS) | set(ZORI_COLUMNS) | set(ZORI_PROVENANCE_COLUMNS)
        actual = set(zori_panel.columns)
        allowed_extra = {"zori_max_geo_contribution"}
        unexpected = actual - expected - allowed_extra
        assert not unexpected, f"Unexpected columns: {unexpected}"
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"


# ============================================================================
# coclab-1c8l: Panel output domain/value range validation tests
# ============================================================================


class TestPanelValueRanges:
    """Assert that output values fall within legal domain ranges."""

    @pytest.mark.parametrize("col", ["pit_total", "pit_sheltered", "pit_unsheltered"])
    def test_pit_columns_non_negative(self, base_panel, col):
        valid = base_panel[col].dropna()
        assert (valid >= 0).all(), f"{col} has negative values"

    @pytest.mark.parametrize("col", ["coverage_ratio"])
    def test_base_ratio_in_unit_interval(self, base_panel, col):
        valid = base_panel[col].dropna()
        assert (valid >= 0).all() and (valid <= 1).all(), f"{col} outside [0,1]"

    @pytest.mark.parametrize("col", ["zori_coverage_ratio"])
    def test_zori_ratio_in_unit_interval(self, zori_panel, col):
        valid = zori_panel[col].dropna()
        assert (valid >= 0).all() and (valid <= 1).all(), f"{col} outside [0,1]"

    def test_rent_to_income_non_negative(self, zori_panel):
        valid = zori_panel["rent_to_income"].dropna()
        assert (valid >= 0).all()

    def test_coc_id_format(self, base_panel):
        """All coc_id values match the ST-NNN pattern."""
        pattern = re.compile(r"^[A-Z]{2}-[0-9]{3}$")
        assert base_panel["coc_id"].apply(lambda x: bool(pattern.match(x))).all()

    @pytest.mark.parametrize("col", ["median_household_income", "median_gross_rent"])
    def test_monetary_column_positive(self, base_panel, col):
        """Monetary column is positive where present."""
        valid = base_panel[col].dropna()
        if len(valid) > 0:
            assert (valid > 0).all()

    def test_population_density_positive(self, base_panel):
        """Population density is positive where present."""
        valid = base_panel["population_density_per_sq_km"].dropna()
        if len(valid) > 0:
            assert (valid > 0).all()

    @pytest.mark.parametrize(
        "col", ["total_population", "adult_population", "population_below_poverty"],
    )
    def test_population_column_non_negative(self, base_panel, col):
        valid = base_panel[col].dropna()
        if len(valid) > 0:
            assert (valid >= 0).all()

    def test_year_in_plausible_range(self, base_panel):
        assert (base_panel["year"] >= 2007).all()
        assert (base_panel["year"] <= 2030).all()

    def test_zori_coc_positive_where_present(self, zori_panel):
        valid = zori_panel["zori_coc"].dropna()
        if len(valid) > 0:
            assert (valid > 0).all()


# ============================================================================
# coclab-5mzo: Panel output cross-column consistency tests
# ============================================================================


class TestPanelCrossColumnConsistency:
    """Assert cross-column invariants on the output panel."""

    @pytest.mark.parametrize(
        "larger_col,smaller_col",
        [
            ("total_population", "adult_population"),
            ("total_population", "population_below_poverty"),
            ("pit_total", "pit_sheltered"),
            ("pit_total", "pit_unsheltered"),
        ],
    )
    def test_column_ordering(self, base_panel, larger_col, smaller_col):
        """larger_col >= smaller_col where both are present."""
        mask = base_panel[larger_col].notna() & base_panel[smaller_col].notna()
        subset = base_panel[mask]
        if len(subset) > 0:
            assert (subset[larger_col] >= subset[smaller_col]).all()

    def test_ineligible_zori_has_null_rent_to_income(self, zori_panel):
        """If zori_is_eligible is False, rent_to_income must be null."""
        ineligible = zori_panel[~zori_panel["zori_is_eligible"]]
        if len(ineligible) > 0:
            assert ineligible["rent_to_income"].isna().all()

    def test_eligible_zori_has_non_null_zori_coc(self, zori_panel):
        """If zori_is_eligible is True, zori_coc must not be null."""
        eligible = zori_panel[zori_panel["zori_is_eligible"]]
        if len(eligible) > 0:
            assert eligible["zori_coc"].notna().all()

    def test_ineligible_zori_has_null_zori_coc(self, zori_panel):
        """If zori_is_eligible is False, zori_coc must be null."""
        ineligible = zori_panel[~zori_panel["zori_is_eligible"]]
        if len(ineligible) > 0:
            assert ineligible["zori_coc"].isna().all()

    def test_boundary_changed_false_for_first_year(self, base_panel):
        """boundary_changed is False for the first year of each CoC."""
        for coc_id in base_panel["coc_id"].unique():
            coc_data = base_panel[base_panel["coc_id"] == coc_id].sort_values("year")
            assert not coc_data.iloc[0]["boundary_changed"], (
                f"boundary_changed should be False for first year of {coc_id}"
            )

    def test_panel_is_sorted(self, base_panel):
        """Panel output is sorted by (coc_id, year)."""
        sorted_df = base_panel.sort_values(["coc_id", "year"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(base_panel.reset_index(drop=True), sorted_df)

    def test_ineligible_has_exclusion_reason(self, zori_panel):
        """If zori_is_eligible is False, zori_excluded_reason must be non-null."""
        ineligible = zori_panel[~zori_panel["zori_is_eligible"]]
        if len(ineligible) > 0:
            assert ineligible["zori_excluded_reason"].notna().all()

    def test_eligible_has_no_exclusion_reason(self, zori_panel):
        """If zori_is_eligible is True, zori_excluded_reason must be null."""
        eligible = zori_panel[zori_panel["zori_is_eligible"]]
        if len(eligible) > 0:
            assert eligible["zori_excluded_reason"].isna().all()


# ============================================================================
# coclab-tsnq: Panel uniqueness and mandatory non-null tests
# ============================================================================

# Columns that must never be null in any panel row.
MANDATORY_NON_NULL_COLUMNS = [
    "coc_id",
    "year",
    "pit_total",
    "boundary_vintage_used",
    "acs5_vintage_used",
    "weighting_method",
    "boundary_changed",
    "alignment_type",
]


class TestPanelUniquenessAndNonNull:
    """Assert uniqueness and non-null constraints on the output panel."""

    def test_no_duplicate_coc_year_pairs(self, base_panel):
        """No duplicate (coc_id, year) pairs."""
        dupes = base_panel.duplicated(subset=["coc_id", "year"], keep=False)
        assert not dupes.any(), f"Found {dupes.sum()} duplicate rows"

    @pytest.mark.parametrize("col", MANDATORY_NON_NULL_COLUMNS)
    def test_base_panel_column_never_null(self, base_panel, col):
        assert base_panel[col].notna().all()

    def test_source_always_hhplab_panel(self, base_panel):
        assert (base_panel["source"] == "hhplab_panel").all()

    def test_zori_panel_no_duplicate_coc_year(self, zori_panel):
        """ZORI panel also has no duplicate (coc_id, year) pairs."""
        dupes = zori_panel.duplicated(subset=["coc_id", "year"], keep=False)
        assert not dupes.any()

    def test_zori_is_eligible_never_null(self, zori_panel):
        """zori_is_eligible is always populated (True or False)."""
        assert zori_panel["zori_is_eligible"].notna().all()


# ============================================================================
# coclab-vn1l: Parquet round-trip dtype fidelity tests
# ============================================================================


class TestParquetRoundTripFidelity:
    """Assert dtype preservation through save_panel / read_parquet."""

    def test_base_panel_dtype_roundtrip(self, base_panel, tmp_path):
        """Base panel dtypes survive a parquet round-trip."""
        output_path = save_panel(base_panel, YEARS[0], YEARS[-1], output_dir=tmp_path)
        reloaded = pd.read_parquet(output_path)

        for col in base_panel.columns:
            original_dtype = str(base_panel[col].dtype)
            reloaded_dtype = str(reloaded[col].dtype)
            assert original_dtype == reloaded_dtype, (
                f"Column '{col}' dtype changed: {original_dtype} -> {reloaded_dtype}"
            )

    def test_zori_panel_dtype_roundtrip(self, zori_panel, tmp_path):
        """ZORI panel dtypes survive a parquet round-trip.

        Columns listed in PARQUET_NORMALIZE_OK are excluded — see that
        constant's docstring for why each column is listed.
        """
        output_path = save_panel(zori_panel, YEARS[0], YEARS[-1], output_dir=tmp_path)
        reloaded = pd.read_parquet(output_path)

        for col in zori_panel.columns:
            if col in PARQUET_NORMALIZE_OK:
                continue
            original_dtype = str(zori_panel[col].dtype)
            reloaded_dtype = str(reloaded[col].dtype)
            assert original_dtype == reloaded_dtype, (
                f"Column '{col}' dtype changed: {original_dtype} -> {reloaded_dtype}"
            )

    def test_rent_to_income_normalizes_to_float_on_roundtrip(self, zori_panel, tmp_path):
        """rent_to_income is object in memory but float64 after parquet save/load.

        This documents a known dtype instability: compute_rent_to_income()
        initializes with None (creating object dtype), then sets float values
        for eligible rows. Parquet correctly coerces the whole column to float64.
        """
        output_path = save_panel(zori_panel, YEARS[0], YEARS[-1], output_dir=tmp_path)
        reloaded = pd.read_parquet(output_path)

        assert str(zori_panel["rent_to_income"].dtype) == "object"
        assert reloaded["rent_to_income"].dtype == np.float64

    def test_base_panel_values_roundtrip(self, base_panel, tmp_path):
        """Base panel values are identical after round-trip."""
        output_path = save_panel(base_panel, YEARS[0], YEARS[-1], output_dir=tmp_path)
        reloaded = pd.read_parquet(output_path)

        assert base_panel.shape == reloaded.shape
        for col in base_panel.columns:
            original = base_panel[col].reset_index(drop=True)
            loaded = reloaded[col].reset_index(drop=True)
            assert original.isna().equals(loaded.isna()), (
                f"Column '{col}' null pattern changed after round-trip"
            )

    @pytest.mark.parametrize("col", ["pit_sheltered", "pit_unsheltered"])
    def test_nullable_int_survives_roundtrip(self, base_panel, tmp_path, col):
        """Int64 nullable columns don't silently become float64."""
        output_path = save_panel(base_panel, YEARS[0], YEARS[-1], output_dir=tmp_path)
        reloaded = pd.read_parquet(output_path)

        if col in reloaded.columns:
            assert str(reloaded[col].dtype) == "Int64", (
                f"'{col}' lost Int64 dtype: {reloaded[col].dtype}"
            )


# ============================================================================
# coclab-3qyl: Panel statistical fingerprint tests
#
# All expected values are computed from the FIXTURE_* constants.
# If you change a fixture value, these tests update automatically.
# ============================================================================


def _expected_pit_total_sum() -> int:
    """Compute expected pit_total sum from FIXTURE_PIT."""
    return sum(
        pit_total
        for coc_totals in FIXTURE_PIT.values()
        for pit_total in coc_totals.values()
    )


def _expected_mean_coverage() -> float:
    """Compute expected mean coverage_ratio from FIXTURE_ACS_COVERAGE.

    Each CoC gets the same coverage in every year, so the mean is just
    the mean of the per-CoC values.
    """
    values = list(FIXTURE_ACS_COVERAGE.values())
    return sum(values) / len(values)


def _expected_eligible_count() -> int:
    """Number of ZORI-eligible (coc_id, year) observations."""
    return len(ZORI_ELIGIBLE_COCS) * len(YEARS)


def _expected_ineligible_count() -> int:
    """Number of ZORI-ineligible (coc_id, year) observations."""
    return len(ZORI_INELIGIBLE_COCS) * len(YEARS)


class TestPanelStatisticalFingerprint:
    """Golden-value assertions derived from FIXTURE_* constants."""

    def test_base_panel_shape(self, base_panel):
        assert len(base_panel) == len(COC_IDS) * len(YEARS)
        assert len(base_panel.columns) == len(PANEL_COLUMNS)

    def test_base_panel_coc_count(self, base_panel):
        assert base_panel["coc_id"].nunique() == len(COC_IDS)

    def test_base_panel_year_range(self, base_panel):
        assert base_panel["year"].min() == YEARS[0]
        assert base_panel["year"].max() == YEARS[-1]
        assert base_panel["year"].nunique() == len(YEARS)

    def test_base_panel_balanced(self, base_panel):
        """Every CoC appears in every year (balanced panel)."""
        counts = base_panel.groupby("coc_id")["year"].count()
        assert (counts == len(YEARS)).all(), f"Unbalanced: {counts.to_dict()}"

    def test_base_panel_pit_total_sum(self, base_panel):
        """Total PIT count matches sum computed from FIXTURE_PIT."""
        assert base_panel["pit_total"].sum() == _expected_pit_total_sum()

    def test_base_panel_mean_coverage_ratio(self, base_panel):
        """Mean coverage matches value computed from FIXTURE_ACS_COVERAGE."""
        assert base_panel["coverage_ratio"].mean() == pytest.approx(
            _expected_mean_coverage(), rel=1e-6,
        )

    def test_zori_panel_shape(self, zori_panel):
        assert len(zori_panel) == len(COC_IDS) * len(YEARS)

    def test_zori_panel_eligible_count(self, zori_panel):
        """Eligible count matches ZORI_ELIGIBLE_COCS * len(YEARS)."""
        assert zori_panel["zori_is_eligible"].sum() == _expected_eligible_count()
        assert (~zori_panel["zori_is_eligible"]).sum() == _expected_ineligible_count()

    def test_zori_panel_rent_to_income_count(self, zori_panel):
        """Non-null rent_to_income count equals eligible count."""
        assert zori_panel["rent_to_income"].notna().sum() == _expected_eligible_count()

    def test_zori_panel_rent_to_income_spot_check(self, zori_panel):
        """Spot-check CO-500 rent_to_income from FIXTURE constants."""
        co500 = zori_panel[
            (zori_panel["coc_id"] == "CO-500") & (zori_panel["year"] == YEARS[0])
        ].iloc[0]
        zori = FIXTURE_ZORI["CO-500"]["zori_coc"]
        income = FIXTURE_ACS_INCOME["CO-500"]
        expected = zori / (income / 12.0)
        assert co500["rent_to_income"] == pytest.approx(expected, rel=1e-6)

    def test_zori_panel_exclusion_reason_breakdown(self, zori_panel):
        """Exclusion reasons match expected distribution.

        NY-501: low_coverage (3 years)
        TX-500: missing — null zori triggers 'missing' before coverage check (3 years)
        """
        ineligible = zori_panel[~zori_panel["zori_is_eligible"]]
        reasons = ineligible["zori_excluded_reason"].value_counts().to_dict()
        assert reasons.get("low_coverage", 0) == len(YEARS)  # NY-501
