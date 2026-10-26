"""Synthetic contracts for year-coded right-to-shelter source and mapping."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
from geopandas import GeoDataFrame
from shapely.geometry import Polygon
from typer.testing import CliRunner

from hhplab.artifacts.curated.policy import CANONICAL_PATTERNS, CURATED_SUBDIRS
from hhplab.artifacts.naming.naming import county_path, msa_county_membership_path
from hhplab.cli.main import app
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.loader import load_recipe
from hhplab.results.runner import run_results
from hhplab.schema.contracts import frame_annotation_contract
from hhplab.sources.coverage import CORE_SOURCE_COVERAGE_SPECS
from hhplab.sources.right_to_shelter import (
    COUNTY_POPULATION_COLUMNS,
    EXCEPTION_COLUMNS,
    JURISDICTION_FLAG_COLUMNS,
    JURISDICTION_SOURCE_COLUMNS,
    POPULATION_SHARE_COLUMNS,
    POPULATIONS,
    RightToShelterError,
    build_coc_county_membership,
    build_curated_outputs,
    derive_rts_flags,
    expand_policy_intervals,
    map_coc_years,
    map_jurisdiction_years,
    map_msa_years,
    mapping_output_path,
    validate_jurisdiction_source_table,
    validate_policy_baseline,
    validate_policy_exceptions,
    validate_substate_county_membership,
    write_mapping_with_provenance,
)
from hhplab.storage.provenance import read_provenance

YEAR_2020 = 2020
YEAR_2021 = 2021
SYNTHETIC_CITATION = "Synthetic test authority; not a real legal citation"
SYNTHETIC_BASELINE = {
    "schema_version": "1.0",
    "baseline_version": "synthetic-1",
    "annual_reference_date": "01-31",
    "reviewed_coverage": [{
        "jurisdiction_id": "01",
        "jurisdiction_type": "state",
        "first_year": 2020,
        "last_year": 2022,
        "evidence": [{
            "citation": SYNTHETIC_CITATION,
            "citation_source_type": "secondary",
            "citation_redistribution": "public",
        }],
    }],
    "unknown_gaps": [],
}


def source_row(
    jurisdiction_id: str,
    year: int,
    *,
    strengths: dict[str, str] | None = None,
    weather: dict[str, bool] | None = None,
) -> dict[str, object]:
    strengths = strengths or {population: "none" for population in POPULATIONS}
    weather = weather or {population: False for population in POPULATIONS}
    row: dict[str, object] = {
        "jurisdiction_id": jurisdiction_id,
        "jurisdiction_type": "sub_state" if jurisdiction_id == "NYC" else "state",
        "year": year,
        "citation": SYNTHETIC_CITATION,
        "citation_source_type": "statute",
        "citation_redistribution": "not_applicable",
        "effective_date": f"{year}-01-01",
        "ended_date": "",
    }
    for population in POPULATIONS:
        row[f"{population}_strength"] = strengths[population]
        row[f"{population}_weather_only"] = weather[population]
    return row


def status_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=JURISDICTION_SOURCE_COLUMNS)


def population_frame(rows: list[tuple[str, int, int, int, int]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=COUNTY_POPULATION_COLUMNS)


def exception_row(
    *, effective_date: str, ended_date: str = "", strength: str = "enforceable",
    qualification_scope: str = "all", qualification_summary: str = "",
) -> dict[str, object]:
    row: dict[str, object] = {
        "exception_id": f"synthetic-{effective_date}",
        "jurisdiction_id": "01",
        "jurisdiction_type": "state",
        "effective_date": effective_date,
        "ended_date": ended_date,
        "citation": SYNTHETIC_CITATION,
        "citation_source_type": "statute",
        "citation_redistribution": "not_applicable",
    }
    for population in POPULATIONS:
        row[f"{population}_strength"] = strength
        row[f"{population}_weather_only"] = False
        row[f"{population}_qualification_scope"] = qualification_scope
        row[f"{population}_qualification_summary"] = qualification_summary
    return row


@pytest.mark.parametrize(
    ("population", "expected"),
    [(population, f"rts_{population}") for population in POPULATIONS],
    ids=list(POPULATIONS),
)
def test_strength_coding_is_population_specific(population: str, expected: str) -> None:
    statuses = {name: "none" for name in POPULATIONS}
    statuses[population] = "enforceable"
    frame = status_frame([source_row("01", YEAR_2020, strengths=statuses)])

    coded = derive_rts_flags(frame, coding_rule="strict")

    assert coded.loc[0, expected] == 1
    assert coded[[column for column in coded if column.startswith("rts_")
                  and column != expected]].iloc[0].eq(0).all()


@pytest.mark.parametrize(
    ("strength", "weather_only", "strict", "broad_without_weather", "broad_with_weather"),
    [
        ("enforceable", False, 1, 1, 1),
        ("conditional", False, 0, 1, 1),
        ("weather_only", False, 0, 0, 1),
        ("none", True, 0, 0, 1),
        ("none", False, 0, 0, 0),
    ],
    ids=["enforceable", "conditional", "weather-level", "weather-flag", "none"],
)
def test_strict_and_broad_coding_truth_table(
    strength: str,
    weather_only: bool,
    strict: int,
    broad_without_weather: int,
    broad_with_weather: int,
) -> None:
    strengths = {population: "none" for population in POPULATIONS}
    weather = {population: False for population in POPULATIONS}
    strengths["families"] = strength
    weather["families"] = weather_only
    frame = status_frame([source_row("01", YEAR_2020, strengths=strengths, weather=weather)])

    assert derive_rts_flags(frame, coding_rule="strict").loc[0, "rts_families"] == strict
    broad = derive_rts_flags(frame, coding_rule="broad").loc[0, "rts_families"]
    assert broad == broad_without_weather
    assert derive_rts_flags(
        frame, coding_rule="broad", include_weather_only=True
    ).loc[0, "rts_families"] == broad_with_weather


@pytest.mark.parametrize(
    ("coding_rule", "include_weather_only"),
    [("strict", True)],
    ids=["strict-weather-only-rejected"],
)
def test_strict_coding_rejects_weather_only_option(
    coding_rule: str, include_weather_only: bool,
) -> None:
    frame = status_frame([source_row("01", YEAR_2020)])

    with pytest.raises(RightToShelterError, match="requires coding_rule='broad'"):
        derive_rts_flags(
            frame, coding_rule=coding_rule, include_weather_only=include_weather_only
        )


@pytest.mark.parametrize(
    ("bad_change", "message"),
    [
        ({"citation": ""}, "citation"),
        ({"single_adults_strength": "strong"}, "single_adults_strength"),
        ({"families_weather_only": "unknown"}, "families_weather_only"),
        ({"citation_source_type": "secondary", "citation_redistribution": "not_applicable"},
         "Secondary citations"),
        ({"effective_date": "2022-01-01", "ended_date": "2021-01-01"}, "end before"),
    ],
    ids=["citation-required", "strength-enum", "weather-boolean", "secondary-terms", "date-order"],
)
def test_source_table_validation_rejects_uncited_or_invalid_rows(
    bad_change: dict[str, object], message: str,
) -> None:
    row = source_row("01", YEAR_2020)
    row.update(bad_change)
    with pytest.raises(RightToShelterError, match=message):
        validate_jurisdiction_source_table(status_frame([row]))


@pytest.mark.parametrize(
    ("effective_date", "ended_date"),
    [("2020-07-01", ""), ("2010-01-01", "2020-01-30")],
    ids=["future-effective-on-reference-date", "expired-before-reference-date"],
)
def test_annual_rows_must_be_active_on_january_31(
    effective_date: str, ended_date: str,
) -> None:
    row = source_row("01", YEAR_2020)
    row.update(effective_date=effective_date, ended_date=ended_date)

    with pytest.raises(RightToShelterError, match="in force on January 31"):
        validate_jurisdiction_source_table(status_frame([row]))


def test_header_only_table_is_valid_schema_but_not_buildable(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=JURISDICTION_SOURCE_COLUMNS)

    validated = validate_jurisdiction_source_table(empty)

    assert validated.empty
    assert tuple(validated.columns) == JURISDICTION_SOURCE_COLUMNS


@pytest.mark.parametrize(
    ("effective_date", "ended_date", "year", "expected"),
    [
        ("2020-01-30", "", 2020, 1),
        ("2020-01-31", "", 2020, 1),
        ("2020-02-01", "", 2020, 0),
        ("2020-07-01", "", 2020, 0),
        ("2020-07-01", "", 2021, 1),
        ("2019-01-01", "2020-01-30", 2020, 0),
        ("2019-01-01", "2020-01-31", 2020, 1),
    ],
    ids=["effective-before-reference", "effective-on-reference", "effective-after-reference",
         "midyear-not-current", "midyear-next-annual", "ended-before-reference",
         "ended-on-reference"],
)
def test_policy_intervals_expand_at_january_31(
    effective_date: str, ended_date: str, year: int, expected: int,
) -> None:
    baseline = dict(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"] = [dict(SYNTHETIC_BASELINE["reviewed_coverage"][0])]
    baseline["reviewed_coverage"][0]["last_year"] = max(year, 2020)
    exceptions = pd.DataFrame([
        exception_row(effective_date=effective_date, ended_date=ended_date)
    ], columns=EXCEPTION_COLUMNS)

    annual = expand_policy_intervals(baseline, exceptions, year, year)

    assert annual.loc[0, "single_adults_strength"] == (
        "enforceable" if expected else "none"
    )


def test_reviewed_baseline_generates_none_only_inside_scoped_coverage() -> None:
    annual = expand_policy_intervals(
        SYNTHETIC_BASELINE,
        pd.DataFrame(columns=EXCEPTION_COLUMNS),
        2021,
        2021,
    )

    assert annual.loc[0, "single_adults_strength"] == "none"
    with pytest.raises(RightToShelterError, match="does not review any jurisdiction"):
        expand_policy_intervals(
            SYNTHETIC_BASELINE,
            pd.DataFrame(columns=EXCEPTION_COLUMNS),
            2019,
            2019,
        )


def test_population_specific_unknown_gap_cannot_be_overwritten_by_exception() -> None:
    baseline = dict(SYNTHETIC_BASELINE)
    baseline["unknown_gaps"] = [{
        "jurisdiction_id": "01",
        "jurisdiction_type": "state",
        "first_year": 2020,
        "last_year": 2020,
        "populations": ["single_adults"],
        "reason": "Single-adult status has not been established.",
    }]
    exceptions = pd.DataFrame([
        exception_row(effective_date="2010-01-01")
    ], columns=EXCEPTION_COLUMNS)

    annual = expand_policy_intervals(baseline, exceptions, 2020, 2020)

    assert annual.loc[0, "single_adults_qualification_scope"] == "unknown"
    with pytest.raises(RightToShelterError, match="Unresolved single_adults legal scope"):
        derive_rts_flags(annual)


def test_conflicting_dated_exceptions_are_rejected() -> None:
    exceptions = pd.DataFrame([
        exception_row(effective_date="2020-01-01", ended_date="2021-01-01"),
        exception_row(effective_date="2021-01-01", ended_date="2022-01-01"),
    ], columns=EXCEPTION_COLUMNS)

    with pytest.raises(RightToShelterError, match="intervals overlap"):
        validate_policy_exceptions(exceptions)


def test_multi_state_msa_uses_population_majority_and_emits_shares() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020, strengths={
            "single_adults": "none", "families": "enforceable", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["multi-state", "multi-state"],
        "county_fips": ["01001", "12001"],
        "allocation_share": [1.0, 1.0],
    })
    population = population_frame([
        ("01001", YEAR_2020, 80, 20, 30),
        ("12001", YEAR_2020, 20, 80, 70),
    ])

    mapped = map_msa_years(
        statuses,
        membership.rename(columns={"geo_id": "msa_id"}),
        population,
    )

    row = mapped.iloc[0]
    assert row["rts_single_adults"] == 1
    assert row["rts_families"] == 1
    assert row["rts_pregnant_people"] == 0
    assert row["rts_single_adults_population_share"] == pytest.approx(0.8)
    assert row["rts_families_population_share"] == pytest.approx(0.8)
    assert row["rts_pregnant_people_population_share"] == pytest.approx(0.0)


def test_homogeneous_geography_builds_without_weights_with_exact_share() -> None:
    statuses = status_frame([source_row("01", YEAR_2020, strengths={
        "single_adults": "enforceable", "families": "enforceable",
        "pregnant_people": "enforceable",
    })])
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})

    mapped = map_jurisdiction_years(
        statuses, membership, requested_years=(YEAR_2020, YEAR_2020)
    )

    assert mapped.loc[0, list(JURISDICTION_FLAG_COLUMNS)].tolist() == [1, 1, 1]
    assert mapped.loc[0, list(POPULATION_SHARE_COLUMNS)].tolist() == [1.0, 1.0, 1.0]


def test_mixed_status_without_weights_fails_instead_of_claiming_share() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020),
        source_row("12", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["unit", "unit"], "county_fips": ["01001", "12001"],
    })

    with pytest.raises(RightToShelterError, match="mixed single_adults status"):
        map_jurisdiction_years(statuses, membership)


def test_missing_uncovered_county_year_cannot_change_classification() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
        source_row("01", YEAR_2021, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2021),
    ])
    membership = pd.DataFrame({
        "geo_id": ["unit", "unit"], "county_fips": ["01001", "12001"],
    })
    population = population_frame([
        ("01001", YEAR_2020, 20, 20, 20), ("12001", YEAR_2020, 80, 80, 80),
        ("01001", YEAR_2021, 20, 20, 20),
    ])

    with pytest.raises(RightToShelterError, match="lack required county-year rows"):
        map_jurisdiction_years(
            statuses, membership, population, requested_years=(YEAR_2020, YEAR_2021)
        )


def test_nonfinite_county_weights_are_rejected() -> None:
    weights = population_frame([("01001", YEAR_2020, 1, 1, 1)])
    weights = weights.astype({"population_single_adults": "float64"})
    weights.loc[0, "population_single_adults"] = float("inf")

    with pytest.raises(RightToShelterError, match="finite, nonnegative"):
        map_jurisdiction_years(
            status_frame([source_row("01", YEAR_2020)]),
            pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]}),
            weights,
        )


def test_nyc_substate_override_differs_from_rest_of_new_york() -> None:
    statuses = status_frame([
        source_row("36", YEAR_2020, strengths={
            "single_adults": "conditional", "families": "none", "pregnant_people": "none",
        }),
        source_row("NYC", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["ny-metro", "ny-metro"],
        "county_fips": ["36061", "36001"],
    })
    population = population_frame([
        ("36061", YEAR_2020, 60, 50, 10),
        ("36001", YEAR_2020, 40, 50, 10),
    ])
    strict = map_jurisdiction_years(statuses, membership, population, coding_rule="strict")
    broad = map_jurisdiction_years(statuses, membership, population, coding_rule="broad")

    assert strict.loc[0, "rts_single_adults_population_share"] == pytest.approx(0.6)
    assert strict.loc[0, "rts_single_adults"] == 1
    assert broad.loc[0, "rts_single_adults_population_share"] == pytest.approx(1.0)


def test_additive_hennepin_assignment_keeps_default_nyc_and_separates_mn_county() -> None:
    hen_stage = source_row("MN-27053", YEAR_2020, strengths={
        "single_adults": "none", "families": "conditional", "pregnant_people": "none",
    })
    hen_stage["jurisdiction_type"] = "sub_state"
    statuses = status_frame([
        source_row("27", YEAR_2020), hen_stage,
        source_row("36", YEAR_2020),
        source_row("NYC", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
    ])
    membership = pd.DataFrame({
        "geo_id": ["mpls", "mpls", "new-york", "new-york"],
        "county_fips": ["27053", "27037", "36061", "36001"],
    })
    weights = population_frame([
        ("27053", YEAR_2020, 70, 70, 70), ("27037", YEAR_2020, 30, 30, 30),
        ("36061", YEAR_2020, 80, 80, 80), ("36001", YEAR_2020, 20, 20, 20),
    ])
    extra = pd.DataFrame({
        "jurisdiction_id": ["MN-27053"],
        "jurisdiction_type": ["sub_state"],
        "county_fips": ["27053"],
    })

    mapped = map_jurisdiction_years(
        statuses, membership, weights, coding_rule="broad",
        substate_county_membership=extra,
    ).set_index("geo_id")

    assert mapped.loc["mpls", "rts_families_population_share"] == pytest.approx(0.7)
    assert mapped.loc["mpls", "rts_families"] == 1
    assert mapped.loc["new-york", "rts_single_adults_population_share"] == pytest.approx(0.8)


@pytest.mark.parametrize(
    "rows",
    [
        [("MN-27053", "sub_state", "27053"), ("MN-27054", "sub_state", "27053")],
        [("MN-27053", "sub_state", "2705")],
        [("MN-27053", "state", "27053")],
        [("MN-27053", "sub_state", "36061")],
    ],
    ids=["overlapping-county", "noncanonical-fips", "wrong-jurisdiction-type", "nyc-reassignment"],
)
def test_substate_membership_rejects_overlap_and_noncanonical_rows(rows: list[tuple]) -> None:
    membership = pd.DataFrame(rows, columns=[
        "jurisdiction_id", "jurisdiction_type", "county_fips",
    ])

    with pytest.raises(RightToShelterError):
        validate_substate_county_membership(membership)


def test_scoped_substate_policy_cannot_fall_back_to_statewide_assignment() -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    substate = {
        "jurisdiction_id": "MN-27053", "jurisdiction_type": "sub_state",
        "first_year": YEAR_2020, "last_year": YEAR_2020, "county_fips": ["27053"],
        "evidence": [{"citation": SYNTHETIC_CITATION}],
    }
    baseline["reviewed_coverage"].append(substate)
    source = expand_policy_intervals(
        baseline, pd.DataFrame(columns=EXCEPTION_COLUMNS), YEAR_2020, YEAR_2020,
    )
    membership = pd.DataFrame({"geo_id": ["mpls"], "county_fips": ["27053"]})

    with pytest.raises(RightToShelterError, match="assignment is missing"):
        map_jurisdiction_years(source, membership)


def test_unrelated_substate_missing_year_does_not_block_selected_geography() -> None:
    state = source_row("01", YEAR_2021)
    substate = source_row("HENNEPIN", YEAR_2020)
    substate["jurisdiction_type"] = "sub_state"
    state["baseline_county_fips"] = None
    substate["baseline_county_fips"] = ["27053"]
    membership = pd.DataFrame({"geo_id": ["selected"], "county_fips": ["01001"]})
    overrides = pd.DataFrame({
        "jurisdiction_id": ["HENNEPIN"], "jurisdiction_type": ["sub_state"],
        "county_fips": ["27053"],
    })

    mapped = map_jurisdiction_years(
        pd.DataFrame([state, substate]), membership,
        substate_county_membership=overrides, requested_years=(YEAR_2021, YEAR_2021),
    )

    assert mapped["geo_id"].tolist() == ["selected"]
    assert mapped["rts_single_adults"].tolist() == [0]


def test_direct_mapper_rejects_substate_assignment_outside_scoped_county() -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"].append({
        "jurisdiction_id": "MN-27053", "jurisdiction_type": "sub_state",
        "first_year": YEAR_2020, "last_year": YEAR_2020, "county_fips": ["27053"],
        "evidence": [{"citation": SYNTHETIC_CITATION}],
    })
    source = expand_policy_intervals(
        baseline, pd.DataFrame(columns=EXCEPTION_COLUMNS), YEAR_2020, YEAR_2020,
    )
    wrong_assignment = pd.DataFrame({
        "jurisdiction_id": ["MN-27053"],
        "jurisdiction_type": ["sub_state"],
        "county_fips": ["27037"],
    })
    membership = pd.DataFrame({"geo_id": ["outside"], "county_fips": ["27037"]})

    with pytest.raises(RightToShelterError, match="outside the reviewed county_fips scope"):
        map_jurisdiction_years(
            source, membership, substate_county_membership=wrong_assignment,
        )


def test_coc_year_mapping_uses_projected_county_intersections() -> None:
    coc_geometries = GeoDataFrame(
        {"coc_id": ["synthetic-coc"]},
        geometry=[Polygon([(0, 0), (2.5, 0), (2.5, 1), (0, 1)])],
        crs="EPSG:5070",
    )
    county_geometries = GeoDataFrame(
        {"GEOID": ["01001", "12001"]},
        geometry=[
            Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:5070",
    )
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
    ])
    population = population_frame([
        ("01001", YEAR_2020, 100, 100, 100),
        ("12001", YEAR_2020, 100, 100, 100),
    ])

    mapped = map_coc_years(statuses, coc_geometries, county_geometries, population)

    assert mapped.loc[0, "rts_single_adults"] == 1
    assert mapped.loc[0, "rts_single_adults_population_share"] == pytest.approx(2 / 3)


@pytest.mark.parametrize(
    ("geography_rule", "expected_flag"),
    [("majority", 0), ("any", 1)],
    ids=["majority", "any-coverage"],
)
def test_geography_flag_rule_is_configurable(
    geography_rule: str, expected_flag: int,
) -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
    ])
    membership = pd.DataFrame({"geo_id": ["unit", "unit"], "county_fips": ["01001", "12001"]})
    population = population_frame([
        ("01001", YEAR_2020, 40, 1, 1),
        ("12001", YEAR_2020, 60, 1, 1),
    ])

    mapped = map_jurisdiction_years(
        statuses, membership, population, geography_rule=geography_rule,
    )

    assert mapped.loc[0, "rts_single_adults"] == expected_flag
    assert mapped.loc[0, "rts_single_adults_population_share"] == pytest.approx(0.4)


def test_mid_panel_change_is_applied_to_each_year() -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("01", YEAR_2021),
    ])
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})
    population = population_frame([
        ("01001", YEAR_2020, 10, 10, 10),
        ("01001", YEAR_2021, 10, 10, 10),
    ])

    mapped = map_jurisdiction_years(statuses, membership, population)

    assert mapped["rts_single_adults"].tolist() == [1, 0]
    assert mapped["year"].tolist() == [YEAR_2020, YEAR_2021]


def test_unknown_jurisdiction_year_fails_instead_of_becoming_zero() -> None:
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})
    population = population_frame([("01001", YEAR_2020, 1, 1, 1)])

    with pytest.raises(RightToShelterError, match="unknown status is not coded as zero"):
        map_jurisdiction_years(status_frame([]), membership, population)


def test_partial_coverage_annotation_reaches_results_package(tmp_path: Path) -> None:
    statuses = status_frame([
        source_row("01", YEAR_2020, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2020),
        source_row("01", YEAR_2021, strengths={
            "single_adults": "enforceable", "families": "none", "pregnant_people": "none",
        }),
        source_row("12", YEAR_2021),
    ])
    membership = pd.DataFrame({
        "geo_id": ["multi-state", "multi-state"],
        "county_fips": ["01001", "12001"],
    })
    population = population_frame([
        ("01001", YEAR_2020, 80, 50, 50),
        ("12001", YEAR_2020, 20, 50, 50),
        ("01001", YEAR_2021, 80, 50, 50),
        ("12001", YEAR_2021, 20, 50, 50),
    ])
    mapped = map_jurisdiction_years(statuses, membership, population)
    mapped["rts_single_adults"] = mapped["rts_single_adults"].astype(float)
    mapped.attrs["measures"] = {
        "rts_single_adults": {"units": "indicator", "aggregation_semantics": "intensive"}
    }
    artifact_path = tmp_path / "rts.parquet"
    write_mapping_with_provenance(
        mapped,
        artifact_path,
        geography="msa",
        coding_rule="strict",
        include_weather_only=False,
        geography_rule="majority",
        boundary_vintage="synthetic",
        membership_vintage="synthetic",
    )

    persisted = RecipeCache().read_parquet(artifact_path)
    contract = frame_annotation_contract(persisted)
    assert contract is not None
    assert contract["records"][0]["category"] == "geographic_approximation"
    assert contract["bindings"]["right-to-shelter-geographic-approximation#r2"][
        "columns"]["rts_single_adults"] == [["multi-state", YEAR_2020], ["multi-state", YEAR_2021]]
    assert read_provenance(artifact_path).extra["coding_rule"] == "strict"

    recipe = load_recipe({
        "version": 2,
        "id": "rts-annotation-test",
        "title": "Synthetic RTS annotation propagation",
        "authors": ["test"],
        "universe": {"range": f"{YEAR_2020}-{YEAR_2021}"},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
        "datasets": {},
        "pipelines": [{"id": "build", "target": "target", "steps": []}],
        "panels": {"panel": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo_id", "time_key": "year",
        }},
        "analyses": [{
            "id": "summary", "method": "descriptive", "input": "panels.panel",
            "sample": {}, "columns": ["rts_single_adults"], "stats": ["n", "mean"],
        }],
        "results": {
            "bundle_dir": "results/{id}/{run_id}", "include_panels": ["panel"],
            "report": {"title": "RTS annotation propagation"},
        },
    })
    run = run_results(
        recipe,
        original_recipe_bytes=json.dumps(recipe.model_dump(mode="json", by_alias=True)).encode(),
        outputs_root=tmp_path / "results",
        panel_frames={"panel": persisted},
        private=True,
    )

    bundle = Path(run["bundle_path"])
    annotations = json.loads((bundle / "annotations.json").read_bytes())
    assert annotations["applied"] == [{
        "id": "right-to-shelter-geographic-approximation", "revision": 2,
    }]
    assert "partially covered" in (bundle / "report" / "index.html").read_text()


def test_subgroup_qualification_is_exactly_annotated_without_eligibility_share_claim() -> None:
    statuses = status_frame([source_row("01", YEAR_2020, strengths={
        "single_adults": "enforceable", "families": "enforceable",
        "pregnant_people": "enforceable",
    })])
    for population in POPULATIONS:
        statuses[f"{population}_qualification_scope"] = "all"
        statuses[f"{population}_qualification_summary"] = ""
    statuses.loc[0, "single_adults_qualification_scope"] = "subgroup"
    statuses.loc[0, "single_adults_qualification_summary"] = "Limited to documented subgroup."
    membership = pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]})
    weights = population_frame([("01001", YEAR_2020, 100, 100, 100)])

    mapped = map_jurisdiction_years(statuses, membership, weights)
    contract = mapped.attrs["annotations"]
    record = next(
        record for record in contract["records"]
        if record["id"].startswith("right-to-shelter-subgroup-qualification-")
    )

    assert contract["bindings"][f"{record['id']}#r1"]["columns"][
        "rts_single_adults"
    ] == [["one", YEAR_2020]]
    assert "does not estimate the share of people eligible" in record["text"]


@pytest.mark.parametrize("unknown_state", [False, True], ids=["reviewed", "unrelated-unknown"])
def test_build_uses_scoped_inputs_records_lineage_and_preserves_existing_outputs(
    tmp_path: Path, unknown_state: bool,
) -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    if unknown_state:
        other_scope = {**baseline["reviewed_coverage"][0], "jurisdiction_id": "12"}
        baseline["reviewed_coverage"].append(other_scope)
        baseline["unknown_gaps"].append({
            **{key: other_scope[key] for key in (
                "jurisdiction_id", "jurisdiction_type", "first_year", "last_year"
            )},
            "reason": "Unrelated jurisdiction has not been established.",
        })
    source_dir = tmp_path / "curated" / "right_to_shelter"
    source_dir.mkdir(parents=True)
    (source_dir / "right_to_shelter__baseline.json").write_text(json.dumps(baseline))
    pd.DataFrame(columns=EXCEPTION_COLUMNS).to_csv(
        source_dir / "right_to_shelter__exceptions.csv", index=False,
    )
    membership_path = msa_county_membership_path("synthetic", tmp_path)
    membership_path.parent.mkdir(parents=True)
    pd.DataFrame({"msa_id": ["one"], "county_fips": ["01001"]}).to_parquet(membership_path)
    county_file = county_path(2020, tmp_path)
    county_file.parent.mkdir(parents=True)
    square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    GeoDataFrame({"geoid": ["01001"]}, geometry=[square], crs="EPSG:5070").to_crs(
        "EPSG:4326",
    ).to_parquet(county_file)
    coc_file = tmp_path / "curated" / "coc_boundaries" / "coc__B2020.parquet"
    coc_file.parent.mkdir(parents=True)
    GeoDataFrame({"coc_id": ["synthetic-coc"]}, geometry=[square], crs="EPSG:5070").to_crs(
        "EPSG:4326",
    ).to_parquet(coc_file)
    build_args = {
        "data_root": tmp_path, "first_year": YEAR_2020, "last_year": YEAR_2021,
        "msa_definition_version": "synthetic", "coc_boundary_vintage": "2020",
        "county_vintage": "2020", "majority_threshold": 0.6,
        "msa_ids": ["one"], "coc_ids": ["synthetic-coc"],
    }

    outputs = build_curated_outputs(**build_args)

    for path in outputs.values():
        assert "__S" in path.name
        assert any(
            pattern.fullmatch(path.name) for pattern in CANONICAL_PATTERNS["right_to_shelter"]
        )
        mapped = pd.read_parquet(path)
        assert mapped["year"].tolist() == [YEAR_2020, YEAR_2021]
        assert mapped[list(JURISDICTION_FLAG_COLUMNS)].eq(0).all().all()
        provenance = read_provenance(path).extra
        assert provenance["majority_threshold"] == 0.6
        assert provenance["msa_ids"] == ["one"]
        assert provenance["geography_inputs"]["county_geometry"]["sha256"] == (
            hashlib.sha256(county_file.read_bytes()).hexdigest()
        )
    retained = {name: path.read_bytes() for name, path in outputs.items()}
    with pytest.raises(RightToShelterError, match="already exist"):
        build_curated_outputs(**build_args)
    assert {name: path.read_bytes() for name, path in outputs.items()} == retained


def test_weights_for_mixed_geography_do_not_require_unrelated_homogeneous_rows() -> None:
    strengths = dict.fromkeys(POPULATIONS, "enforceable")
    source = status_frame([
        source_row("01", YEAR_2020, strengths=strengths),
        source_row("12", YEAR_2020), source_row("25", YEAR_2020),
    ])
    membership = pd.DataFrame({
        "geo_id": ["mixed", "mixed", "homogeneous"],
        "county_fips": ["01001", "12001", "25001"],
    })
    weights = pd.DataFrame({
        "county_fips": ["01001", "12001"], "year": [YEAR_2020, YEAR_2020],
        "resident_population": [20, 80],
    })

    mapped = map_jurisdiction_years(source, membership, weights).set_index("geo_id")

    assert mapped.loc["mixed", "rts_single_adults_population_share"] == pytest.approx(0.2)
    assert mapped.loc["homogeneous", list(POPULATION_SHARE_COLUMNS)].eq(0).all()
    contract = mapped.attrs["annotations"]
    assert contract["bindings"]["right-to-shelter-resident-population-weights#r1"][
        "columns"
    ]["rts_single_adults_population_share"] == [["mixed", YEAR_2020]]


@pytest.mark.parametrize(
    "invalid_change", [{"jurisdiction_id": "DC"}, {"first_year": 2020.5}],
    ids=["state-fips-required", "fractional-year-rejected"],
)
def test_baseline_rejects_unmappable_state_or_fractional_year(invalid_change: dict) -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"][0].update(invalid_change)

    with pytest.raises(RightToShelterError):
        validate_policy_baseline(baseline)


@pytest.mark.parametrize(
    "annotation_change",
    [
        {"id": None},
        {"public_text": None},
        {"revision": True},
        {"revision": "1"},
        {"unexpected": "ignored fields are forbidden"},
        {"scope": {"unknown": "invalid scope field"}},
    ],
    ids=["null-id", "public-text-required", "boolean-revision", "string-revision",
         "extra-field", "invalid-scope"],
)
def test_baseline_annotation_uses_strict_data_annotation_contract(
    annotation_change: dict,
) -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["provisional_baseline"] = {
        "id": "right-to-shelter-provisional-baseline", "revision": 1,
        "category": "caveat", "summary": "Provisional none baseline",
        "text": "Unlisted status is provisionally coded as none.",
        "public_text": "Unlisted status is provisionally coded as none.",
    }
    baseline["provisional_baseline"].update(annotation_change)

    with pytest.raises(RightToShelterError, match="provisional_baseline"):
        validate_policy_baseline(baseline)


def test_open_ended_exception_cannot_hide_later_conflicting_interval() -> None:
    exceptions = pd.DataFrame([
        exception_row(effective_date="2020-01-01"),
        exception_row(effective_date="2021-01-01"),
    ], columns=EXCEPTION_COLUMNS)

    with pytest.raises(RightToShelterError, match="open-ended exception overlaps"):
        validate_policy_exceptions(exceptions)


@pytest.mark.parametrize(
    ("effective_date", "first_year", "last_year", "expected"),
    [
        ("2024-03-15", 2024, 2025, [0, 1]),
        ("2023-11-01", 2023, 2024, [0, 1]),
    ],
    ids=["nyc-march-2024-order-timing", "ma-november-2023-declaration-timing"],
)
def test_verified_policy_change_dates_affect_next_pit_reference(
    effective_date: str, first_year: int, last_year: int, expected: list[int],
) -> None:
    # These dates are sourced in right-to-shelter-evidence.md. Strengths and
    # jurisdiction are synthetic: only annual timing is asserted here.
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"][0].update(first_year=first_year, last_year=last_year)
    exceptions = pd.DataFrame([exception_row(effective_date=effective_date)])

    annual = expand_policy_intervals(baseline, exceptions, first_year, last_year)

    assert derive_rts_flags(annual)["rts_single_adults"].tolist() == expected


def test_subgroup_notes_bind_only_their_own_population_and_geography() -> None:
    rows = [source_row("01", YEAR_2020), source_row("12", YEAR_2020)]
    for row, population in zip(rows, ["single_adults", "families"], strict=True):
        for name in POPULATIONS:
            row[f"{name}_qualification_scope"] = "subgroup" if name == population else "all"
            row[f"{name}_qualification_summary"] = (
                f"Limited {population}." if name == population else ""
            )
    source = pd.DataFrame(rows)
    membership = pd.DataFrame({"geo_id": ["a", "b"], "county_fips": ["01001", "12001"]})

    mapped = map_jurisdiction_years(source, membership)
    contract = mapped.attrs["annotations"]

    assert len(contract["records"]) == 2
    for record in contract["records"]:
        columns = contract["bindings"][f"{record['id']}#r1"]["columns"]
        if "Limited single_adults." in record["text"]:
            assert columns == {
                "rts_single_adults": [["a", YEAR_2020]],
                "rts_single_adults_population_share": [["a", YEAR_2020]],
            }
            assert "Limited families." not in record["text"]
        else:
            assert columns == {
                "rts_families": [["b", YEAR_2020]],
                "rts_families_population_share": [["b", YEAR_2020]],
            }


@pytest.mark.parametrize(
    ("overlap_width", "expected_counties"),
    [(1e-10, ["01001"]), (1e-6, ["01001", "12001"])],
    ids=["microscopic-reprojection-sliver", "retain-real-fragment"],
)
def test_overlay_filters_only_microscopic_county_slivers(
    overlap_width: float, expected_counties: list[str],
) -> None:
    coc = GeoDataFrame(
        {"coc_id": ["one"]},
        geometry=[Polygon([(0, 0), (1 + overlap_width, 0), (1 + overlap_width, 1), (0, 1)])],
        crs="EPSG:5070",
    )
    counties = GeoDataFrame(
        {"geoid": ["01001", "12001"]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ], crs="EPSG:5070",
    )

    membership = build_coc_county_membership(coc, counties)

    assert sorted(membership["county_fips"].tolist()) == expected_counties


def test_geographic_caveat_record_is_immutable_across_output_geographies() -> None:
    source = status_frame([
        source_row("01", YEAR_2020, strengths=dict.fromkeys(POPULATIONS, "enforceable")),
        source_row("12", YEAR_2020),
    ])
    weights = population_frame([
        ("01001", YEAR_2020, 20, 20, 20), ("12001", YEAR_2020, 80, 80, 80),
    ])
    membership = pd.DataFrame({"geo_id": ["a", "a"], "county_fips": ["01001", "12001"]})

    first = map_jurisdiction_years(source, membership, weights).attrs["annotations"]
    second = map_jurisdiction_years(
        source, membership.assign(geo_id="b"), weights,
    ).attrs["annotations"]

    assert first["records"] == second["records"]
    assert first["bindings"] != second["bindings"]


@pytest.mark.parametrize(
    "change", [{"jurisdiction_id": "12"}, {"effective_date": "2030-01-01"}],
    ids=["unreviewed-jurisdiction", "outside-reviewed-calendar-interval"],
)
def test_orphan_exceptions_cannot_silently_become_baseline_none(change: dict) -> None:
    exception = exception_row(effective_date="2020-01-01")
    exception.update(change)

    with pytest.raises(RightToShelterError, match="no matching reviewed baseline"):
        expand_policy_intervals(
            SYNTHETIC_BASELINE, pd.DataFrame([exception]), YEAR_2020, YEAR_2020,
        )


def test_provisional_baseline_annotation_binds_generated_flags_and_shares() -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"][0]["basis"] = "provisional_none"
    baseline["provisional_baseline"] = {
        "id": "right-to-shelter-provisional-baseline", "revision": 1,
        "category": "caveat", "summary": "Provisional none baseline",
        "text": "Unlisted status is provisionally coded as none.",
        "public_text": "Unlisted status is provisionally coded as none.",
    }
    source = expand_policy_intervals(
        baseline, pd.DataFrame(columns=EXCEPTION_COLUMNS), YEAR_2020, YEAR_2020,
    )
    mapped = map_jurisdiction_years(
        source, pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]}),
    )

    contract = mapped.attrs["annotations"]
    binding = contract["bindings"]["right-to-shelter-provisional-baseline#r1"]["columns"]
    assert binding["rts_single_adults"] == [["one", YEAR_2020]]
    assert binding["rts_single_adults_population_share"] == [["one", YEAR_2020]]


def test_active_policy_exception_is_not_attributed_to_provisional_absence() -> None:
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"][0]["basis"] = "provisional_none"
    baseline["provisional_baseline"] = {
        "id": "right-to-shelter-provisional-baseline", "revision": 1,
        "category": "caveat", "summary": "Provisional none baseline",
        "text": "Unlisted status is provisionally coded as none.",
        "public_text": "Unlisted status is provisionally coded as none.",
    }
    exception = exception_row(effective_date="2020-01-01", strength="enforceable")
    source = expand_policy_intervals(
        baseline, pd.DataFrame([exception]), YEAR_2020, YEAR_2020,
    )
    mapped = map_jurisdiction_years(
        source, pd.DataFrame({"geo_id": ["one"], "county_fips": ["01001"]}),
    )

    assert "right-to-shelter-provisional-baseline#r1" not in mapped.attrs[
        "annotations"
    ]["bindings"]


def test_subset_outputs_have_distinct_order_independent_names() -> None:
    complete = mapping_output_path("coc", YEAR_2020, YEAR_2021, 2025)
    subset = mapping_output_path(
        "coc", YEAR_2020, YEAR_2021, 2025, geography_ids=["NY-600", "NY-601"],
    )
    reversed_subset = mapping_output_path(
        "coc", YEAR_2020, YEAR_2021, 2025, geography_ids=["NY-601", "NY-600", "NY-600"],
    )

    assert subset != complete
    assert subset == reversed_subset


def test_curated_layout_supports_canonical_table_and_output_names() -> None:
    assert "right_to_shelter" in CURATED_SUBDIRS
    assert any(
        pattern.fullmatch("right_to_shelter__jurisdiction_year.csv")
        for pattern in CANONICAL_PATTERNS["right_to_shelter"]
    )
    assert any(
        pattern.fullmatch("right_to_shelter__baseline.json")
        for pattern in CANONICAL_PATTERNS["right_to_shelter"]
    )
    assert any(
        pattern.fullmatch("right_to_shelter__exceptions.csv")
        for pattern in CANONICAL_PATTERNS["right_to_shelter"]
    )
    assert mapping_output_path("msa", YEAR_2020, YEAR_2021, "census_2025").name == (
        "right_to_shelter__msa__Y2020-2021@Mcensus_2025.parquet"
    )


def test_source_coverage_is_registered_with_dynamic_availability_note() -> None:
    spec = CORE_SOURCE_COVERAGE_SPECS["right_to_shelter"]

    assert spec.native_geo == "jurisdiction"
    assert "depends on cited reviewed coverage" in spec.notes
    assert spec.first_year == 2007


def test_cli_json_error_tells_operator_how_to_add_source_rows(tmp_path: Path) -> None:
    population_path = tmp_path / "population.csv"
    pd.DataFrame(columns=COUNTY_POPULATION_COLUMNS).to_csv(population_path, index=False)
    missing_source = tmp_path / "right-to-shelter.csv"
    runner = CliRunner()

    result = runner.invoke(app, [
        "build", "right-to-shelter",
        "--first-year", str(YEAR_2020),
        "--last-year", str(YEAR_2020),
        "--county-population", str(population_path),
        "--weight-definition", "synthetic resident population 2020",
        "--coc-boundary-vintage", "synthetic",
        "--county-vintage", "synthetic",
        "--exceptions", str(missing_source),
        "--json",
    ])

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert "Add cited intervals" in payload["error"]


def test_cli_accepts_explicit_substate_membership_option_and_validates_csv(tmp_path: Path) -> None:
    source_dir = tmp_path / "curated" / "right_to_shelter"
    source_dir.mkdir(parents=True)
    (source_dir / "right_to_shelter__baseline.json").write_text(
        json.dumps(SYNTHETIC_BASELINE), encoding="utf-8",
    )
    pd.DataFrame(columns=EXCEPTION_COLUMNS).to_csv(
        source_dir / "right_to_shelter__exceptions.csv", index=False,
    )
    membership_path = tmp_path / "substate.csv"
    pd.DataFrame({
        "jurisdiction_id": ["MN-27053"],
        "jurisdiction_type": ["sub_state"],
        "county_fips": ["2705"],
    }).to_csv(membership_path, index=False)
    runner = CliRunner()

    result = runner.invoke(app, [
        "build", "right-to-shelter",
        "--first-year", str(YEAR_2020), "--last-year", str(YEAR_2020),
        "--coc-boundary-vintage", "synthetic", "--county-vintage", "synthetic",
        "--msa-definition-version", "synthetic", "--data-root", str(tmp_path),
        "--substate-county-membership", str(membership_path), "--json",
    ])

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert "canonical five-digit FIPS" in payload["error"]


def test_curated_build_uses_hennepin_csv_and_preserves_nyc_membership(tmp_path: Path) -> None:
    source_dir = tmp_path / "curated" / "right_to_shelter"
    source_dir.mkdir(parents=True)
    baseline = copy.deepcopy(SYNTHETIC_BASELINE)
    baseline["reviewed_coverage"] = [
        {
            "jurisdiction_id": "27", "jurisdiction_type": "state",
            "first_year": YEAR_2020, "last_year": YEAR_2020,
            "evidence": [{"citation": SYNTHETIC_CITATION}],
        },
        {
            "jurisdiction_id": "36", "jurisdiction_type": "state",
            "first_year": YEAR_2020, "last_year": YEAR_2020,
            "evidence": [{"citation": SYNTHETIC_CITATION}],
        },
        {
            "jurisdiction_id": "MN-27053", "jurisdiction_type": "sub_state",
            "county_fips": ["27053"], "first_year": YEAR_2020, "last_year": YEAR_2020,
            "evidence": [{"citation": SYNTHETIC_CITATION}],
        },
        {
            "jurisdiction_id": "NYC", "jurisdiction_type": "sub_state",
            "county_fips": ["36005", "36047", "36061", "36081", "36085"],
            "first_year": YEAR_2020, "last_year": YEAR_2020,
            "evidence": [{"citation": SYNTHETIC_CITATION}],
        },
    ]
    (source_dir / "right_to_shelter__baseline.json").write_text(
        json.dumps(baseline), encoding="utf-8",
    )
    hen_exception = exception_row(effective_date="2020-01-01", strength="none")
    hen_exception.update(
        exception_id="hennepin-2020", jurisdiction_id="MN-27053",
        jurisdiction_type="sub_state", families_strength="enforceable",
    )
    nyc_exception = exception_row(effective_date="2020-01-01", strength="none")
    nyc_exception.update(
        exception_id="nyc-2020", jurisdiction_id="NYC", jurisdiction_type="sub_state",
        single_adults_strength="enforceable",
    )
    pd.DataFrame([hen_exception, nyc_exception], columns=EXCEPTION_COLUMNS).to_csv(
        source_dir / "right_to_shelter__exceptions.csv", index=False,
    )
    membership_path = tmp_path / "hennepin-membership.csv"
    pd.DataFrame({
        "jurisdiction_id": ["MN-27053"],
        "jurisdiction_type": ["sub_state"],
        "county_fips": ["27053"],
    }).to_csv(membership_path, index=False)
    msa_membership = pd.DataFrame({
        "msa_id": ["minneapolis", "minneapolis", "new-york", "new-york"],
        "county_fips": ["27053", "27037", "36061", "36001"],
    })
    msa_path = msa_county_membership_path("synthetic", tmp_path)
    msa_path.parent.mkdir(parents=True)
    msa_membership.to_parquet(msa_path)
    county_file = county_path(2020, tmp_path)
    county_file.parent.mkdir(parents=True)
    county_squares = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
    ]
    GeoDataFrame({"geoid": ["27053", "27037"]}, geometry=county_squares,
                 crs="EPSG:5070").to_crs("EPSG:4326").to_parquet(county_file)
    coc_path = tmp_path / "curated" / "coc_boundaries" / "coc__B2020.parquet"
    coc_path.parent.mkdir(parents=True)
    GeoDataFrame(
        {"coc_id": ["minneapolis-coc"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])],
        crs="EPSG:5070",
    ).to_crs("EPSG:4326").to_parquet(coc_path)
    weights_path = tmp_path / "county-population.csv"
    population_frame([
        ("27053", YEAR_2020, 70, 70, 70), ("27037", YEAR_2020, 30, 30, 30),
        ("36061", YEAR_2020, 80, 80, 80), ("36001", YEAR_2020, 20, 20, 20),
    ]).to_csv(weights_path, index=False)

    outputs = build_curated_outputs(
        data_root=tmp_path, first_year=YEAR_2020, last_year=YEAR_2020,
        msa_definition_version="synthetic", coc_boundary_vintage="2020",
        county_vintage="2020", county_population_path=weights_path,
        weight_definition="synthetic county population 2020", coding_rule="strict",
        msa_ids=["minneapolis", "new-york"],
        substate_county_membership_path=membership_path,
    )

    msa = pd.read_parquet(outputs["msa"]).set_index("geo_id")
    assert msa.loc["minneapolis", "rts_families_population_share"] == pytest.approx(0.7)
    assert msa.loc["new-york", "rts_single_adults_population_share"] == pytest.approx(0.8)
    provenance = read_provenance(outputs["msa"]).extra
    assert provenance["substate_county_membership_mode"] == "additive_with_default_nyc"
    assert provenance["substate_county_membership_input_sha256"] == hashlib.sha256(
        membership_path.read_bytes()
    ).hexdigest()
    effective_membership = provenance["substate_county_membership"]
    nyc_counties = {
        row["county_fips"] for row in effective_membership
        if row["jurisdiction_id"] == "NYC"
    }
    assert nyc_counties == {"36005", "36047", "36061", "36081", "36085"}
    coc = pd.read_parquet(outputs["coc"])
    assert coc.loc[0, "rts_families_population_share"] == pytest.approx(0.7)
    assert read_provenance(outputs["coc"]).extra[
        "substate_county_membership_input_sha256"
    ] == provenance["substate_county_membership_input_sha256"]


@pytest.mark.parametrize(
    ("coding_rule", "weather_option"),
    [("strict", "--include-weather-only")],
    ids=["strict-weather-only-rejected"],
)
def test_cli_json_error_rejects_strict_weather_only(
    tmp_path: Path, coding_rule: str, weather_option: str,
) -> None:
    population_path = tmp_path / "population.csv"
    pd.DataFrame(columns=COUNTY_POPULATION_COLUMNS).to_csv(population_path, index=False)
    runner = CliRunner()

    result = runner.invoke(app, [
        "build", "right-to-shelter",
        "--first-year", str(YEAR_2020),
        "--last-year", str(YEAR_2020),
        "--county-population", str(population_path),
        "--weight-definition", "synthetic resident population 2020",
        "--coc-boundary-vintage", "synthetic",
        "--county-vintage", "synthetic",
        "--coding-rule", coding_rule,
        weather_option,
        "--json",
    ])

    payload = json.loads(result.output)
    assert result.exit_code == 1
    assert payload["status"] == "error"
    assert "requires --coding-rule broad" in payload["error"]
