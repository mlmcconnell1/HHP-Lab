"""Regression coverage for RTS exclusion at both endpoints of panel differences."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.analysis.sample import resolve_sample
from hhplab.recipe.derive import apply_masks, derive_panel
from hhplab.recipe.loader import load_recipe

RECIPE_PATH = Path(__file__).parents[1] / "recipes/examples/shelter-capacity-results.yaml"
ENDPOINT_DERIVATION_IDS = {
    "right_to_shelter_population_count",
    "right_to_shelter",
    "lag_right_to_shelter_population_count_1y",
    "right_to_shelter_endpoint_count_1y",
    "right_to_shelter_either_endpoint_1y",
    "lag_right_to_shelter_population_count_2y",
    "right_to_shelter_endpoint_count_2y",
    "right_to_shelter_either_endpoint_2y",
}
POPULATION_FLAG_COLUMNS = (
    "rts_single_adults",
    "rts_families",
    "rts_pregnant_people",
)
RTS_FLAG_TRUTH = {
    "A": {
        2010: (0, 0, 0),
        2011: (1, 0, 0),
        2012: (0, 0, 0),
        2013: (0, 0, 0),
        2014: (0, 1, 0),
        2015: (0, 0, 0),
    },
    # B has no 2011 row, so a 2012 one-year endpoint remains unknown despite
    # zero status on both rows present in the source.
    "B": {
        2010: (0, 0, 0),
        2012: (0, 0, 0),
        2013: (0, 0, 0),
        2014: (0, 0, 0),
        2015: (0, 0, 0),
    },
    # C has a source-level unknown family status in 2012. Composite and
    # endpoint flags must retain that uncertainty.
    "C": {
        2010: (0, 0, 0),
        2011: (0, 0, 0),
        2012: (0, pd.NA, 0),
        2013: (0, 0, 0),
        2014: (0, 0, 0),
        2015: (0, 0, 0),
    },
}
ENDPOINT_EXPECTATIONS = [
    pytest.param("A", 2011, "right_to_shelter_either_endpoint_1y", 1, id="one-year-current"),
    pytest.param("A", 2012, "right_to_shelter_either_endpoint_1y", 1, id="one-year-prior"),
    pytest.param("A", 2013, "right_to_shelter_either_endpoint_1y", 0, id="one-year-neither"),
    pytest.param("A", 2013, "right_to_shelter_either_endpoint_2y", 1, id="two-year-prior"),
    pytest.param("A", 2015, "right_to_shelter_either_endpoint_2y", 0, id="two-year-neither"),
    pytest.param("B", 2012, "right_to_shelter_either_endpoint_1y", pd.NA, id="calendar-gap"),
    pytest.param("C", 2012, "right_to_shelter", pd.NA, id="source-population-unknown"),
    pytest.param(
        "C", 2013, "right_to_shelter_either_endpoint_1y", pd.NA, id="unknown-prior-endpoint"
    ),
]


def _recipe():
    return load_recipe(RECIPE_PATH)


def _derive_endpoint_state(*, with_prior_annotation: bool = False) -> dict:
    recipe = _recipe()
    frame = pd.DataFrame.from_records(
        {
            "geo_id": geo_id,
            "year": year,
            **dict(zip(POPULATION_FLAG_COLUMNS, flags, strict=True)),
            "outcome": float(year),
        }
        for geo_id, years in RTS_FLAG_TRUTH.items()
        for year, flags in years.items()
    )
    specs = [
        spec.model_dump(mode="json", by_alias=True, exclude_none=True)
        for spec in recipe.derive
        if spec.id in ENDPOINT_DERIVATION_IDS
    ]
    annotations = None
    if with_prior_annotation:
        annotations = [
            {
                "id": "rts-prior-endpoint-evidence",
                "revision": 1,
                "category": "caveat",
                "summary": "Prior RTS status evidence",
                "text": "The qualifying source status is documented for this prior endpoint.",
                "evidence": [{"name": "synthetic evidence", "ref": "docs/synthetic-rts.md"}],
                "scope": {
                    "panels": ["cbsa"],
                    "columns": ["rts_single_adults"],
                    "years": [2011],
                    "geographies": {"include": ["A"]},
                },
            }
        ]
    state = apply_masks(
        frame,
        [],
        panel_name="cbsa",
        geo_key="geo_id",
        time_key="year",
        annotations=annotations,
    )
    return derive_panel(
        state,
        specs,
        panel_name="cbsa",
        geo_key="geo_id",
        time_key="year",
        input_measures={
            column: {
                "units": "1",
                "aggregation_semantics": "extensive",
                "source_lineage": [column],
            }
            for column in (
                *POPULATION_FLAG_COLUMNS,
            )
        },
    )


def _derive_endpoint_panel(*, with_prior_annotation: bool = False) -> pd.DataFrame:
    return _derive_endpoint_state(with_prior_annotation=with_prior_annotation)["frame"]


def _resolve_variant(
    frame: pd.DataFrame,
    variant_id: str,
    *,
    years: dict,
    mask_provenance: pd.DataFrame | None = None,
    annotation_provenance: pd.DataFrame | None = None,
) -> dict:
    recipe = _recipe()
    variant = next(item for item in recipe.variants if item.id == variant_id)
    sample = {
        "years": years,
        "geographies": variant.override["sample.geographies"],
        "require": variant.override["sample.require"],
    }
    return resolve_sample(
        frame,
        {
            "analysis_id": "subst_fd",
            "variant_id": variant_id,
            "result_key": f"subst_fd@{variant_id}",
            "panel_name": "cbsa",
            "panel_identity": "synthetic-cbsa-panel",
            "geo_key": "geo_id",
            "time_key": "year",
            "required_columns": ["outcome"],
            "numeric_columns": ["outcome"],
            "weight_columns": [],
            "fixed_effects": [],
            "predicate_columns": [],
            "sample": sample,
            "sample_source": f"subst_fd@{variant_id}",
        },
        mask_provenance=mask_provenance,
        annotation_provenance=annotation_provenance,
    )


@pytest.mark.parametrize(
    ("geo_id", "year", "column", "expected"),
    ENDPOINT_EXPECTATIONS,
)
def test_recipe_derives_either_endpoint_flags_from_population_truth_table(
    geo_id: str,
    year: int,
    column: str,
    expected: int | object,
) -> None:
    frame = _derive_endpoint_panel().set_index(["geo_id", "year"])

    observed = frame.loc[(geo_id, year), column]
    if pd.isna(expected):
        assert pd.isna(observed)
    else:
        assert observed == expected


def test_fd_variants_exclude_either_endpoint_and_require_known_flags() -> None:
    recipe = _recipe()
    variants = {item.id: item for item in recipe.variants}
    annual_variant = variants["no_right_to_shelter_fd"]
    biennial_variant = variants["no_right_to_shelter_odd_years_fd"]
    fd_analyses = {"subst_fd", "sheltered_fd", "total_fd"}

    assert set(annual_variant.applies_to) == fd_analyses
    assert set(biennial_variant.applies_to) == fd_analyses
    assert biennial_variant.substitute["d_beds"] == "d2_beds"
    assert annual_variant.override["sample.require"] == [
        "right_to_shelter_either_endpoint_1y"
    ]
    assert biennial_variant.override["sample.require"] == [
        "right_to_shelter_either_endpoint_2y"
    ]

    frame = _derive_endpoint_panel()
    annual = _resolve_variant(frame, annual_variant.id, years={"from": 2011, "to": 2015})[
        "membership"
    ].set_index(["geo_id", "year"])
    assert annual.loc[("A", 2012), "reason"] == "geography_excluded"
    assert annual.loc[("A", 2013), "included"]
    assert annual.loc[("C", 2013), "reason"] == (
        "missing:right_to_shelter_either_endpoint_1y"
    )
    assert annual.loc[("B", 2012), "reason"] == (
        "missing:right_to_shelter_either_endpoint_1y"
    )

    biennial_years = biennial_variant.override["sample.years"]
    biennial = _resolve_variant(frame, biennial_variant.id, years=biennial_years)[
        "membership"
    ].set_index(["geo_id", "year"])
    assert biennial.loc[("A", 2013), "reason"] == "geography_excluded"
    assert biennial.loc[("A", 2015), "included"]


def test_prior_endpoint_annotation_reaches_sample_exclusion_audit() -> None:
    state = _derive_endpoint_state(with_prior_annotation=True)
    frame = state["frame"]
    annotation_keys = state["annotation_provenance"][
        "right_to_shelter_either_endpoint_1y"
    ].loc[frame.index[(frame["geo_id"] == "A") & (frame["year"] == 2012)][0]]
    assert annotation_keys == (("rts-prior-endpoint-evidence", 1),)

    result = _resolve_variant(
        frame,
        "no_right_to_shelter_fd",
        years={"from": 2011, "to": 2015},
        mask_provenance=state["mask_provenance"],
        annotation_provenance=state["annotation_provenance"],
    )
    dependencies = set(result["annotations"]["dependencies"])
    assert (
        "A",
        2012,
        "right_to_shelter_either_endpoint_1y",
        ("rts-prior-endpoint-evidence", 1),
    ) in dependencies
