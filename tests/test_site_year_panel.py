"""Truth table for whole-CoC site-year outcome aggregation.

CO-1 and CO-2 form one frozen MSA site. PIT maps CO-2 to an approved 2020
source ID, but its 2021 boundary status is unknown. HIC remains compatible in
both years. PIT total and unsheltered are source-limited in 2021; sheltered
and HIC are evaluated normally.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from hhplab.geographies.coc.coc_validate import validate_coc_boundary_compatibility
from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.msa_coc_panel import (
    SiteYearSourceBinding,
    _coc_measures_for_year,
    _FrameRecord,
    assemble_site_year_panel,
    site_year_panel_columns,
)
from hhplab.schema.columns import HIC_PANEL_MEASURE_COLUMNS
from hhplab.sources.hud.pit.observation import (
    COUNT_TYPES_SHELTERED_ONLY,
    with_pit_count_metadata,
)

ARTIFACT_ID = "overlap_samples"
THRESHOLD = 0.95
MEMBERSHIP_IDENTITY = "sha256:site-fixture-membership"
FROZEN_VINTAGE = "2024"
MEMBERSHIPS = (("MSA-A", "CO-1"), ("MSA-A", "CO-2"))
YEARS = (2020, 2021)
PIT_MEASURE = "pit_total"
PIT_SHELTERED = "pit_sheltered"
PIT_UNSHELTERED = "pit_unsheltered"
HIC_MEASURE = HIC_PANEL_MEASURE_COLUMNS[0]
OUTCOME_BINDINGS = {
    "pit": SiteYearSourceBinding(
        panel="pit_coc",
        measures=(PIT_MEASURE, PIT_SHELTERED, PIT_UNSHELTERED),
    ),
    "hic": SiteYearSourceBinding(panel="hic_coc", measures=(HIC_MEASURE,)),
}
SOURCE_PANEL_VINTAGES = {
    (panel, year): str(year)
    for panel in ("pit_coc", "hic_coc")
    for year in YEARS
}
SOURCE_PANEL_FINGERPRINTS = {
    "pit_coc": "sha256:pit-fixture",
    "hic_coc": "sha256:hic-fixture",
}


def _compatibility_payload():
    records = []
    for panel_id in ("pit_coc", "hic_coc"):
        for year in YEARS:
            for coc_id in ("CO-1", "CO-2"):
                status = "compatible"
                record = {
                    "source_panel": panel_id,
                    "frozen_coc_id": coc_id,
                    "year": year,
                    "frozen_boundary_vintage": FROZEN_VINTAGE,
                    "source_boundary_vintage": str(year),
                    "status": status,
                    "reason": "Documented boundary comparison.",
                    "evidence_ref": f"audit://{panel_id}/{coc_id}/{year}",
                }
                if panel_id == "pit_coc" and coc_id == "CO-2" and year == 2020:
                    record["status"] = "mapped"
                    record["source_coc_id"] = "CO-2-OLD"
                    record["approval_ref"] = "approval://pit/CO-2/2020"
                elif panel_id == "pit_coc" and coc_id == "CO-2" and year == 2021:
                    record["status"] = "unknown"
                    record["reason"] = "No historical boundary evidence is available."
                    record.pop("evidence_ref")
                records.append(record)
    return {
        "schema_version": "coc_boundary_compatibility/1",
        "evidence_id": "boundary-audit-fixture",
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_VINTAGE,
        "records": records,
    }


PROVIDENCE_MEMBERSHIPS = (("MSA-Providence", "MA-505"),)
PROVIDENCE_PREDECESSORS = ("MA-505", "MA-519")


def _providence_v2_evidence():
    records = []
    for panel_id in ("pit_coc", "hic_coc"):
        record = {
            "source_panel": panel_id,
            "frozen_coc_id": "MA-505",
            "year": 2020,
            "frozen_boundary_vintage": FROZEN_VINTAGE,
            "source_boundary_vintage": "2020",
            "status": "reconciled",
            "reason": "Reviewed administrative predecessor-union count reconciliation.",
            "evidence_ref": "audit://providence/ma505-ma519-reconciliation",
            "approval_ref": (
                "docs/providence-historical-reconciliation.md#integration-review-"
                "administrative-count-reconciliation"
            ),
            "reconciliation_basis": (
                "The same named municipal membership is supported by the merger record "
                "and jurisdiction cross-check."
            ),
            "geometry_status": "different_unresolved",
            "geometry_evidence_ref": "audit://providence/geometry/2020",
            "geometry_residual": "13.499083 km2 symmetric difference; cause unresolved.",
            "source_coc_ids": ["MA-505"],
            "count_operation": "preaggregated",
            "preaggregated_source_coc_ids": list(PROVIDENCE_PREDECESSORS),
            "count_evidence_ref": "audit://providence/pit-p2024-hic-2020-counts",
        }
        if panel_id == "hic_coc":
            record.update(
                {
                    "source_coc_ids": list(PROVIDENCE_PREDECESSORS),
                    "count_operation": "sum",
                    "preaggregated_source_coc_ids": [],
                }
            )
        records.append(record)
    payload = {
        "schema_version": "coc_boundary_compatibility/2",
        "evidence_id": "providence-ma505-ma519-2020-review-candidate",
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_VINTAGE,
        "records": records,
    }
    return validate_coc_boundary_compatibility(
        payload,
        artifact_id=ARTIFACT_ID,
        threshold=THRESHOLD,
        membership_identity=MEMBERSHIP_IDENTITY,
        frozen_boundary_vintage=FROZEN_VINTAGE,
        frozen_memberships=PROVIDENCE_MEMBERSHIPS,
        requested_years=(2020,),
        source_panel_vintages={("pit_coc", 2020): "2020", ("hic_coc", 2020): "2020"},
    )


def _compatibility(memberships=MEMBERSHIPS):
    return validate_coc_boundary_compatibility(
        _compatibility_payload(),
        artifact_id=ARTIFACT_ID,
        threshold=THRESHOLD,
        membership_identity=MEMBERSHIP_IDENTITY,
        frozen_boundary_vintage=FROZEN_VINTAGE,
        frozen_memberships=memberships,
        requested_years=YEARS,
        source_panel_vintages=SOURCE_PANEL_VINTAGES,
    )


def _source_panels():
    pit = pd.DataFrame(
        [
            ("CO-1", 2020, 5, 2, 3),
            ("CO-2-OLD", 2020, 7, 4, 3),
            ("CO-1", 2021, 8, 5, 3),
            # Present only for diagnostics; unknown evidence blocks its use.
            ("CO-2", 2021, 6, 4, 2),
        ],
        columns=["coc_id", "year", PIT_MEASURE, PIT_SHELTERED, PIT_UNSHELTERED],
    )
    pit["count_types"] = "Sheltered and Unsheltered Count"
    hic = pd.DataFrame(
        [
            ("CO-1", 2020, 11),
            ("CO-2", 2020, 13),
            ("CO-1", 2021, 12),
            ("CO-2", 2021, 14),
        ],
        columns=["coc_id", "year", HIC_MEASURE],
    )
    return {"pit_coc": pit, "hic_coc": hic}


def _assemble(*, source_panels=None, memberships=MEMBERSHIPS, evidence=None, **overrides):
    arguments = {
        "frozen_memberships": memberships,
        "requested_years": YEARS,
        "source_panels": _source_panels() if source_panels is None else source_panels,
        "outcome_bindings": OUTCOME_BINDINGS,
        "boundary_compatibility": _compatibility(memberships) if evidence is None else evidence,
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_VINTAGE,
        "source_panel_fingerprints": SOURCE_PANEL_FINGERPRINTS,
    }
    arguments.update(overrides)
    return assemble_site_year_panel(**arguments)


def test_aggregation_sums_exact_whole_coc_values_and_emits_canonical_schema():
    result = _assemble()

    assert tuple(result.panel.columns) == site_year_panel_columns(
        (PIT_MEASURE, PIT_SHELTERED, PIT_UNSHELTERED, HIC_MEASURE)
    )
    assert result.panel[["msa_id", "year"]].to_records(index=False).tolist() == [
        ("MSA-A", 2020),
        ("MSA-A", 2021),
    ]
    row_2020 = result.panel.loc[result.panel["year"] == 2020].iloc[0]
    assert row_2020[PIT_MEASURE] == 12
    assert row_2020[PIT_SHELTERED] == 6
    assert row_2020[PIT_UNSHELTERED] == 6
    assert row_2020[HIC_MEASURE] == 24
    assert row_2020[f"{PIT_MEASURE}__status"] == "complete"
    assert row_2020[f"{PIT_MEASURE}__expected_coc_count"] == 2
    assert row_2020[f"{PIT_MEASURE}__observed_coc_count"] == 2
    assert result.provenance["site_year_panel"]["boundary_evidence"] == {
        "evidence_id": "boundary-audit-fixture",
        "fingerprint": _compatibility().evidence_fingerprint,
        "schema_version": "coc_boundary_compatibility/1",
    }
    assert result.provenance["site_year_panel"]["row_count"] == 2


def test_unknown_boundary_is_reported_even_when_a_frozen_id_row_exists():
    result = _assemble()
    row_2021 = result.panel.loc[result.panel["year"] == 2021].iloc[0]

    assert row_2021[f"{PIT_MEASURE}__observed_coc_count"] == 2
    assert row_2021[f"{PIT_MEASURE}__missing_coc_ids"] == []
    assert row_2021[f"{PIT_MEASURE}__unknown_boundary_coc_ids"] == ["CO-2"]
    assert row_2021[PIT_MEASURE] is None or pd.isna(row_2021[PIT_MEASURE])
    assert row_2021[f"{PIT_MEASURE}__status"] == "incomplete_source_limited"
    assert row_2021[f"{PIT_SHELTERED}__status"] == "incomplete"
    assert row_2021[f"{PIT_UNSHELTERED}__status"] == "incomplete_source_limited"
    assert row_2021[HIC_MEASURE] == 26
    assert row_2021[f"{HIC_MEASURE}__status"] == "complete"


def test_incompatible_boundary_is_reported_and_never_summed():
    payload = _compatibility_payload()
    incompatible = next(
        record
        for record in payload["records"]
        if record["source_panel"] == "hic_coc"
        and record["frozen_coc_id"] == "CO-2"
        and record["year"] == 2020
    )
    incompatible["status"] = "incompatible"
    evidence = validate_coc_boundary_compatibility(
        payload,
        artifact_id=ARTIFACT_ID,
        threshold=THRESHOLD,
        membership_identity=MEMBERSHIP_IDENTITY,
        frozen_boundary_vintage=FROZEN_VINTAGE,
        frozen_memberships=MEMBERSHIPS,
        requested_years=YEARS,
        source_panel_vintages=SOURCE_PANEL_VINTAGES,
    )

    result = _assemble(evidence=evidence)
    row_2020 = result.panel.loc[result.panel["year"] == 2020].iloc[0]

    assert row_2020[f"{HIC_MEASURE}__observed_coc_count"] == 2
    assert row_2020[f"{HIC_MEASURE}__incompatible_boundary_coc_ids"] == ["CO-2"]
    assert pd.isna(row_2020[HIC_MEASURE])
    assert row_2020[f"{HIC_MEASURE}__status"] == "incomplete"


def test_missing_source_member_prevents_partial_sum():
    source_panels = _source_panels()
    source_panels["pit_coc"] = source_panels["pit_coc"].loc[
        source_panels["pit_coc"]["coc_id"] != "CO-2-OLD"
    ].copy()

    result = _assemble(source_panels=source_panels)
    row_2020 = result.panel.loc[result.panel["year"] == 2020].iloc[0]

    assert pd.isna(row_2020[PIT_MEASURE])
    assert row_2020[f"{PIT_MEASURE}__observed_coc_count"] == 1
    assert row_2020[f"{PIT_MEASURE}__missing_coc_ids"] == ["CO-2"]
    assert row_2020[f"{PIT_MEASURE}__status"] == "incomplete"


def test_v2_preaggregated_pit_and_hic_predecessor_sum_have_distinct_operations():
    evidence = _providence_v2_evidence()
    pit = pd.DataFrame(
        [("MA-505", 2020, 76, 50, 26)],
        columns=["coc_id", "year", PIT_MEASURE, PIT_SHELTERED, PIT_UNSHELTERED],
    )
    pit["count_types"] = "Sheltered and Unsheltered Count"
    hic = pd.DataFrame(
        [("MA-505", 2020, 300), ("MA-519", 2020, 378)],
        columns=["coc_id", "year", HIC_MEASURE],
    )
    result = _assemble(
        source_panels={"pit_coc": pit, "hic_coc": hic},
        memberships=PROVIDENCE_MEMBERSHIPS,
        evidence=evidence,
        requested_years=(2020,),
    )

    row = result.panel.iloc[0]
    assert row[PIT_MEASURE] == 76
    assert row[HIC_MEASURE] == 678
    assert row[f"{PIT_MEASURE}__status"] == "complete"
    assert row[f"{HIC_MEASURE}__status"] == "complete"


def test_v2_missing_hic_predecessor_prevents_partial_sum():
    evidence = _providence_v2_evidence()
    pit = pd.DataFrame(
        [("MA-505", 2020, 76, 50, 26)],
        columns=["coc_id", "year", PIT_MEASURE, PIT_SHELTERED, PIT_UNSHELTERED],
    )
    pit["count_types"] = "Sheltered and Unsheltered Count"
    hic = pd.DataFrame(
        [("MA-505", 2020, 300)],
        columns=["coc_id", "year", HIC_MEASURE],
    )
    result = _assemble(
        source_panels={"pit_coc": pit, "hic_coc": hic},
        memberships=PROVIDENCE_MEMBERSHIPS,
        evidence=evidence,
        requested_years=(2020,),
    )

    row = result.panel.iloc[0]
    assert pd.isna(row[HIC_MEASURE])
    assert row[f"{HIC_MEASURE}__observed_coc_count"] == 0
    assert row[f"{HIC_MEASURE}__missing_coc_ids"] == ["MA-505"]
    assert row[f"{HIC_MEASURE}__status"] == "incomplete"


@pytest.mark.parametrize(
    "invalid_value",
    [
        pytest.param(-1, id="negative"),
        pytest.param(1.5, id="fractional"),
        pytest.param(float("inf"), id="infinite"),
        pytest.param("not-a-count", id="nonnumeric"),
    ],
)
def test_invalid_count_is_incomplete_and_never_partially_summed(invalid_value):
    source_panels = _source_panels()
    source_panels["pit_coc"] = source_panels["pit_coc"].astype({PIT_MEASURE: "object"})
    source_panels["pit_coc"].loc[0, PIT_MEASURE] = invalid_value

    result = _assemble(source_panels=source_panels)
    row_2020 = result.panel.loc[result.panel["year"] == 2020].iloc[0]

    assert row_2020[PIT_MEASURE] is None or pd.isna(row_2020[PIT_MEASURE])
    assert row_2020[f"{PIT_MEASURE}__observed_coc_count"] == 1
    assert row_2020[f"{PIT_MEASURE}__missing_coc_ids"] == ["CO-1"]
    assert row_2020[f"{PIT_MEASURE}__status"] == "incomplete"


def test_duplicate_source_keys_fail_even_when_values_are_invalid():
    source_panels = _source_panels()
    duplicate = source_panels["pit_coc"].iloc[[0]].copy()
    duplicate[PIT_MEASURE] = None
    source_panels["pit_coc"] = pd.concat(
        [source_panels["pit_coc"], duplicate], ignore_index=True
    )

    with pytest.raises(ExecutorError, match="duplicate normalized key"):
        _assemble(source_panels=source_panels)


def test_evidence_must_match_exact_membership_identity_and_member_set():
    mismatched_identity = replace(
        _compatibility(), membership_identity="sha256:other-membership"
    )
    with pytest.raises(ExecutorError, match="does not match the selected site-universe identity"):
        _assemble(evidence=mismatched_identity)

    with pytest.raises(ExecutorError, match="outside the selected universe"):
        _assemble(memberships=(("MSA-A", "CO-1"),), evidence=_compatibility())


def test_memberships_require_nonempty_string_ids():
    with pytest.raises(ExecutorError, match="must be strings"):
        _assemble(memberships=((None, "CO-1"),), evidence=_compatibility())


@pytest.mark.parametrize(
    ("change", "message"),
    [
        pytest.param("missing-panel", "Missing source panel frames", id="missing-panel"),
        pytest.param("missing-measure", "missing required columns", id="missing-measure"),
        pytest.param(
            "unsupported-measure", "not registered additive counts", id="unsupported-measure"
        ),
        pytest.param(
            "missing-fingerprint",
            "Missing source panel fingerprints",
            id="missing-fingerprint",
        ),
    ],
)
def test_binding_and_provenance_inputs_are_required(change, message):
    source_panels = _source_panels()
    outcome_bindings = OUTCOME_BINDINGS
    fingerprints = SOURCE_PANEL_FINGERPRINTS
    if change == "missing-panel":
        source_panels = {"pit_coc": source_panels["pit_coc"]}
    elif change == "missing-measure":
        source_panels["hic_coc"] = source_panels["hic_coc"].drop(columns=[HIC_MEASURE])
    elif change == "unsupported-measure":
        outcome_bindings = {
            "pit": SiteYearSourceBinding(panel="pit_coc", measures=(PIT_MEASURE,)),
            "hic": SiteYearSourceBinding(panel="hic_coc", measures=(PIT_MEASURE,)),
        }
    elif change == "missing-fingerprint":
        fingerprints = {"pit_coc": SOURCE_PANEL_FINGERPRINTS["pit_coc"]}

    with pytest.raises(ExecutorError, match=message):
        _assemble(
            source_panels=source_panels,
            outcome_bindings=outcome_bindings,
            source_panel_fingerprints=fingerprints,
        )


def test_normalized_duplicate_source_keys_fail():
    source_panels = _source_panels()
    duplicate = source_panels["pit_coc"].iloc[[0]].copy()
    duplicate["coc_id"] = " CO-1 "
    duplicate["year"] = "2020"
    source_panels["pit_coc"] = pd.concat(
        [source_panels["pit_coc"], duplicate], ignore_index=True
    )

    with pytest.raises(ExecutorError, match="duplicate normalized key"):
        _assemble(source_panels=source_panels)


def test_nonfinite_source_year_is_an_actionable_key_error():
    source_panels = _source_panels()
    source_panels["pit_coc"] = source_panels["pit_coc"].astype({"year": "object"})
    source_panels["pit_coc"].loc[0, "year"] = float("inf")

    with pytest.raises(ExecutorError, match="non-integer year key"):
        _assemble(source_panels=source_panels)


def test_nullable_output_preserves_large_integer_totals_exactly():
    source_panels = _source_panels()
    source_panels["pit_coc"] = source_panels["pit_coc"].astype(
        {PIT_MEASURE: "object"}
    )
    source_panels["pit_coc"].loc[0, PIT_MEASURE] = 2**53 + 1
    source_panels["pit_coc"].loc[1, PIT_MEASURE] = 0

    result = _assemble(source_panels=source_panels)
    row_2020 = result.panel.loc[result.panel["year"] == 2020].iloc[0]

    assert int(row_2020[PIT_MEASURE]) == 2**53 + 1
    assert str(result.panel[PIT_MEASURE].dtype) == "Int64"


@pytest.mark.parametrize(
    ("declaration", "diagnostic"),
    [
        ("Sheltered-Only Count", "unobserved_count_coc_ids"),
        ("Sheltered and Partial Unsheltered Count", "partial_count_coc_ids"),
        (None, "unknown_count_type_coc_ids"),
        ("No 2020 PIT Count", "unobserved_count_coc_ids"),
    ],
    ids=["carried", "partial", "unknown", "no-count"],
)
def test_unsheltered_and_total_require_source_declared_complete_counts(declaration, diagnostic):
    sources = _source_panels()
    sources["pit_coc"].loc[sources["pit_coc"].coc_id.eq("CO-2-OLD"), "count_types"] = declaration
    original = sources["pit_coc"].copy(deep=True)
    result = _assemble(source_panels=sources)
    row = result.panel.loc[result.panel.year.eq(2020)].iloc[0]
    assert pd.isna(row[PIT_UNSHELTERED])
    assert pd.isna(row[PIT_MEASURE])
    assert row[f"{PIT_UNSHELTERED}__status"] == "source_limited"
    assert row[f"{PIT_MEASURE}__status"] == "source_limited"
    assert row[f"{PIT_UNSHELTERED}__{diagnostic}"] == ["CO-2"]
    if declaration in {"Sheltered-Only Count", "Sheltered and Partial Unsheltered Count"}:
        assert row[PIT_SHELTERED] == 6
        assert row[f"{PIT_SHELTERED}__status"] == "complete"
    else:
        assert pd.isna(row[PIT_SHELTERED])
    pd.testing.assert_frame_equal(sources["pit_coc"], original)


def test_msa_coc_measure_selection_preserves_nonnumeric_count_metadata():
    source = with_pit_count_metadata(pd.DataFrame({
        "coc_id": ["CO-1"],
        "year": [2020],
        "pit_total": [10],
        "pit_unsheltered": [5],
        "count_types": [COUNT_TYPES_SHELTERED_ONLY],
    }))
    record = _FrameRecord(
        dataset_id="pit", source_token="pit", geo_type="coc", year=2020, frame=source
    )

    selected = _coc_measures_for_year([record], year=2020)

    assert selected.count_types.tolist() == [COUNT_TYPES_SHELTERED_ONLY]
    assert selected.unsheltered_count_observed.tolist() == [False]
    assert selected.unsheltered_count_carried_forward.tolist() == [True]
    assert str(selected.unsheltered_count_observed.dtype) == "boolean"
