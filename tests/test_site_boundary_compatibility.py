"""Truth table for site-universe CoC boundary evidence validation.

The fixture has two frozen CoCs in one MSA over two years. PIT and HIC use
separate source boundary vintages. A missing row is explicitly unknown; a
stable identifier is never accepted as compatibility evidence by itself.
"""

from __future__ import annotations

import csv
import json
from copy import deepcopy
from itertools import product
from pathlib import Path

import pytest

from hhplab.geographies.coc.coc_validate import (
    BoundaryCompatibilityError,
    validate_coc_boundary_compatibility,
)
from hhplab.schema.contracts import DataAnnotationSpec

ARTIFACT_ID = "overlap_samples"
THRESHOLD = 0.95
MEMBERSHIP_IDENTITY = "sha256:membership-fixture"
FROZEN_BOUNDARY_VINTAGE = "2024"
FROZEN_MEMBERSHIPS = (("MSA-A", "CO-1"), ("MSA-A", "CO-2"))
REQUESTED_YEARS = (2020, 2021)
SOURCE_PANEL_VINTAGES = {
    ("pit_panel", 2020): "2020",
    ("pit_panel", 2021): "2021",
    ("hic_panel", 2020): "2020",
    ("hic_panel", 2021): "2021",
}

SITE_INPUTS = Path(__file__).resolve().parents[1] / "recipes" / "site-inputs"
WESTERN_MASS_COCS = ("MA-504", "MA-507")
WESTERN_MASS_OUTCOMES = ("pit", "hic")
WESTERN_MASS_INCOMPATIBLE_YEARS = (2010, 2011, 2012)
WESTERN_MASS_CELLS = tuple(
    product(WESTERN_MASS_COCS, WESTERN_MASS_OUTCOMES, WESTERN_MASS_INCOMPATIBLE_YEARS)
)
YUBA_SUTTER_OVERRIDE_YEARS = tuple(range(2010, 2015))
YUBA_SUTTER_COUNTY_GEOIDS = ("06101", "06115")
CITY_OMISSION_OVERRIDE_YEARS = (2012, 2013, 2014)
CITY_OMISSION_COUNTIES = {"MI-514": ("26025",), "CA-514": ("06019", "06039")}
CITY_OMISSION_NAMES = {"MI-514": "Battle Creek", "CA-514": "Clovis"}
CITY_OMISSION_CELLS = tuple(product(CITY_OMISSION_COUNTIES, CITY_OMISSION_OVERRIDE_YEARS))
ADDITIONAL_MUNICIPAL_GIS_OVERRIDES = (
    ("CA-511", 2010, 2012, ("06077",), ("Lodi",)),
    ("FL-504", 2013, 2014, ("1254200",), ("Palm Coast",)),
    ("FL-505", 2013, 2014, ("1224475",), ("Fort Walton Beach",)),
    ("FL-513", 2013, 2014, ("1213150", "1271900"), ("Cocoa", "Titusville")),
    ("FL-601", 2013, 2014, ("1276582",), ("Weston",)),
    ("FL-605", 2013, 2014, ("1235875", "1275812"), ("Wellington", "Jupiter")),
)
FL606_MUNICIPAL_YEARS = (2012, 2013, 2014)
FL606_PLACE_GEOIDS = ("1243083", "1247625")
FL606_PRESERVED_NEIGHBORS = ("FL-604", "FL-517", "FL-603")
WASHINGTON_DECISIONS_PATH = SITE_INPUTS / "coc-boundary-washington-decisions.json"
WASHINGTON_NOTE_IDS = (
    "hud-annual-wa500-wa503-cross-county-assignments",
    "hud-annual-wa504-bothell-source-assignments",
    "hud-annual-washington-2015-source-overlaps",
)
PERIMETER_DECISIONS_PATH = SITE_INPUTS / "coc-boundary-fl-ca-maintenance-decisions.json"
PERIMETER_CORE_COCS = (
    "FL-500", "FL-504", "FL-505", "FL-511", "FL-513", "FL-600", "FL-601",
    "FL-602", "FL-603", "FL-605", "CA-502", "CA-504", "CA-505", "CA-507",
    "CA-512", "CA-517", "CA-518", "CA-601", "CA-602", "CA-603", "CA-606", "CA-611",
)
PERIMETER_CONTEXT_COCS = ("CA-502", "CA-505", "CA-603", "CA-606", "CA-611")
# Truth table: 2023 redraws bind baseline 2022 plus 2023/24; 2022 redraws
# bind 2021 through 2024; the separate 2024 adjustment binds 2023/24 only.
PERIMETER_SCOPE_CASES = (
    *((coc, 2023, (2022, 2023, 2024)) for coc in PERIMETER_CORE_COCS),
    *((coc, 2022, (2021, 2022, 2023, 2024)) for coc in PERIMETER_CONTEXT_COCS),
    ("CA-601", 2024, (2023, 2024)),
)


@pytest.mark.parametrize(
    "coc,transition_year,source_years", PERIMETER_SCOPE_CASES,
    ids=[f"{coc}-{year}" for coc, year, _ in PERIMETER_SCOPE_CASES],
)
def test_perimeter_maintenance_notes_preserve_original_source_year_scope(
    coc, transition_year, source_years,
):
    policy = json.loads(PERIMETER_DECISIONS_PATH.read_text())
    matches = [
        n for n in policy["geometry_annotations"]
        if n["scope"]["geographies"]["include"] == [coc]
        and f"-{transition_year}-" in n["id"]
    ]
    assert len(matches) == 1
    note = DataAnnotationSpec.model_validate({**policy["annotation_defaults"], **matches[0]})
    assert note.scope.years == list(source_years)
    assert note.scope.columns == ["geometry"]
    assert note.revision == 1 and note.category == "caveat"
    assert note.evidence and note.uncertainty
    assert "Published PIT/HIC" in note.text and "membership" in note.text
    assert "count-scope transfer" in note.text
    assert policy["publish_annotation_text"] is True


def test_perimeter_catalog_has_exact_reviewed_cases_without_fl606_redefinition():
    policy = json.loads(PERIMETER_DECISIONS_PATH.read_text())
    notes = policy["geometry_annotations"]
    assert len(notes) == len(PERIMETER_SCOPE_CASES)
    assert len({note["id"] for note in notes}) == len(notes)
    assert {c for note in notes for c in note["scope"]["geographies"]["include"]} == set(PERIMETER_CORE_COCS)
    assert "Retain existing MSA memberships" in policy["membership_treatment"]


def test_marysville_repair_targets_the_two_year_city_gap():
    with (SITE_INPUTS / "coc-boundary-municipal-omissions.csv").open(newline="") as stream:
        matches = [row for row in csv.DictReader(stream) if row["coc_id"] == "WA-504"]
    assert len(matches) == 1
    rule = matches[0]
    assert (rule["first_year"], rule["last_year"]) == ("2013", "2014")
    assert (rule["place_vintage"], rule["place_geoids"]) == ("2010", "5343955")
    assert (rule["bracket_before_year"], rule["bracket_after_year"]) == ("2012", "2015")
    assert "Marysville" in rule["footnote"] and "GIS omission" in rule["footnote"]
    assert "PIT/HIC counts are unchanged" in rule["footnote"]


def test_washington_exception_removes_only_donor_covered_restoration():
    policy = json.loads(WASHINGTON_DECISIONS_PATH.read_text())
    assert policy["metro_id"] == "42660"
    assert policy["metro_cocs"] == ["WA-500", "WA-503", "WA-504"]
    exception = policy["restoration_exception"]
    assert (exception["coc_id"], exception["year"], exception["donor_coc_id"]) == (
        "WA-504", 2016, "WA-500",
    )
    assert exception["place_geoid"] == "5307380"
    assert "Preserve original WA-504 coverage and all other restorations" in exception["selection"]
    note = DataAnnotationSpec.model_validate(exception["annotation"])
    assert note.scope.years == [2016] and note.scope.columns == ["geometry"]
    assert "PIT/HIC counts are unchanged" in note.public_text


@pytest.mark.parametrize("note_id", WASHINGTON_NOTE_IDS)
def test_washington_source_caveats_have_portable_original_year_scopes(note_id):
    policy = json.loads(WASHINGTON_DECISIONS_PATH.read_text())
    note = next(item for item in policy["geometry_annotations"] if item["id"] == note_id)
    parsed = DataAnnotationSpec.model_validate(note)
    assert parsed.scope.columns == ["geometry"]
    assert parsed.public_text and parsed.evidence
    if note_id.endswith("2015-source-overlaps"):
        assert parsed.scope.years == [2015]
        assert parsed.scope.geographies.include == policy["metro_cocs"]
    elif "wa500-wa503" in note_id:
        assert parsed.scope.years == list(range(2013, 2025))
        assert parsed.scope.geographies.include == ["WA-500", "WA-503"]
    else:
        assert parsed.scope.years == [2013, 2014, 2015, 2016]
        assert parsed.scope.geographies.include == ["WA-504"]


def test_snohomish_count_method_note_is_separate_from_geometry_caveats():
    policy = json.loads(WASHINGTON_DECISIONS_PATH.read_text())
    note = DataAnnotationSpec.model_validate(policy["count_method_annotation"])
    assert note.category == "measurement_change"
    assert note.scope.columns == ["pit_total", "pit_sheltered"]
    assert note.scope.years == list(range(2013, 2025))
    assert note.scope.geographies.include == ["WA-504"]
    assert note.evidence[0].kind == "url"
    assert "HIC effects have not been quantified" in note.public_text


def test_fl606_city_repair_preserves_separate_county_line_allocations():
    with (SITE_INPUTS / "coc-boundary-municipal-omissions.csv").open(newline="") as stream:
        matches = [row for row in csv.DictReader(stream) if row["coc_id"] == "FL-606"]
    assert len(matches) == 1
    rule = matches[0]
    actual_years = tuple(range(int(rule["first_year"]), int(rule["last_year"]) + 1))
    assert actual_years == FL606_MUNICIPAL_YEARS
    assert tuple(rule["place_geoids"].split(";")) == FL606_PLACE_GEOIDS
    assert rule["place_vintage"] == "2010"
    assert (rule["bracket_before_year"], rule["bracket_after_year"]) == ("2011", "2015")
    assert rule["decision_basis"] == "study_owner_decision"
    assert "Naples and Marco Island" in rule["footnote"]
    assert "GIS omissions" in rule["footnote"]
    assert all(coc in rule["footnote"] for coc in FL606_PRESERVED_NEIGHBORS)
    assert "Original GIS files are retained; PIT/HIC counts are unchanged" in rule["footnote"]
    assert rule["decision_ref"].endswith("#fl-606-naples-and-marco-island-2026-10-27")


@pytest.mark.parametrize(
    "coc_id,first_year,last_year,geoids,places",
    ADDITIONAL_MUNICIPAL_GIS_OVERRIDES,
    ids=[case[0] for case in ADDITIONAL_MUNICIPAL_GIS_OVERRIDES],
)
def test_additional_municipal_gis_overrides_record_owner_scope(
    coc_id, first_year, last_year, geoids, places,
):
    filename = (
        "coc-boundary-county-overrides.csv" if coc_id == "CA-511"
        else "coc-boundary-municipal-omissions.csv"
    )
    with (SITE_INPUTS / filename).open(newline="") as stream:
        matches = [row for row in csv.DictReader(stream) if row["coc_id"] == coc_id]
    assert len(matches) == 1
    rule = matches[0]
    assert (int(rule["first_year"]), int(rule["last_year"])) == (first_year, last_year)
    geography = "county" if coc_id == "CA-511" else "place"
    assert tuple(rule[f"{geography}_geoids"].split(";")) == geoids
    assert rule[f"{geography}_vintage"] == "2010"
    if coc_id != "CA-511":
        assert (rule["bracket_before_year"], rule["bracket_after_year"]) == ("2012", "2015")
        assert "preserving HUD's coastal-water extent" in rule["footnote"]
    assert rule["decision_basis"] == "study_owner_decision"
    assert all(place in rule["footnote"] for place in places)
    assert "GIS error" in rule["footnote"]
    assert "Original GIS files are retained; PIT/HIC counts are unchanged" in rule["footnote"]
    assert rule["decision_ref"].endswith("#lodi-and-florida-gis-omissions-2026-10-27")


@pytest.mark.parametrize(
    "coc_id,year", CITY_OMISSION_CELLS,
    ids=[f"{coc}-{year}" for coc, year in CITY_OMISSION_CELLS],
)
def test_city_omission_override_restores_complete_counties(coc_id, year):
    with (SITE_INPUTS / "coc-boundary-county-overrides.csv").open(newline="") as stream:
        selected = [row for row in csv.DictReader(stream) if row["coc_id"] == coc_id]
    assert len(selected) == 1
    rule = selected[0]
    interval = tuple(range(int(rule["first_year"]), int(rule["last_year"]) + 1))
    assert interval == CITY_OMISSION_OVERRIDE_YEARS and year in interval
    assert tuple(rule["county_geoids"].split(";")) == CITY_OMISSION_COUNTIES[coc_id]
    assert rule["county_vintage"] == "2010"
    assert rule["decision_basis"] == "study_owner_decision"
    assert CITY_OMISSION_NAMES[coc_id] in rule["footnote"]
    assert "GIS error" in rule["footnote"]
    assert "PIT/HIC counts are unchanged" in rule["footnote"]


@pytest.mark.parametrize("year", YUBA_SUTTER_OVERRIDE_YEARS)
def test_yuba_sutter_county_override_covers_pre2015_source_errors(year):
    with (SITE_INPUTS / "coc-boundary-county-overrides.csv").open(newline="") as stream:
        rules = [row for row in csv.DictReader(stream) if row["coc_id"] == "CA-524"]
    assert len(rules) == 1
    rule = rules[0]
    interval = tuple(range(int(rule["first_year"]), int(rule["last_year"]) + 1))
    assert interval == YUBA_SUTTER_OVERRIDE_YEARS
    assert int(rule["first_year"]) <= year <= int(rule["last_year"])
    assert tuple(rule["county_geoids"].split(";")) == YUBA_SUTTER_COUNTY_GEOIDS
    assert rule["county_vintage"] == "2010"
    assert rule["decision_basis"] == "study_owner_decision"
    assert "2010–2012" in rule["footnote"] and "2013–2014" in rule["footnote"]
    assert "GIS errors" in rule["footnote"]
    assert "Original source files are retained" in rule["footnote"]


@pytest.mark.parametrize(
    "coc_id,outcome,year",
    WESTERN_MASS_CELLS,
    ids=[f"{coc}-{outcome}-{year}" for coc, outcome, year in WESTERN_MASS_CELLS],
)
def test_western_mass_owner_decision_flags_exact_historical_cells(coc_id, outcome, year):
    with (SITE_INPUTS / "coc-count-geography-decisions.csv").open(newline="") as stream:
        decisions = list(csv.DictReader(stream))
    actual_cells = {
        (row["coc_id"], panel, observation_year)
        for row in decisions
        for panel in row["outcomes"].split(";")
        for observation_year in range(int(row["first_year"]), int(row["last_year"]) + 1)
    }
    assert actual_cells == set(WESTERN_MASS_CELLS)
    matches = [row for row in decisions if row["coc_id"] == coc_id]
    assert len(matches) == 1
    decision = matches[0]
    assert decision["status"] == "incompatible"
    assert decision["decision_basis"] == "study_owner_decision"
    assert decision["provisional_start_year"] == "2013"
    assert "not established by primary sources" in decision["footnote"]
    assert "does not certify compatibility from 2013 onward" in decision["footnote"]
    assert "downstream study" in decision["revisit_condition"]


def test_packaged_ma504_evidence_blocks_2010_2012_without_approving_2013():
    payload = json.loads(
        (SITE_INPUTS / "coc-boundary-compatibility-longitudinal-mergers.json").read_text()
    )
    # Project the packaged rows into a one-member validation fixture. This does
    # not claim a different membership identity for the real longitudinal study.
    payload["records"] = [
        row for row in payload["records"] if row["frozen_coc_id"] == "MA-504"
    ]
    payload.update(artifact_id=ARTIFACT_ID, membership_identity=MEMBERSHIP_IDENTITY)
    years = (*WESTERN_MASS_INCOMPATIBLE_YEARS, 2013)
    panels = ("pit_p2024", "hic_annual")
    result = validate_coc_boundary_compatibility(
        payload,
        artifact_id=ARTIFACT_ID,
        threshold=payload["threshold"],
        membership_identity=MEMBERSHIP_IDENTITY,
        frozen_boundary_vintage="2024",
        frozen_memberships=(("44140", "MA-504"),),
        requested_years=years,
        source_panel_vintages={
            (panel, year): "2024" if panel == "pit_p2024" else str(year)
            for panel, year in product(panels, years)
        },
    )
    blocked = [
        row for row in result.records if row.year in WESTERN_MASS_INCOMPATIBLE_YEARS
    ]
    assert len(blocked) == len(panels) * len(WESTERN_MASS_INCOMPATIBLE_YEARS)
    assert {row.status for row in blocked} == {"incompatible"}
    assert all(row.count_operation is None and not row.source_coc_ids for row in blocked)
    assert all("not established by primary sources" in row.reason for row in blocked)
    assert {row.status for row in result.records if row.year == 2013} == {"unknown"}

COMPATIBLE_PIT_CO1_2020 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-1",
    "year": 2020,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2020",
    "status": "compatible",
    "reason": "Archived boundary comparison confirms equivalent coverage.",
    "evidence_ref": "audit://pit/CO-1/2020",
}
MAPPED_PIT_CO2_2020 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-2",
    "year": 2020,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2020",
    "status": "mapped",
    "reason": "Documented one-to-one CoC rename.",
    "evidence_ref": "audit://pit/CO-2/2020",
    "source_coc_id": "CO-2-OLD",
    "approval_ref": "approval://boundary-map/2020-CO-2",
}
INCOMPATIBLE_PIT_CO2_2021 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-2",
    "year": 2021,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2021",
    "status": "incompatible",
    "reason": "The historical CoC boundary has a documented material change.",
    "evidence_ref": "audit://pit/CO-2/2021",
}
COMPATIBLE_HIC_CO1_2020 = {
    **COMPATIBLE_PIT_CO1_2020,
    "source_panel": "hic_panel",
    "source_boundary_vintage": "2020",
    "evidence_ref": "audit://hic/CO-1/2020",
}
MAPPED_HIC_CO2_2020 = {
    **MAPPED_PIT_CO2_2020,
    "source_panel": "hic_panel",
    "evidence_ref": "audit://hic/CO-2/2020",
    "source_coc_id": "HIC-CO-2-OLD",
    "approval_ref": "approval://hic-boundary-map/2020-CO-2",
}
COMPATIBLE_HIC_CO1_2021 = {
    **COMPATIBLE_PIT_CO1_2020,
    "source_panel": "hic_panel",
    "year": 2021,
    "source_boundary_vintage": "2021",
    "evidence_ref": "audit://hic/CO-1/2021",
}
VALID_RECORDS = (
    COMPATIBLE_PIT_CO1_2020,
    MAPPED_PIT_CO2_2020,
    {
        **COMPATIBLE_PIT_CO1_2020,
        "year": 2021,
        "source_boundary_vintage": "2021",
        "evidence_ref": "audit://pit/CO-1/2021",
    },
    INCOMPATIBLE_PIT_CO2_2021,
    COMPATIBLE_HIC_CO1_2020,
    MAPPED_HIC_CO2_2020,
    COMPATIBLE_HIC_CO1_2021,
)


def _evidence_payload(records=VALID_RECORDS):
    return {
        "schema_version": "coc_boundary_compatibility/1",
        "evidence_id": "boundary-audit-2026-01",
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_BOUNDARY_VINTAGE,
        "records": deepcopy(list(records)),
    }


def _v2_evidence_payload(records=VALID_RECORDS):
    v2_records = []
    for source_record in records:
        record = deepcopy(source_record)
        old_source_id = record.pop("source_coc_id", None)
        status = record["status"]
        record["source_coc_ids"] = (
            [record["frozen_coc_id"]]
            if status == "compatible"
            else [old_source_id]
            if status == "mapped"
            else []
        )
        record["count_operation"] = "single" if record["source_coc_ids"] else None
        record["geometry_status"] = (
            "compatible" if status in {"compatible", "mapped"} else "unknown"
        )
        v2_records.append(record)
    return {
        **_evidence_payload([]),
        "schema_version": "coc_boundary_compatibility/2",
        "records": v2_records,
    }


def _validate(payload=None, **overrides):
    arguments = {
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_BOUNDARY_VINTAGE,
        "frozen_memberships": FROZEN_MEMBERSHIPS,
        "requested_years": REQUESTED_YEARS,
        "source_panel_vintages": SOURCE_PANEL_VINTAGES,
    }
    arguments.update(overrides)
    return validate_coc_boundary_compatibility(
        _evidence_payload() if payload is None else payload,
        **arguments,
    )


def test_valid_evidence_resolves_per_panel_and_emits_unknown_for_missing_rows():
    result = _validate()

    assert result.evidence_id == "boundary-audit-2026-01"
    assert result.artifact_id == ARTIFACT_ID
    assert result.threshold == THRESHOLD
    assert result.membership_identity == MEMBERSHIP_IDENTITY
    assert result.frozen_boundary_vintage == FROZEN_BOUNDARY_VINTAGE
    assert result.evidence_fingerprint.startswith("sha256:")
    assert len(result.records) == 8

    pit_rename = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("pit_panel", "CO-2", 2020)
    )
    assert (pit_rename.msa_id, pit_rename.status, pit_rename.source_coc_id) == (
        "MSA-A", "mapped", "CO-2-OLD"
    )
    hic_rename = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("hic_panel", "CO-2", 2020)
    )
    assert hic_rename.source_coc_id == "HIC-CO-2-OLD"

    missing = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("hic_panel", "CO-2", 2021)
    )
    assert missing.status == "unknown"
    assert missing.source_coc_id is None
    assert "No boundary compatibility evidence" in missing.reason


def test_integer_frozen_vintage_and_optional_evidence_fields_are_supported():
    result = _validate(frozen_boundary_vintage=2024)

    assert result.frozen_boundary_vintage == "2024"
    assert next(row for row in result.records if row.status == "compatible").source_coc_id == "CO-1"


def test_explicit_unknown_record_can_omit_optional_evidence_fields():
    payload = _evidence_payload()
    unknown_record = payload["records"].pop(0)
    unknown_record["status"] = "unknown"
    unknown_record["reason"] = "Historical boundary evidence is unavailable."
    unknown_record.pop("evidence_ref")
    payload["records"].append(unknown_record)

    result = _validate(payload)

    unknown = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("pit_panel", "CO-1", 2020)
    )
    assert unknown.status == "unknown"
    assert unknown.evidence_ref is None


def test_evidence_fingerprint_changes_when_evidence_changes():
    original = _validate()
    payload = _evidence_payload()
    payload["records"][0]["reason"] = "A different documented boundary comparison."

    changed = _validate(payload)

    assert changed.evidence_fingerprint != original.evidence_fingerprint


def test_v2_reconciles_multiple_source_ids_without_claiming_geometry_equivalence():
    payload = _v2_evidence_payload()
    record = next(
        row for row in payload["records"]
        if (row["source_panel"], row["frozen_coc_id"], row["year"])
        == ("pit_panel", "CO-1", 2020)
    )
    record.update(
        {
            "status": "reconciled",
            "source_coc_ids": ["CO-1-PRE", "CO-1-OLD"],
            "count_operation": "sum",
            "evidence_ref": "audit://admin-reconciliation",
            "count_evidence_ref": "audit://pit-source-counts",
            "approval_ref": "review://providence/MA-505",
            "reconciliation_basis": "administrative predecessor union",
            "geometry_status": "different_unresolved",
            "geometry_evidence_ref": "audit://geometry/2020",
            "geometry_residual": "13.499083 km2 symmetric difference; cause unresolved",
        }
    )

    result = _validate(payload)
    resolved = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year)
        == ("pit_panel", "CO-1", 2020)
    )

    assert resolved.status == "reconciled"
    assert resolved.source_coc_ids == ("CO-1-PRE", "CO-1-OLD")
    assert resolved.count_operation == "sum"
    assert resolved.geometry_status == "different_unresolved"
    assert resolved.geometry_residual.startswith("13.499083 km2")


@pytest.mark.parametrize(
    ("change", "error_code"),
    [
        pytest.param("duplicate-source-id", "duplicate_source_id", id="duplicate-source-id"),
        pytest.param(
            "missing-count-evidence", "incomplete_reconciliation", id="count-evidence-required"
        ),
        pytest.param(
            "missing-geometry-residual", "missing_geometry_residual", id="residual-required"
        ),
        pytest.param("sum-one-id", "invalid_count_operation", id="sum-needs-two-ids"),
        pytest.param(
            "preaggregated-no-scope", "invalid_count_operation", id="preaggregated-scope-required"
        ),
        pytest.param(
            "preaggregated-overlap",
            "non_bijective_mapping",
            id="predecessor-scope-cannot-cross-members",
        ),
        pytest.param(
            "compatible-preaggregated",
            "invalid_resolved_record",
            id="compatible-cannot-be-preaggregated",
        ),
        pytest.param("source-reused", "non_bijective_mapping", id="source-cannot-cross-members"),
    ],
)
def test_v2_reconciliation_requires_explicit_complete_evidence(change, error_code):
    payload = _v2_evidence_payload()
    row = payload["records"][0]
    row.update(
        {
            "status": "reconciled",
            "source_coc_ids": ["CO-1-PRE", "CO-1-OLD"],
            "count_operation": "sum",
            "evidence_ref": "audit://admin-reconciliation",
            "count_evidence_ref": "audit://source-counts",
            "approval_ref": "review://providence/MA-505",
            "reconciliation_basis": "administrative predecessor union",
            "geometry_status": "different_unresolved",
            "geometry_evidence_ref": "audit://geometry/2020",
            "geometry_residual": "documented residual",
        }
    )
    if change == "duplicate-source-id":
        row["source_coc_ids"] = ["CO-1", "CO-1"]
    elif change == "missing-count-evidence":
        row["count_evidence_ref"] = None
    elif change == "missing-geometry-residual":
        row["geometry_residual"] = None
    elif change == "sum-one-id":
        row["source_coc_ids"] = ["CO-1"]
    elif change == "preaggregated-no-scope":
        row["source_coc_ids"] = ["CO-1"]
        row["count_operation"] = "preaggregated"
    elif change == "preaggregated-overlap":
        row["source_coc_ids"] = ["CO-1"]
        row["count_operation"] = "preaggregated"
        row["preaggregated_source_coc_ids"] = ["CO-1", "CO-2-OLD"]
        row["geometry_status"] = "compatible"
        row.pop("geometry_evidence_ref")
        row.pop("geometry_residual")
    elif change == "compatible-preaggregated":
        row["status"] = "compatible"
        row["source_coc_ids"] = ["CO-1"]
        row["count_operation"] = "preaggregated"
        row["preaggregated_source_coc_ids"] = ["CO-1", "CO-OLD"]
        row["geometry_status"] = "compatible"
        row.pop("count_evidence_ref")
        row.pop("approval_ref")
        row.pop("reconciliation_basis")
        row.pop("geometry_evidence_ref")
        row.pop("geometry_residual")
    elif change == "source-reused":
        row["source_coc_ids"] = ["CO-1", "CO-OLD"]
        other = next(
            item for item in payload["records"]
            if (item["source_panel"], item["frozen_coc_id"], item["year"])
            == ("pit_panel", "CO-2", 2020)
        )
        other["status"] = "reconciled"
        other["source_coc_ids"] = ["CO-OLD"]
        other["count_operation"] = "single"
        other["evidence_ref"] = "audit://other"
        other["count_evidence_ref"] = "audit://other-count"
        other["approval_ref"] = "review://other"
        other["reconciliation_basis"] = "documented"
        other["geometry_status"] = "compatible"

    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(payload)

    assert error.value.code == error_code


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        pytest.param(
            "schema_version", "coc_boundary_compatibility/99", "unknown_schema", id="unknown-schema"
        ),
        pytest.param(
            "artifact_id", "another-artifact", "artifact_mismatch", id="producer-artifact-mismatch"
        ),
        pytest.param("threshold", 0.90, "threshold_mismatch", id="threshold-mismatch"),
        pytest.param(
            "membership_identity",
            "sha256:other-membership",
            "membership_mismatch",
            id="membership-changed",
        ),
        pytest.param(
            "frozen_boundary_vintage",
            "2020",
            "frozen_vintage_mismatch",
            id="producer-vintage-mismatch",
        ),
    ],
)
def test_header_identity_mismatches_are_rejected(field, value, error_code):
    payload = _evidence_payload()
    payload[field] = value

    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(payload)

    assert error.value.code == error_code


@pytest.mark.parametrize(
    ("change", "error_code"),
    [
        pytest.param("duplicate", "duplicate_record", id="duplicate-panel-member-year"),
        pytest.param(
            "wrong-source-vintage", "source_vintage_mismatch", id="source-vintage-mismatch"
        ),
        pytest.param(
            "wrong-frozen-vintage", "frozen_vintage_mismatch", id="record-frozen-vintage-mismatch"
        ),
        pytest.param("wrong-panel", "record_outside_universe", id="unknown-source-panel"),
        pytest.param("mapped-many-to-one", "non_bijective_mapping", id="mapped-many-to-one"),
        pytest.param("mapped-missing-approval", "incomplete_mapping", id="mapped-missing-approval"),
        pytest.param(
            "compatible-no-evidence",
            "missing_evidence_reference",
            id="stable-id-without-evidence",
        ),
    ],
)
def test_record_evidence_failures_are_actionable(change, error_code):
    payload = _evidence_payload()
    if change == "duplicate":
        payload["records"].append(deepcopy(COMPATIBLE_PIT_CO1_2020))
    elif change == "wrong-source-vintage":
        payload["records"][0]["source_boundary_vintage"] = "2019"
    elif change == "wrong-frozen-vintage":
        payload["records"][0]["frozen_boundary_vintage"] = "2020"
    elif change == "wrong-panel":
        payload["records"][0]["source_panel"] = "other_panel"
    elif change == "mapped-many-to-one":
        payload["records"][1]["source_coc_id"] = "CO-1"
    elif change == "mapped-missing-approval":
        payload["records"][1]["approval_ref"] = None
    elif change == "compatible-no-evidence":
        payload["records"][0]["evidence_ref"] = None

    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(payload)

    assert error.value.code == error_code


@pytest.mark.parametrize(
    ("memberships", "years", "panel_vintages", "error_code"),
    [
        pytest.param(
            (("MSA-A", "CO-1"), ("MSA-B", "CO-1")),
            REQUESTED_YEARS,
            SOURCE_PANEL_VINTAGES,
            "shared_coc_membership",
            id="shared-coc-rejected",
        ),
        pytest.param(
            FROZEN_MEMBERSHIPS,
            (),
            SOURCE_PANEL_VINTAGES,
            "empty_years",
            id="empty-requested-years",
        ),
        pytest.param(
            FROZEN_MEMBERSHIPS,
            REQUESTED_YEARS,
            {("pit_panel", 2020): "2020"},
            "missing_source_vintage",
            id="source-vintage-required-for-each-year",
        ),
    ],
)
def test_membership_and_source_vintage_contracts_are_enforced(
    memberships, years, panel_vintages, error_code
):
    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(
            frozen_memberships=memberships,
            requested_years=years,
            source_panel_vintages=panel_vintages,
        )

    assert error.value.code == error_code
