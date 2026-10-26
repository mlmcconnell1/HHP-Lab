"""Reviewed-baseline and dated-exception right-to-shelter mapping.

The maintained inputs are a versioned, explicitly scoped reviewed baseline
and cited policy exception intervals. Absence of an exception means no covered
right only for jurisdiction-years in the reviewed baseline; unreviewed years
and explicit unknown gaps remain errors. Annual mapping rows are generated at
the January 31 reference date.

Strict coding counts only enforceable year-round strength. Broad coding counts
enforceable and conditional year-round strength, and can optionally include
the separate weather-only indication. Geography flags default to a population
weighted majority; accompanying shares preserve the degree of coverage.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import ValidationError

from hhplab.geographies.msa.msa_definitions import (
    DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION,
)
from hhplab.schema.contracts import DataAnnotationSpec, serialize_annotation_contract
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

STRENGTH_LEVELS = ("enforceable", "conditional", "weather_only", "none")
POPULATIONS = ("single_adults", "families", "pregnant_people")
CODING_RULES = ("strict", "broad")
GEOGRAPHY_RULES = ("majority", "any")
# Independent HUD/TIGER reprojections can create microscopic boundary slivers.
# Ignore less than one hundred-millionth of a county; retain real fragments.
COUNTY_OVERLAY_SLIVER_SHARE = 1e-8
SOURCE_TYPES = {"statute", "regulation", "administrative_directive", "court_order", "secondary"}
REDISTRIBUTION_STATUSES = {
    "public_domain", "public", "permission_required", "unknown", "not_applicable"
}

ANNUAL_JURISDICTION_COLUMNS = (
    "jurisdiction_id",
    "jurisdiction_type",
    "year",
    "single_adults_strength",
    "single_adults_weather_only",
    "families_strength",
    "families_weather_only",
    "pregnant_people_strength",
    "pregnant_people_weather_only",
    "citation",
    "citation_source_type",
    "citation_redistribution",
    "effective_date",
    "ended_date",
)
# Public annual contract used by mapping functions; maintained source inputs
# use the separate baseline JSON and exception CSV contracts below.
JURISDICTION_SOURCE_COLUMNS = ANNUAL_JURISDICTION_COLUMNS
EXCEPTION_COLUMNS = (
    "exception_id", "jurisdiction_id", "jurisdiction_type", "effective_date", "ended_date",
    "single_adults_strength", "single_adults_weather_only", "families_strength",
    "families_weather_only", "pregnant_people_strength", "pregnant_people_weather_only",
    "single_adults_qualification_scope", "single_adults_qualification_summary",
    "families_qualification_scope", "families_qualification_summary",
    "pregnant_people_qualification_scope", "pregnant_people_qualification_summary",
    "citation", "citation_source_type", "citation_redistribution",
)
BASELINE_FILENAME = "right_to_shelter__baseline.json"
EXCEPTIONS_FILENAME = "right_to_shelter__exceptions.csv"
JURISDICTION_FLAG_COLUMNS = tuple(f"rts_{population}" for population in POPULATIONS)
POPULATION_SHARE_COLUMNS = tuple(
    f"rts_{population}_population_share" for population in POPULATIONS
)
MAPPING_COLUMNS = ("geo_id", "year", *JURISDICTION_FLAG_COLUMNS, *POPULATION_SHARE_COLUMNS)
COUNTY_MEMBERSHIP_COLUMNS = ("geo_id", "county_fips", "allocation_share")
COUNTY_POPULATION_COLUMNS = (
    "county_fips",
    "year",
    "population_single_adults",
    "population_families",
    "population_pregnant_people",
)

# Census county equivalents for New York City. Statewide rows use two-digit
# state FIPS identifiers; these five counties are assigned to the explicit NYC
# sub-state jurisdiction when present in the curated table.
NYC_COUNTY_FIPS = ("36005", "36047", "36061", "36081", "36085")
DEFAULT_SUBSTATE_COUNTY_MEMBERSHIP = pd.DataFrame(
    {"jurisdiction_id": ["NYC"] * len(NYC_COUNTY_FIPS),
     "jurisdiction_type": ["sub_state"] * len(NYC_COUNTY_FIPS),
     "county_fips": list(NYC_COUNTY_FIPS)}
)
SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS = (
    "jurisdiction_id", "jurisdiction_type", "county_fips",
)

RTS_ANNOTATION_ID = "right-to-shelter-geographic-approximation"
RTS_QUALIFICATION_ANNOTATION_ID = "right-to-shelter-subgroup-qualification"
RTS_SOURCE_TABLE_FILENAME = EXCEPTIONS_FILENAME


class RightToShelterError(ValueError):
    """Raised when a right-to-shelter source or mapping is incomplete or invalid."""


def curated_source_table_path(data_root: Path | str | None = None) -> Path:
    """Return the canonical policy-exception path under ``data/curated``."""
    root = Path("data") if data_root is None else Path(data_root)
    return root / "curated" / "right_to_shelter" / RTS_SOURCE_TABLE_FILENAME


def curated_baseline_path(data_root: Path | str | None = None) -> Path:
    """Return the canonical reviewed-baseline JSON path."""
    root = Path("data") if data_root is None else Path(data_root)
    return root / "curated" / "right_to_shelter" / BASELINE_FILENAME


def load_substate_county_membership(path: Path | str) -> pd.DataFrame:
    """Load explicit additive county-to-substate assignments from canonical CSV."""
    membership_path = Path(path)
    try:
        frame = pd.read_csv(membership_path, dtype={"county_fips": "string"})
    except FileNotFoundError:
        raise
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise RightToShelterError(
            f"Could not read substate county membership at {membership_path}: {exc}. "
            f"Provide CSV columns {SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS}."
        ) from exc
    return validate_substate_county_membership(frame, include_default=False)


def load_policy_baseline(path: Path | str | None = None) -> dict[str, object]:
    """Load a versioned baseline with explicitly enumerated coverage."""
    baseline_path = curated_baseline_path() if path is None else Path(path)
    try:
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Reviewed right-to-shelter baseline not found at {baseline_path}. Create "
            f"{BASELINE_FILENAME} with explicit reviewed_coverage and unknown_gaps."
        ) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise RightToShelterError(
            f"Could not read baseline JSON at {baseline_path}: {exc}."
        ) from exc
    return validate_policy_baseline(baseline)


def _validate_baseline_interval(row: object, label: str) -> dict[str, object]:
    if not isinstance(row, dict):
        raise RightToShelterError(f"Baseline {label} entries must be objects.")
    jurisdiction_id = str(row.get("jurisdiction_id", "")).strip()
    jurisdiction_type = str(row.get("jurisdiction_type", "")).strip()
    if (not jurisdiction_id or row.get("jurisdiction_id") is None or
            jurisdiction_type not in {"state", "sub_state"}):
        raise RightToShelterError(
            f"Baseline {label} entries need jurisdiction_id and jurisdiction_type "
            "'state' or 'sub_state'."
        )
    if jurisdiction_type == "state" and (
        len(jurisdiction_id) != 2 or not jurisdiction_id.isascii() or not jurisdiction_id.isdigit()
    ):
        raise RightToShelterError("State jurisdiction_id must be a two-digit state FIPS code.")
    try:
        raw_first, raw_last = row["first_year"], row["last_year"]
        first_year, last_year = int(raw_first), int(raw_last)
    except (KeyError, TypeError, ValueError) as exc:
        raise RightToShelterError(
            f"Baseline {label} entries need integer first_year/last_year."
        ) from exc
    if (first_year != raw_first or last_year != raw_last or first_year > last_year or
            first_year < 1900 or last_year > 2200):
        raise RightToShelterError(f"Baseline {label} has an invalid year interval.")
    result = dict(row)
    result.update(jurisdiction_id=jurisdiction_id, jurisdiction_type=jurisdiction_type,
                  first_year=first_year, last_year=last_year)
    if label == "reviewed_coverage":
        basis = row.get("basis", "reviewed")
        if basis not in {"reviewed", "provisional_none"}:
            raise RightToShelterError(
                "Reviewed coverage basis must be 'reviewed' or 'provisional_none'."
            )
        result["basis"] = basis
        county_fips = row.get("county_fips")
        if jurisdiction_type == "sub_state":
            if county_fips is not None:
                if (not isinstance(county_fips, list) or not county_fips
                        or not all(isinstance(value, str) and re.fullmatch(r"[0-9]{5}", value)
                                   for value in county_fips)
                        or len(set(county_fips)) != len(county_fips)):
                    raise RightToShelterError(
                        "Substate reviewed_coverage county_fips must be a unique list of "
                        "canonical five-digit FIPS strings."
                    )
                result["county_fips"] = list(county_fips)
        elif county_fips is not None:
            raise RightToShelterError("State reviewed_coverage cannot specify county_fips.")
    if label == "reviewed_coverage":
        evidence = row.get("evidence")
        if not isinstance(evidence, list) or not evidence:
            raise RightToShelterError(
                "Each reviewed_coverage interval needs evidence supporting the review."
            )
        for item in evidence:
            if (not isinstance(item, dict) or not isinstance(item.get("citation"), str)
                    or not item["citation"].strip()):
                raise RightToShelterError("Baseline evidence entries need a nonempty citation.")
            if (item.get("citation_source_type", "secondary") not in SOURCE_TYPES or
                    item.get("citation_redistribution", "unknown") not in REDISTRIBUTION_STATUSES):
                raise RightToShelterError("Baseline evidence needs valid citation metadata.")
    elif not str(row.get("reason", "")).strip():
        raise RightToShelterError("Each unknown_gaps interval needs a reason.")
    if label == "unknown_gaps":
        populations = row.get("populations", list(POPULATIONS))
        if (not isinstance(populations, list) or not populations or
                not set(populations) <= set(POPULATIONS)):
            raise RightToShelterError(
                f"Unknown gap populations must be a nonempty subset of {POPULATIONS}."
            )
        result["populations"] = list(populations)
    return result


def validate_policy_baseline(baseline: dict[str, object]) -> dict[str, object]:
    """Validate explicit, evidence-backed baseline and unknown intervals."""
    if not isinstance(baseline, dict) or baseline.get("schema_version") != "1.0":
        raise RightToShelterError("Baseline schema_version must be '1.0'.")
    if not str(baseline.get("baseline_version", "")).strip():
        raise RightToShelterError("Baseline needs a nonempty baseline_version.")
    if baseline.get("annual_reference_date") != "01-31":
        raise RightToShelterError("Baseline annual_reference_date must be '01-31'.")
    coverage = baseline.get("reviewed_coverage")
    gaps = baseline.get("unknown_gaps", [])
    if not isinstance(coverage, list) or not coverage:
        raise RightToShelterError(
            "Baseline reviewed_coverage must explicitly enumerate jurisdictions; "
            "wildcard or national default coverage is not supported."
        )
    if not isinstance(gaps, list):
        raise RightToShelterError("Baseline unknown_gaps must be a list.")
    normalized_coverage = [
        _validate_baseline_interval(row, "reviewed_coverage") for row in coverage
    ]
    normalized_gaps = [_validate_baseline_interval(row, "unknown_gaps") for row in gaps]
    types_by_id: dict[str, str] = {}
    for row in normalized_coverage:
        previous_type = types_by_id.setdefault(row["jurisdiction_id"], row["jurisdiction_type"])
        if previous_type != row["jurisdiction_type"]:
            raise RightToShelterError("A jurisdiction ID cannot have multiple jurisdiction types.")
    for key, intervals in _baseline_groups(normalized_coverage).items():
        ordered = sorted(intervals, key=lambda row: row["first_year"])
        if any(left["last_year"] >= right["first_year"]
               for left, right in zip(ordered, ordered[1:], strict=False)):
            raise RightToShelterError(f"Baseline reviewed_coverage overlaps for {key}.")
    for gap in normalized_gaps:
        key = (gap["jurisdiction_type"], gap["jurisdiction_id"])
        matching = sorted(_baseline_groups(normalized_coverage).get(key, []),
                          key=lambda row: row["first_year"])
        covered_years = {year for row in matching
                         for year in range(row["first_year"], row["last_year"] + 1)}
        if not set(range(gap["first_year"], gap["last_year"] + 1)) <= covered_years:
            raise RightToShelterError(f"Unknown gap {key} is outside reviewed_coverage.")
    provisional = baseline.get("provisional_baseline")
    normalized_provisional = None
    if provisional is not None:
        if not isinstance(provisional, dict):
            raise RightToShelterError(
                "provisional_baseline must be a valid data-annotation record."
            )
        try:
            annotation = DataAnnotationSpec.model_validate(provisional)
        except ValidationError as exc:
            raise RightToShelterError(
                "Invalid provisional_baseline annotation metadata; correct its DataAnnotationSpec "
                f"fields: {exc}"
            ) from exc
        if annotation.category != "caveat" or not annotation.public_text:
            raise RightToShelterError(
                "provisional_baseline must use category='caveat' and include public_text."
            )
        normalized_provisional = annotation.model_dump(
            mode="json", exclude_none=True,
        )
    provisional_rows = [row for row in normalized_coverage
                        if row.get("basis", "reviewed") == "provisional_none"]
    if provisional_rows and provisional is None:
        raise RightToShelterError(
            "Coverage marked basis='provisional_none' needs top-level provisional_baseline "
            "annotation metadata."
        )
    result = dict(baseline)
    result["reviewed_coverage"] = normalized_coverage
    result["unknown_gaps"] = normalized_gaps
    if normalized_provisional is not None:
        result["provisional_baseline"] = normalized_provisional
    return result


def _baseline_groups(
    rows: list[dict[str, object]],
) -> dict[tuple[str, str], list[dict[str, object]]]:
    groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (str(row["jurisdiction_type"]), str(row["jurisdiction_id"]))
        groups.setdefault(key, []).append(row)
    return groups


def load_policy_exceptions(path: Path | str | None = None) -> pd.DataFrame:
    """Load cited exception intervals from the canonical CSV."""
    source_path = curated_source_table_path() if path is None else Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(
            f"Right-to-shelter exceptions not found at {source_path}. Add cited intervals to "
            f"{EXCEPTIONS_FILENAME} using EXCEPTION_COLUMNS."
        )
    try:
        frame = pd.read_csv(source_path, dtype={"jurisdiction_id": "string"})
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise RightToShelterError(f"Could not read exception CSV at {source_path}: {exc}.") from exc
    return validate_policy_exceptions(frame)


def validate_policy_exceptions(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate dated exception intervals, coding fields, and citations."""
    missing = sorted(set(EXCEPTION_COLUMNS) - set(frame.columns))
    if missing:
        raise RightToShelterError(
            f"Exception CSV is missing columns {missing}; use EXCEPTION_COLUMNS."
        )
    result = frame.loc[:, EXCEPTION_COLUMNS].copy()
    if result.empty:
        return result
    for column in ("exception_id", "jurisdiction_id", "jurisdiction_type", "citation"):
        result[column] = result[column].astype("string").str.strip()
        if result[column].isna().any() or result[column].eq("").any():
            raise RightToShelterError(f"Every exception needs a nonempty {column}.")
    if result["exception_id"].duplicated().any():
        raise RightToShelterError("Exception IDs must be unique across policy intervals.")
    if not result["jurisdiction_type"].isin(["state", "sub_state"]).all():
        raise RightToShelterError("Exception jurisdiction_type must be state or sub_state.")
    state_rows = result["jurisdiction_type"].eq("state")
    if not result.loc[state_rows, "jurisdiction_id"].str.fullmatch(r"[0-9]{2}").all():
        raise RightToShelterError("State exception jurisdiction_id must be two-digit state FIPS.")
    for column in ("effective_date", "ended_date"):
        values = result[column].replace({"": pd.NA})
        try:
            parsed = pd.to_datetime(values, format="%Y-%m-%d", errors="raise")
        except (TypeError, ValueError) as exc:
            raise RightToShelterError(f"{column} values must be ISO dates or blank.") from exc
        result[column] = parsed.dt.strftime("%Y-%m-%d").astype("string")
    starts = pd.to_datetime(result["effective_date"], errors="coerce")
    ends = pd.to_datetime(result["ended_date"], errors="coerce")
    if starts.isna().any() or (ends.notna() & starts.gt(ends)).any():
        raise RightToShelterError(
            "Exception intervals need effective dates and cannot end before start."
        )
    for population in POPULATIONS:
        strength = f"{population}_strength"
        result[strength] = result[strength].astype("string").str.strip()
        if result[strength].isna().any() or not result[strength].isin(STRENGTH_LEVELS).all():
            raise RightToShelterError(f"{strength} must use {STRENGTH_LEVELS}.")
        weather = f"{population}_weather_only"
        result[weather] = _normalize_boolean(result[weather], weather)
    if not result["citation_source_type"].isin(SOURCE_TYPES).all():
        raise RightToShelterError("Invalid exception citation_source_type.")
    if not result["citation_redistribution"].isin(REDISTRIBUTION_STATUSES).all():
        raise RightToShelterError("Invalid exception citation_redistribution.")
    if (result["citation_source_type"].eq("secondary") &
            result["citation_redistribution"].eq("not_applicable")).any():
        raise RightToShelterError("Secondary citations need an explicit redistribution status.")
    for population in POPULATIONS:
        scope_column = f"{population}_qualification_scope"
        summary_column = f"{population}_qualification_summary"
        if not result[scope_column].isin(["all", "subgroup", "unknown"]).all():
            raise RightToShelterError(f"{scope_column} must be all, subgroup, or unknown.")
        limited = result[scope_column].isin(["subgroup", "unknown"])
        caveats = result.loc[limited, summary_column].astype("string").str.strip()
        if caveats.isna().any() or caveats.eq("").any():
            raise RightToShelterError(
                f"Subgroup/unknown {population} exceptions need a {summary_column} caveat."
            )
    for _, group in result.assign(_start=starts, _end=ends).sort_values("_start").groupby(
        ["jurisdiction_type", "jurisdiction_id"]
    ):
        previous_end = None
        for position, (_, row) in enumerate(group.iterrows()):
            if position and pd.isna(previous_end):
                raise RightToShelterError("An open-ended exception overlaps a later interval.")
            if position and row["_start"] <= previous_end:
                raise RightToShelterError(
                    "Conflicting exception intervals overlap for one jurisdiction."
                )
            previous_end = row["_end"]
    return result.reset_index(drop=True)


def expand_policy_intervals(
    baseline: dict[str, object],
    exceptions: pd.DataFrame,
    first_year: int,
    last_year: int,
    *,
    annual_reference_date: str = "01-31",
) -> pd.DataFrame:
    """Generate annual jurisdiction rows at the baseline reference date.

    Reviewed baseline intervals create explicit ``none`` rows. Exceptions are
    applied only when their inclusive date interval contains January 31.
    """
    baseline = validate_policy_baseline(baseline)
    exceptions = validate_policy_exceptions(exceptions)
    coverage_by_key = _baseline_groups(baseline["reviewed_coverage"])
    for _, exception in exceptions.iterrows():
        key = (exception["jurisdiction_type"], exception["jurisdiction_id"])
        intervals = coverage_by_key.get(key, [])
        start = pd.Timestamp(exception["effective_date"])
        end = (pd.Timestamp.max if pd.isna(exception["ended_date"])
               else pd.Timestamp(exception["ended_date"]))
        if not any(
            start <= pd.Timestamp(f"{scope['last_year']}-12-31") and
            end >= pd.Timestamp(f"{scope['first_year']}-01-01") for scope in intervals
        ):
            raise RightToShelterError(
                f"Exception {exception['exception_id']} has no matching reviewed baseline "
                f"jurisdiction/date coverage for {key}. Correct its ID/type or extend the "
                "baseline with reviewed evidence and explicit unknown gaps."
            )
    if (int(first_year) != first_year or int(last_year) != last_year or
            first_year > last_year or first_year < 1900 or last_year > 2200):
        raise RightToShelterError("Requested years must be a valid inclusive range.")
    if annual_reference_date != baseline["annual_reference_date"]:
        raise RightToShelterError("Requested annual reference date must match baseline convention.")
    coverage = baseline["reviewed_coverage"]
    gaps = baseline["unknown_gaps"]
    rows: list[dict[str, object]] = []
    for year in range(first_year, last_year + 1):
        reference = pd.Timestamp(f"{year}-{annual_reference_date}")
        for scope in coverage:
            if not scope["first_year"] <= year <= scope["last_year"]:
                continue
            key = (scope["jurisdiction_type"], scope["jurisdiction_id"])
            evidence = scope["evidence"][0]
            row: dict[str, object] = {
                "jurisdiction_id": key[1],
                "jurisdiction_type": key[0],
                "year": year,
                "baseline_basis": scope.get("basis", "reviewed"),
                "baseline_county_fips": scope.get("county_fips"),
                "citation": evidence["citation"],
                "citation_source_type": evidence.get("citation_source_type", "secondary"),
                "citation_redistribution": evidence.get("citation_redistribution", "unknown"),
                "effective_date": f"{scope['first_year']}-01-01",
                "ended_date": "",
            }
            for population in POPULATIONS:
                row[f"{population}_strength"] = "none"
                row[f"{population}_weather_only"] = False
                row[f"{population}_qualification_scope"] = "all"
                row[f"{population}_qualification_summary"] = ""
            active = exceptions.loc[
                exceptions["jurisdiction_type"].eq(key[0]) &
                exceptions["jurisdiction_id"].eq(key[1]) &
                pd.to_datetime(exceptions["effective_date"]).le(reference) &
                (pd.to_datetime(exceptions["ended_date"]).isna() |
                 pd.to_datetime(exceptions["ended_date"]).ge(reference))
            ]
            if len(active) > 1:
                raise RightToShelterError(f"Overlapping active exceptions for {key} in {year}.")
            if not active.empty:
                exception = active.iloc[0]
                row.update(exception.to_dict())
                row["year"] = year
                row["baseline_basis"] = "reviewed"
            # Baseline uncertainty is authoritative and cannot be overridden by
            # a policy exception record that happens to contain a placeholder.
            for gap in gaps:
                if ((gap["jurisdiction_type"], gap["jurisdiction_id"]) == key and
                        gap["first_year"] <= year <= gap["last_year"]):
                    for population in gap["populations"]:
                        row[f"{population}_qualification_scope"] = "unknown"
                        row[f"{population}_qualification_summary"] = gap["reason"]
            rows.append(row)
    if not rows:
        raise RightToShelterError(
            f"Baseline does not review any jurisdiction during {first_year}-{last_year}. "
            "Extend reviewed_coverage with cited evidence."
        )
    expanded = pd.DataFrame(rows)
    result = validate_jurisdiction_source_table(expanded)
    result.attrs["provisional_baseline"] = baseline.get("provisional_baseline")
    return result


def mapping_output_path(
    geography: Literal["msa", "coc"],
    first_year: int,
    last_year: int,
    vintage: str | int,
    data_root: Path | str | None = None,
    *,
    geography_ids: list[str] | None = None,
) -> Path:
    """Return a deterministic curated output path for one geography family."""
    if geography not in {"msa", "coc"}:
        raise RightToShelterError("geography must be 'msa' or 'coc'.")
    if first_year > last_year:
        raise RightToShelterError("first_year must be less than or equal to last_year.")
    token = f"@M{vintage}" if geography == "msa" else f"@B{vintage}"
    scope_token = ""
    if geography_ids is not None:
        if not geography_ids:
            raise RightToShelterError("Output geography_ids cannot be empty.")
        scope_digest = hashlib.sha256(
            json.dumps(sorted(set(geography_ids))).encode()
        ).hexdigest()[:12]
        scope_token = f"__S{scope_digest}"
    filename = (
        f"right_to_shelter__{geography}__Y{first_year}-{last_year}{token}{scope_token}.parquet"
    )
    root = Path("data") if data_root is None else Path(data_root)
    return root / "curated" / "right_to_shelter" / filename


def load_jurisdiction_source_table(path: Path | str) -> pd.DataFrame:
    """Validate an explicitly supplied generated annual CSV, not maintained inputs."""
    source_path = Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(
            f"Generated annual table not found at {source_path}. Expand reviewed baseline "
            "and exception inputs before importing annual rows."
        )
    try:
        frame = pd.read_csv(source_path, dtype={"jurisdiction_id": "string"})
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise RightToShelterError(
            f"Could not read right-to-shelter CSV at {source_path}: {exc}. "
            "Check that it is a UTF-8 CSV with one header row."
        ) from exc
    return validate_jurisdiction_source_table(frame)


def validate_jurisdiction_source_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate source columns and row evidence without requiring any real rows."""
    missing = sorted(set(JURISDICTION_SOURCE_COLUMNS) - set(frame.columns))
    if missing:
        raise RightToShelterError(
            f"Right-to-shelter table is missing columns {missing}. "
            "Use JURISDICTION_SOURCE_COLUMNS as the header."
        )
    extra_columns = [column for column in (
        "exception_id",
        "baseline_basis",
        "baseline_county_fips",
        *(f"{population}_qualification_scope" for population in POPULATIONS),
        *(f"{population}_qualification_summary" for population in POPULATIONS),
    ) if column in frame.columns]
    result = frame.loc[:, [*JURISDICTION_SOURCE_COLUMNS, *extra_columns]].copy()
    if result.empty:
        return result

    for column in ("jurisdiction_id", "jurisdiction_type", "citation"):
        result[column] = result[column].astype("string").str.strip()
        empty = result[column].isna() | result[column].eq("")
        if empty.any():
            raise RightToShelterError(f"Rows {list(result.index[empty])} need a nonempty {column}.")
    if not result["jurisdiction_type"].isin(["state", "sub_state"]).all():
        raise RightToShelterError("jurisdiction_type must be state or sub_state.")
    state_rows = result["jurisdiction_type"].eq("state")
    if not result.loc[state_rows, "jurisdiction_id"].str.fullmatch(r"[0-9]{2}").all():
        raise RightToShelterError("State jurisdiction_id must be a two-digit state FIPS code.")
    for population in POPULATIONS:
        scope_column = f"{population}_qualification_scope"
        summary_column = f"{population}_qualification_summary"
        if scope_column not in result:
            continue
        if not result[scope_column].isin(["all", "subgroup", "unknown"]).all():
            raise RightToShelterError(f"{scope_column} must be all, subgroup, or unknown.")
        limited = result[scope_column].ne("all")
        if limited.any():
            if summary_column not in result:
                raise RightToShelterError(f"Limited legal scope requires {summary_column}.")
            summaries = result.loc[limited, summary_column].astype("string").str.strip()
            if summaries.isna().any() or summaries.eq("").any():
                raise RightToShelterError(
                    f"Limited legal scope requires nonempty {summary_column}."
                )

    result["citation_source_type"] = (
        result["citation_source_type"].astype("string").str.strip()
    )
    result["citation_redistribution"] = (
        result["citation_redistribution"].astype("string").str.strip()
    )
    invalid_source_types = sorted(
        set(result["citation_source_type"].dropna()) - SOURCE_TYPES
    )
    if result["citation_source_type"].isna().any() or invalid_source_types:
        raise RightToShelterError(
            f"citation_source_type must be one of {sorted(SOURCE_TYPES)}."
        )
    invalid_redistribution = sorted(
        set(result["citation_redistribution"].dropna()) - REDISTRIBUTION_STATUSES
    )
    if result["citation_redistribution"].isna().any() or invalid_redistribution:
        raise RightToShelterError(
            "citation_redistribution must record public_domain, public, permission_required, "
            "unknown, or not_applicable for every cited row."
        )
    secondary_missing_status = result["citation_source_type"].eq("secondary") & (
        result["citation_redistribution"].eq("not_applicable")
    )
    if secondary_missing_status.any():
        raise RightToShelterError(
            "Secondary citations need an explicit redistribution status; "
            "not_applicable is reserved for primary legal sources."
        )

    try:
        years = pd.to_numeric(result["year"], errors="raise")
    except (TypeError, ValueError) as exc:
        raise RightToShelterError("Each jurisdiction-year needs an integer year.") from exc
    if years.isna().any() or (years % 1 != 0).any() or (years < 1900).any() or (years > 2200).any():
        raise RightToShelterError(
            "Each jurisdiction-year needs an integer year from 1900 through 2200."
        )
    result["year"] = years.astype("int64")

    duplicate_key = result.duplicated(["jurisdiction_id", "year"], keep=False)
    if duplicate_key.any():
        keys = result.loc[duplicate_key, ["jurisdiction_id", "year"]].to_dict("records")
        raise RightToShelterError(f"Duplicate jurisdiction-year rows found: {keys}.")

    for population in POPULATIONS:
        strength_column = f"{population}_strength"
        result[strength_column] = result[strength_column].astype("string").str.strip()
        bad_levels = sorted(set(result[strength_column].dropna()) - set(STRENGTH_LEVELS))
        if result[strength_column].isna().any() or bad_levels:
            raise RightToShelterError(
                f"{strength_column} must use {STRENGTH_LEVELS}; invalid values: {bad_levels}."
            )
        weather_column = f"{population}_weather_only"
        result[weather_column] = _normalize_boolean(result[weather_column], weather_column)

    for column in ("effective_date", "ended_date"):
        values = result[column].replace({"": pd.NA})
        try:
            parsed = pd.to_datetime(values, format="%Y-%m-%d", errors="raise")
        except (TypeError, ValueError) as exc:
            raise RightToShelterError(
                f"{column} values must be ISO dates (YYYY-MM-DD) or blank."
            ) from exc
        result[column] = parsed.dt.strftime("%Y-%m-%d").astype("string")

    effective = pd.to_datetime(result["effective_date"], errors="coerce")
    ended = pd.to_datetime(result["ended_date"], errors="coerce")
    reversed_dates = effective.notna() & ended.notna() & (effective > ended)
    if reversed_dates.any():
        raise RightToShelterError(
            f"Rows {list(result.index[reversed_dates])} end before their effective date."
        )
    reference_dates = pd.to_datetime(result["year"].astype(str) + "-01-31")
    future = effective.notna() & effective.gt(reference_dates)
    expired = ended.notna() & ended.lt(reference_dates)
    if future.any() or expired.any():
        invalid = (future | expired)
        raise RightToShelterError(
            "Annual jurisdiction rows must be in force on January 31; invalid rows "
            f"{list(result.index[invalid])}."
        )
    attrs = dict(frame.attrs)
    validated = result.reset_index(drop=True)
    validated.attrs.update(attrs)
    return validated


def _normalize_boolean(values: pd.Series, column: str) -> pd.Series:
    truth_values = {"1", "true", "yes"}
    false_values = {"0", "false", "no"}
    normalized: list[bool] = []
    for index, value in values.items():
        if pd.isna(value):
            raise RightToShelterError(f"{column} cannot be blank (row {index}).")
        normalized_value = str(value).strip().lower()
        if normalized_value in truth_values:
            normalized.append(True)
        elif normalized_value in false_values:
            normalized.append(False)
        else:
            raise RightToShelterError(
                f"{column} must contain true/false or 1/0 values (row {index}: {value!r})."
            )
    return pd.Series(normalized, index=values.index, dtype="bool")


def derive_rts_flags(
    frame: pd.DataFrame,
    *,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
) -> pd.DataFrame:
    """Derive per-population 0/1 flags from validated strength fields.

    ``strict`` means enforceable year-round protection only. ``broad`` means
    enforceable or conditional year-round protection; when
    ``include_weather_only`` is true, the independent weather-only field or
    ``weather_only`` strength also qualifies. This option requires broad
    coding; combining it with strict coding raises ``RightToShelterError``.
    """
    if coding_rule not in CODING_RULES:
        raise RightToShelterError(f"coding_rule must be one of {CODING_RULES}.")
    if coding_rule == "strict" and include_weather_only:
        raise RightToShelterError(
            "include_weather_only requires coding_rule='broad'; use broad coding or disable "
            "the weather-only option."
        )
    result = validate_jurisdiction_source_table(frame)
    for population in POPULATIONS:
        scope_column = f"{population}_qualification_scope"
        if scope_column in result and result[scope_column].eq("unknown").any():
            rows = result.loc[result[scope_column].eq("unknown"), ["jurisdiction_id", "year"]]
            raise RightToShelterError(
                f"Unresolved {population} legal scope for {rows.to_dict('records')[:20]}; "
                "Resolve the population-specific unknown gap before building all-population "
                "outputs."
            )
        strength = result[f"{population}_strength"]
        weather = result[f"{population}_weather_only"]
        if coding_rule == "strict":
            qualifies = strength.eq("enforceable")
        else:
            qualifies = strength.isin(("enforceable", "conditional"))
            if include_weather_only:
                qualifies |= strength.eq("weather_only") | weather
        result[f"rts_{population}"] = qualifies.astype("int8")
    return result


def build_coc_county_membership(
    coc_geometries: pd.DataFrame,
    county_geometries: pd.DataFrame,
    *,
    coc_id_column: str = "coc_id",
    county_id_column: str | None = None,
) -> pd.DataFrame:
    """Overlay CoC/county polygons in equal-area coordinates to get county shares.

    Shares use the existing CoC and TIGER county boundary geometries. Population
    mapping applies each county's population weight by this area share, which is
    an explicit approximation for county fragments.
    """
    import geopandas as gpd

    if county_id_column is None:
        county_id_column = "geoid" if "geoid" in county_geometries else "GEOID"
    if coc_id_column not in coc_geometries or county_id_column not in county_geometries:
        raise RightToShelterError(
            f"Geometry inputs need '{coc_id_column}' and '{county_id_column}' IDs."
        )
    if coc_geometries.crs is None or county_geometries.crs is None:
        raise RightToShelterError("CoC and county geometries need declared coordinate systems.")
    # Canonical HUD/TIGER artifacts use WGS84. Equal-area projection is needed
    # even when the supplied inputs already use another projected CRS.
    coc = coc_geometries[[coc_id_column, "geometry"]].to_crs("EPSG:5070")
    county = county_geometries[[county_id_column, "geometry"]].to_crs("EPSG:5070")
    county["_county_area"] = county.geometry.area
    pieces = gpd.overlay(
        coc, county, how="intersection",
        keep_geom_type=False,
    )
    pieces["_intersection_area"] = pieces.geometry.area
    pieces["allocation_share"] = pieces["_intersection_area"] / pieces["_county_area"]
    result = pieces.rename(
        columns={coc_id_column: "geo_id", county_id_column: "county_fips"}
    )[["geo_id", "county_fips", "allocation_share"]]
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    return result.loc[
        result["allocation_share"] >= COUNTY_OVERLAY_SLIVER_SHARE
    ].reset_index(drop=True)


def map_jurisdiction_years(
    source_table: pd.DataFrame,
    geography_county_membership: pd.DataFrame,
    county_population: pd.DataFrame | None = None,
    *,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
    geography_rule: Literal["majority", "any"] = "majority",
    majority_threshold: float = 0.5,
    substate_county_membership: pd.DataFrame | None = None,
    requested_years: tuple[int, int] | range | None = None,
) -> pd.DataFrame:
    """Map cited jurisdiction-year flags to geography-years and population shares.

    State jurisdiction IDs are two-digit state FIPS prefixes. Explicit sub-state
    membership overrides the state assignment; NYC's five borough counties are
    supplied as the default override. County weights are multiplied by geometry
    allocation shares where membership is fractional.
    """
    if geography_rule not in GEOGRAPHY_RULES:
        raise RightToShelterError(f"geography_rule must be one of {GEOGRAPHY_RULES}.")
    if not 0 <= majority_threshold <= 1:
        raise RightToShelterError("majority_threshold must be between 0 and 1.")
    source = validate_jurisdiction_source_table(source_table)
    membership = _validate_geography_membership(geography_county_membership)
    provisional_annotation = source_table.attrs.get("provisional_baseline")
    resident_weights = county_population is not None and "resident_population" in county_population
    if county_population is None:
        population = None
    else:
        population = _validate_county_population(county_population)
    if requested_years is None:
        years = sorted(source["year"].unique().tolist())
        if not years and population is not None:
            years = sorted(population["year"].unique().tolist())
    elif isinstance(requested_years, range):
        if requested_years.step != 1:
            raise RightToShelterError("Requested year ranges must include every consecutive year.")
        years = list(requested_years)
    else:
        if (len(requested_years) != 2 or any(
            int(year) != year or year < 1900 or year > 2200 for year in requested_years
        )):
            raise RightToShelterError(
                "requested_years must be an inclusive valid (first, last) pair."
            )
        years = list(range(requested_years[0], requested_years[1] + 1))
    if not years or any(not isinstance(year, int) or year < 1900 or year > 2200 for year in years):
        raise RightToShelterError(
            "Requested years must contain valid years from 1900 through 2200."
        )
    explicit_substate_membership = validate_substate_county_membership(
        substate_county_membership, include_default=False
    )
    overrides = validate_substate_county_membership(
        explicit_substate_membership, include_default=True
    )
    if "baseline_county_fips" in source and not explicit_substate_membership.empty:
        _validate_substate_membership_against_source(
            explicit_substate_membership.loc[
                explicit_substate_membership["county_fips"].isin(membership["county_fips"])
            ],
            source, years,
        )

    counties = membership.assign(_join_key=1).merge(
        pd.DataFrame({"year": years, "_join_key": 1}), on="_join_key"
    ).drop(columns="_join_key")
    counties["jurisdiction_id"] = counties["county_fips"].str[:2]
    counties = counties.merge(
        overrides[["county_fips", "jurisdiction_id"]].rename(
            columns={"jurisdiction_id": "substate_jurisdiction_id"}
        ), on="county_fips", how="left", validate="many_to_one",
    )
    counties["jurisdiction_id"] = counties["substate_jurisdiction_id"].fillna(
        counties["jurisdiction_id"]
    )
    if "baseline_county_fips" in source:
        _validate_scoped_substate_assignments(source, membership, overrides, years)
    if population is not None:
        counties = counties.merge(
            population, on=["county_fips", "year"], how="left", validate="many_to_one"
        )

    jurisdiction_keys = counties[["jurisdiction_id", "year"]].drop_duplicates()
    relevant_source = source.merge(
        jurisdiction_keys, on=["jurisdiction_id", "year"], how="inner", validate="one_to_one"
    )
    relevant_source = derive_rts_flags(
        relevant_source, coding_rule=coding_rule, include_weather_only=include_weather_only
    )
    source_extra = [column for population in POPULATIONS for column in (
        f"{population}_qualification_scope", f"{population}_qualification_summary"
    ) if column in relevant_source]
    source_flags = relevant_source[["jurisdiction_id", "year", *JURISDICTION_FLAG_COLUMNS,
                           *source_extra,
                           *(column for column in ("baseline_basis", "baseline_county_fips")
                             if column in relevant_source)]]
    joined = counties.merge(
        source_flags, on=["jurisdiction_id", "year"], how="left", validate="many_to_one",
        indicator=True,
    )
    missing_rows = joined["_merge"].eq("left_only")
    if missing_rows.any():
        missing_keys = joined.loc[missing_rows, ["jurisdiction_id", "year"]].drop_duplicates()
        preview = missing_keys.to_dict("records")
        raise RightToShelterError(
            f"No cited status row exists for mapped jurisdiction-years {preview}. Add those "
            "rows to reviewed coverage or resolve the unknown gap before building; unknown "
            "status is not coded as zero."
        )
    joined = joined.drop(columns="_merge")

    output_rows: list[dict[str, object]] = []
    qualifications: dict[tuple[str, str], set[tuple[str, int]]] = {}
    resident_weight_cells: dict[str, list[list[object]]] = {}
    provisional_cells: dict[str, list[list[object]]] = {}
    for (geo_id, year), group in joined.groupby(["geo_id", "year"], sort=True):
        row: dict[str, object] = {"geo_id": str(geo_id), "year": int(year)}
        uses_provisional_baseline = (
            "baseline_basis" in group
            and group["baseline_basis"].eq("provisional_none").any()
        )
        for population_name, flag_column, weight_column, share_column in zip(
            POPULATIONS, JURISDICTION_FLAG_COLUMNS,
            COUNTY_POPULATION_COLUMNS[2:], POPULATION_SHARE_COLUMNS, strict=True,
        ):
            scope_column = f"{population_name}_qualification_scope"
            if scope_column in group and group[scope_column].eq("subgroup").any():
                summary_column = f"{population_name}_qualification_summary"
                for summary in group.loc[group[scope_column].eq("subgroup"), summary_column]:
                    qualifications.setdefault((population_name, str(summary)), set()).add(
                        (str(geo_id), int(year))
                    )
            values = group[flag_column].astype("int8").unique()
            if len(values) == 1:
                share = float(values[0])
            else:
                if population is None:
                    raise RightToShelterError(
                        f"Geography {geo_id} in {year} has mixed {population_name} status; "
                        "provide complete county-year weights to calculate a covered share."
                    )
                missing = group[weight_column].isna()
                if missing.any():
                    missing_keys = group.loc[missing, ["county_fips", "year"]].to_dict("records")
                    raise RightToShelterError(
                        "County population weights lack required county-year rows "
                        f"{missing_keys[:20]} for mixed {population_name} status; "
                        "missing rows cannot shrink denominators."
                    )
                weighted = group[weight_column] * group["allocation_share"]
                denominator = float(weighted.sum())
                if not math.isfinite(denominator) or denominator <= 0:
                    raise RightToShelterError(
                        f"Geography {geo_id} in {year} has no positive {population_name} weight."
                    )
                covered = float((weighted * group[flag_column]).sum())
                share = covered / denominator
                if resident_weights:
                    for column in (flag_column, share_column):
                        resident_weight_cells.setdefault(column, []).append(
                            [str(geo_id), int(year)]
                        )
            row[share_column] = share
            row[flag_column] = int(share > 0) if geography_rule == "any" else int(
                share >= majority_threshold
            )
            if uses_provisional_baseline:
                for column in (flag_column, share_column):
                    provisional_cells.setdefault(column, []).append([str(geo_id), int(year)])
        output_rows.append(row)

    result = pd.DataFrame(output_rows, columns=MAPPING_COLUMNS)
    result["year"] = result["year"].astype("int64")
    for column in JURISDICTION_FLAG_COLUMNS:
        result[column] = result[column].astype("int8")
    result.attrs["rts_qualifications"] = [
        {"population": population_name, "summary": summary,
         "cells": [[geo_id, year] for geo_id, year in sorted(cells)]}
        for (population_name, summary), cells in sorted(qualifications.items())
    ]
    result.attrs["rts_resident_weight_cells"] = resident_weight_cells
    result.attrs["annotations"] = _geographic_approximation_contract(result)
    if provisional_annotation is not None and provisional_cells:
        _attach_provisional_baseline_annotation(
            result, provisional_annotation, provisional_cells
        )
    return result


def _validate_scoped_substate_assignments(
    source: pd.DataFrame,
    membership: pd.DataFrame,
    overrides: pd.DataFrame,
    years: list[int],
) -> None:
    """Fail if baseline substate scopes are omitted or not explicitly assigned."""
    substate_rows = source.loc[
        source["jurisdiction_type"].eq("sub_state")
        & source["year"].isin(years)
    ]
    if substate_rows.empty:
        return
    selected_counties = set(membership["county_fips"].astype(str))
    for _, row in substate_rows.iterrows():
        scoped = row.get("baseline_county_fips")
        if scoped is None or (isinstance(scoped, float) and math.isnan(scoped)):
            raise RightToShelterError(
                f"Substate jurisdiction {row['jurisdiction_id']} in {row['year']} has no "
                "county_fips scope in reviewed baseline metadata. Add its explicit county "
                "scope before a national geography build."
            )
        relevant = selected_counties.intersection(scoped)
        if not relevant:
            continue
        assigned = set(overrides.loc[
            overrides["jurisdiction_id"].eq(row["jurisdiction_id"]), "county_fips"
        ].astype(str))
        missing = sorted(relevant - assigned)
        if missing:
            raise RightToShelterError(
                f"Substate jurisdiction {row['jurisdiction_id']} intersects selected "
                f"geography but county assignment is missing for {missing}; provide explicit "
                "substate county membership so those counties do not fall back to statewide status."
            )


def _validate_substate_membership_against_source(
    membership: pd.DataFrame,
    source: pd.DataFrame,
    requested_years: list[int],
) -> None:
    """Ensure every supplied county assignment has matching dated source coverage."""
    years = set(requested_years)
    for assignment in membership.to_dict("records"):
        jurisdiction_id = assignment["jurisdiction_id"]
        rows = source.loc[
            source["jurisdiction_type"].eq("sub_state")
            & source["jurisdiction_id"].eq(jurisdiction_id)
            & source["year"].isin(years)
        ]
        covered_years = set(rows["year"].astype(int))
        missing_years = sorted(years - covered_years)
        if missing_years:
            raise RightToShelterError(
                f"Substate membership for {jurisdiction_id} has no reviewed source rows for "
                f"years {missing_years}; add dated source coverage before assigning counties."
            )
        if "baseline_county_fips" in rows:
            for _, source_row in rows.iterrows():
                scope = source_row["baseline_county_fips"]
                if scope is None or (isinstance(scope, float) and math.isnan(scope)):
                    raise RightToShelterError(
                        f"Substate source {jurisdiction_id} has no explicit county_fips scope."
                    )
                if assignment["county_fips"] not in scope:
                    raise RightToShelterError(
                        f"County {assignment['county_fips']} is outside the reviewed county_fips "
                        f"scope for substate jurisdiction {jurisdiction_id}."
                    )


def _attach_provisional_baseline_annotation(
    frame: pd.DataFrame,
    metadata: dict[str, object],
    cells_by_column: dict[str, list[list[object]]],
) -> None:
    """Bind the accepted provisional-baseline caveat to every affected RTS cell."""
    annotation = DataAnnotationSpec(**metadata).model_dump(mode="json", exclude_none=True)
    contract = frame.attrs.get("annotations", serialize_annotation_contract([]))
    contract = dict(contract)
    records = list(contract.get("records", []))
    bindings = dict(contract.get("bindings", {}))
    records.append(annotation)
    identity = f"{annotation['id']}#r{annotation['revision']}"
    bindings[identity] = {"columns": cells_by_column}
    frame.attrs["annotations"] = serialize_annotation_contract(records, bindings=bindings)


def validate_substate_county_membership(
    frame: pd.DataFrame | None,
    *,
    include_default: bool = True,
) -> pd.DataFrame:
    """Validate additive county-to-substate assignments and retain NYC by default."""
    if frame is None:
        supplied = pd.DataFrame(columns=SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS)
    else:
        missing = set(SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS) - set(frame.columns)
        if missing:
            raise RightToShelterError(
                "substate_county_membership needs jurisdiction_id, jurisdiction_type, "
                "and county_fips columns."
            )
        supplied = frame.loc[:, SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS].copy()
    if not supplied.empty:
        for column in SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS:
            supplied[column] = supplied[column].astype("string").str.strip()
            if supplied[column].isna().any() or supplied[column].eq("").any():
                raise RightToShelterError(
                    f"Substate county membership requires nonempty {column} values."
                )
        supplied["county_fips"] = supplied["county_fips"].astype("string")
        if not supplied["county_fips"].str.fullmatch(r"[0-9]{5}").all():
            raise RightToShelterError(
                "Substate county_fips values must be canonical five-digit FIPS strings; "
                "do not omit leading zeroes or include punctuation."
            )
        if not supplied["jurisdiction_type"].eq("sub_state").all():
            raise RightToShelterError(
                "Explicit county membership jurisdiction_type must be 'sub_state'."
            )
        if supplied["county_fips"].duplicated().any():
            duplicated = sorted(supplied.loc[
                supplied["county_fips"].duplicated(keep=False), "county_fips"
            ].unique())
            raise RightToShelterError(
                "A county may map to only one sub-state jurisdiction; duplicate FIPS: "
                f"{duplicated}."
            )
    defaults = (DEFAULT_SUBSTATE_COUNTY_MEMBERSHIP.copy() if include_default
                else pd.DataFrame(columns=SUBSTATE_COUNTY_MEMBERSHIP_COLUMNS))
    if supplied.empty:
        return defaults.reset_index(drop=True)
    default_by_county = defaults.set_index("county_fips")["jurisdiction_id"].to_dict()
    conflicts = [
        row for row in supplied.to_dict("records")
        if row["county_fips"] in default_by_county
        and row["jurisdiction_id"] != default_by_county[row["county_fips"]]
    ]
    if conflicts:
        raise RightToShelterError(
            "Explicit substate membership cannot reassign default NYC counties; "
            f"conflicting rows: {conflicts}."
        )
    additional = supplied.loc[~supplied["county_fips"].isin(defaults["county_fips"])]
    if additional.empty:
        return defaults.reset_index(drop=True)
    if defaults.empty:
        return additional.reset_index(drop=True)
    return pd.concat([defaults, additional], ignore_index=True).reset_index(drop=True)


def map_msa_years(
    source_table: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
    county_population: pd.DataFrame | None = None,
    **options,
) -> pd.DataFrame:
    """Map using Census MSA county membership rows from ``msa`` geography code."""
    if not {"msa_id", "county_fips"} <= set(msa_county_membership.columns):
        raise RightToShelterError("MSA membership needs msa_id and county_fips columns.")
    membership = msa_county_membership[["msa_id", "county_fips"]].rename(
        columns={"msa_id": "geo_id"}
    )
    membership["allocation_share"] = 1.0
    return map_jurisdiction_years(
        source_table, membership, county_population, **options
    )


def map_coc_years(
    source_table: pd.DataFrame,
    coc_geometries: pd.DataFrame,
    county_geometries: pd.DataFrame,
    county_population: pd.DataFrame | None = None,
    **options,
) -> pd.DataFrame:
    """Map using CoC/county geometry intersections from ``coc`` geography code."""
    membership = build_coc_county_membership(coc_geometries, county_geometries)
    missing = sorted(
        set(coc_geometries["coc_id"].astype(str)) - set(membership["geo_id"].astype(str))
    )
    if missing:
        raise RightToShelterError(
            f"CoCs {missing} have no county intersection; check geometry coverage and vintages."
        )
    return map_jurisdiction_years(
        source_table, membership, county_population, **options
    )


def _validate_geography_membership(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"geo_id", "county_fips"}
    if required - set(frame.columns):
        raise RightToShelterError(
            "Geography membership needs geo_id and county_fips columns."
        )
    result = frame.copy()
    if result.empty:
        raise RightToShelterError(
            "Geography membership is empty; select at least one supported geography."
        )
    result["geo_id"] = result["geo_id"].astype("string").str.strip()
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    if result["geo_id"].isna().any() or result["geo_id"].eq("").any():
        raise RightToShelterError("Geography membership contains a blank geo_id.")
    if "allocation_share" not in result:
        result["allocation_share"] = 1.0
    result["allocation_share"] = pd.to_numeric(result["allocation_share"], errors="raise")
    if result["allocation_share"].isna().any() or (
        (result["allocation_share"] <= 0) | (result["allocation_share"] > 1)
    ).any():
            raise RightToShelterError(
                "Membership allocation_share must be greater than 0 and at most 1."
            )
    if result.duplicated(["geo_id", "county_fips"]).any():
        raise RightToShelterError("Geography membership repeats a geo_id/county_fips pair.")
    return result[["geo_id", "county_fips", "allocation_share"]]


def _validate_county_population(frame: pd.DataFrame) -> pd.DataFrame:
    if "resident_population" in frame:
        if any(column in frame for column in COUNTY_POPULATION_COLUMNS[2:]):
            raise RightToShelterError(
                "Supply resident_population or population-specific weights, not both."
            )
        frame = frame.assign(**{
            column: frame["resident_population"] for column in COUNTY_POPULATION_COLUMNS[2:]
        })
    missing = sorted(set(COUNTY_POPULATION_COLUMNS) - set(frame.columns))
    if missing:
        raise RightToShelterError(
            f"County population weights are missing columns {missing}. Required columns are "
            f"{COUNTY_POPULATION_COLUMNS}."
        )
    result = frame.loc[:, COUNTY_POPULATION_COLUMNS].copy()
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    numeric_year = pd.to_numeric(result["year"], errors="raise")
    if numeric_year.isna().any() or (numeric_year % 1 != 0).any() or (
        (numeric_year < 1900) | (numeric_year > 2200)
    ).any():
        raise RightToShelterError(
            "County population years must be integers from 1900 through 2200."
        )
    result["year"] = numeric_year.astype("int64")
    for column in COUNTY_POPULATION_COLUMNS[2:]:
        result[column] = pd.to_numeric(result[column], errors="raise")
        if result[column].isna().any() or not result[column].map(math.isfinite).all() or (
            result[column] < 0
        ).any():
            raise RightToShelterError(f"{column} must contain finite, nonnegative numeric weights.")
    if result.duplicated(["county_fips", "year"]).any():
        raise RightToShelterError("County population weights repeat a county-year.")
    return result


def _geographic_approximation_contract(frame: pd.DataFrame) -> dict:
    partial_cells: dict[str, list[list[object]]] = {}
    for flag_column, share_column in zip(
        JURISDICTION_FLAG_COLUMNS, POPULATION_SHARE_COLUMNS, strict=True
    ):
        partial = frame[share_column].between(0, 1, inclusive="neither")
        cells = [
            [str(geo_id), int(year)]
            for geo_id, year in frame.loc[partial, ["geo_id", "year"]].itertuples(
                index=False, name=None
            )
        ]
        partial_cells[flag_column] = cells
        partial_cells[share_column] = cells
    records: list[dict] = []
    bindings: dict[str, dict] = {}
    if any(partial_cells.values()):
        annotation = DataAnnotationSpec(
            id=RTS_ANNOTATION_ID,
            revision=2,
            category="geographic_approximation",
            summary="Right-to-shelter status is assigned to mixed jurisdictions approximately",
            text=(
                "Right-to-shelter protections are defined by state or sub-state law. "
                "MSA and CoC values are assigned using county population weights; the "
                "listed geography-years are only partially covered by the coded status."
            ),
            public_text=(
                "Right-to-shelter protections are defined by state or sub-state law and "
                "assigned to MSAs and CoCs using county population weights. This "
                "geography-year has partial population coverage."
            ),
            treatment="Use the population-specific covered share alongside each rts flag.",
            uncertainty=(
                "County-level population weighting approximates sub-county legal boundaries."
            ),
            evidence=[{
                "name": "Results recipe right-to-shelter coding rule",
                "ref": "docs/results-recipe-spec_2.md",
            }],
            scope={
                "columns": [*JURISDICTION_FLAG_COLUMNS, *POPULATION_SHARE_COLUMNS],
            },
        ).model_dump(mode="json", exclude_none=True)
        records.append(annotation)
        identity = f"{RTS_ANNOTATION_ID}#r2"
        bindings[identity] = {"columns": {
            column: cells for column, cells in partial_cells.items() if cells
        }}

    for qualification in frame.attrs.get("rts_qualifications", []):
        summary = qualification["summary"]
        population_name = qualification["population"]
        columns = [f"rts_{population_name}", f"rts_{population_name}_population_share"]
        digest = hashlib.sha256(f"{population_name}\n{summary}".encode()).hexdigest()[:16]
        annotation_id = f"{RTS_QUALIFICATION_ANNOTATION_ID}-{digest}"
        annotation = DataAnnotationSpec(
            id=annotation_id,
            category="caveat",
            summary="Legal protection has a documented subgroup qualification",
            text=(
                f"The cited legal policy is qualified by subgroup: {summary} "
                "The covered share measures geography with a coded policy; it does not "
                "estimate the share of people eligible under that subgroup rule."
            ),
            public_text=(
                f"{summary} Geographic covered shares do not measure the share of eligible people."
            ),
            treatment="Interpret the right-to-shelter flag as a policy-presence summary only.",
            uncertainty="Subgroup eligibility prevalence is not estimated.",
            scope={"columns": columns},
        ).model_dump(mode="json", exclude_none=True)
        records.append(annotation)
        bindings[f"{annotation_id}#r1"] = {
            "columns": {column: qualification["cells"] for column in columns},
        }
    resident_cells = frame.attrs.get("rts_resident_weight_cells", {})
    if resident_cells:
        annotation_id = "right-to-shelter-resident-population-weights"
        annotation = DataAnnotationSpec(
            id=annotation_id,
            category="caveat",
            summary="Coverage uses common resident-population geography weights",
            text=(
                "The same resident-population weights are used for the separate legal-policy "
                "populations. Shares describe residence within jurisdictions with the coded "
                "policy, not the proportion legally eligible for shelter."
            ),
            public_text=(
                "Coverage shares use resident-population geography weights and do not "
                "measure legal eligibility."
            ),
            treatment="Keep the legal-policy population separate from its geographic weight basis.",
            scope={"columns": [*JURISDICTION_FLAG_COLUMNS, *POPULATION_SHARE_COLUMNS]},
        ).model_dump(mode="json", exclude_none=True)
        records.append(annotation)
        bindings[f"{annotation_id}#r1"] = {"columns": resident_cells}
    if not records:
        return serialize_annotation_contract([])
    return serialize_annotation_contract(records, bindings=bindings)


def write_mapping_with_provenance(
    frame: pd.DataFrame,
    path: Path | str,
    *,
    geography: Literal["msa", "coc"],
    coding_rule: Literal["strict", "broad"],
    include_weather_only: bool,
    geography_rule: Literal["majority", "any"],
    boundary_vintage: str | int,
    membership_vintage: str,
    extra_provenance: dict[str, object] | None = None,
) -> Path:
    """Write one mapping table with coding lineage and exact annotation cells."""
    annotations = frame.attrs.get("annotations", serialize_annotation_contract([]))
    provenance = ProvenanceBlock(
        geo_type=geography,
        boundary_vintage=str(boundary_vintage) if geography == "coc" else None,
        definition_version=membership_vintage if geography == "msa" else None,
        extra={
            "dataset_type": "right_to_shelter_geography_year",
            "coding_rule": coding_rule,
            "include_weather_only": bool(include_weather_only),
            "geography_rule": geography_rule,
            "membership_vintage": str(membership_vintage),
            "annotations": annotations,
            **(extra_provenance or {}),
        },
    )
    return write_parquet_with_provenance(frame, path, provenance)


def build_curated_outputs(
    *,
    first_year: int,
    last_year: int,
    county_population_path: Path | str | None = None,
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    coc_boundary_vintage: str,
    county_vintage: str,
    exceptions_path: Path | str | None = None,
    baseline_path: Path | str | None = None,
    data_root: Path | str | None = None,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
    geography_rule: Literal["majority", "any"] = "majority",
    majority_threshold: float = 0.5,
    msa_ids: list[str] | None = None,
    coc_ids: list[str] | None = None,
    weight_definition: str | None = None,
    substate_county_membership_path: Path | str | None = None,
) -> dict[str, Path]:
    """Build requested-year outputs from reviewed baseline and exception inputs."""
    from hhplab.artifacts.naming.naming import county_path, msa_county_membership_path
    from hhplab.geographies.coc.coc_io import read_geoparquet, resolve_curated_boundary_path
    from hhplab.geographies.msa.msa_io import read_msa_county_membership

    if msa_ids == [] or coc_ids == []:
        raise RightToShelterError("Geography selections cannot be empty.")
    if first_year > last_year:
        raise RightToShelterError("first_year must be less than or equal to last_year.")
    source = expand_policy_intervals(
        load_policy_baseline(
            curated_baseline_path(data_root) if baseline_path is None else baseline_path
        ),
        load_policy_exceptions(
            curated_source_table_path(data_root) if exceptions_path is None else exceptions_path
        ),
        first_year, last_year,
    )
    substate_membership = (
        None if substate_county_membership_path is None
        else load_substate_county_membership(substate_county_membership_path)
    )
    county_population = None
    if county_population_path is not None:
        if not weight_definition or not weight_definition.strip():
            raise RightToShelterError(
                "When county weights are supplied, set --weight-definition to describe their "
                "population basis and vintage; covered shares are geographic policy shares, "
                "not eligible-person prevalence."
            )
        try:
            weight_path = Path(county_population_path)
            if weight_path.suffix == ".parquet":
                county_population = pd.read_parquet(weight_path)
            else:
                county_population = pd.read_csv(weight_path, dtype={"county_fips": "string"})
        except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
            raise RightToShelterError(
                f"Could not read county population weights at {county_population_path}: {exc}. "
                "Provide CSV or Parquet with county_fips/year and resident_population "
                f"or the population-specific columns {COUNTY_POPULATION_COLUMNS[2:]}."
            ) from exc

    msa_membership = read_msa_county_membership(msa_definition_version, data_root)
    if msa_ids is not None:
        available = set(msa_membership["msa_id"].astype(str))
        missing_ids = sorted(set(msa_ids) - available)
        if missing_ids:
            raise RightToShelterError(
                f"Requested MSA IDs are not in membership vintage: {missing_ids}."
            )
        msa_membership = msa_membership.loc[msa_membership["msa_id"].astype(str).isin(msa_ids)]
    msa = map_msa_years(
        source, msa_membership, county_population,
        coding_rule=coding_rule, include_weather_only=include_weather_only,
        geography_rule=geography_rule, majority_threshold=majority_threshold,
        substate_county_membership=substate_membership,
        requested_years=(first_year, last_year),
    )
    coc_path = resolve_curated_boundary_path(coc_boundary_vintage, data_root)
    county_geometry_path = county_path(county_vintage, data_root)
    coc_geometry = read_geoparquet(coc_path)
    if coc_ids is not None:
        available = set(coc_geometry["coc_id"].astype(str))
        missing_ids = sorted(set(coc_ids) - available)
        if missing_ids:
            raise RightToShelterError(
                f"Requested CoC IDs are not in boundary vintage: {missing_ids}."
            )
        coc_geometry = coc_geometry.loc[coc_geometry["coc_id"].astype(str).isin(coc_ids)]
    county_geometry = read_geoparquet(county_geometry_path)
    coc = map_coc_years(
        source, coc_geometry, county_geometry, county_population,
        coding_rule=coding_rule, include_weather_only=include_weather_only,
        geography_rule=geography_rule, majority_threshold=majority_threshold,
        substate_county_membership=substate_membership,
        requested_years=(first_year, last_year),
    )

    msa_output = mapping_output_path(
        "msa", first_year, last_year, msa_definition_version, data_root, geography_ids=msa_ids,
    )
    coc_output = mapping_output_path(
        "coc", first_year, last_year, coc_boundary_vintage, data_root, geography_ids=coc_ids,
    )
    existing_outputs = [str(path) for path in (msa_output, coc_output) if path.exists()]
    if existing_outputs:
        raise RightToShelterError(
            f"Output artifacts already exist: {existing_outputs}. Choose a separate data root "
            "or preserve and explicitly relocate prior outputs before rebuilding."
        )
    baseline_file = (
        curated_baseline_path(data_root) if baseline_path is None else Path(baseline_path)
    )
    exceptions_file = (
        curated_source_table_path(data_root) if exceptions_path is None else Path(exceptions_path)
    )
    input_ids = {
        "rts_baseline_version": load_policy_baseline(baseline_file)["baseline_version"],
        "rts_baseline_sha256": hashlib.sha256(baseline_file.read_bytes()).hexdigest(),
        "rts_exceptions_sha256": hashlib.sha256(exceptions_file.read_bytes()).hexdigest(),
        "rts_annual_reference_date": "01-31",
        "rts_requested_years": [first_year, last_year],
        "rts_population_qualification_fields": (
            "scoped per population; shares are geography coverage only"
        ),
        "weight_definition": weight_definition,
        "majority_threshold": majority_threshold,
        "msa_ids": sorted(msa_ids) if msa_ids is not None else None,
        "coc_ids": sorted(coc_ids) if coc_ids is not None else None,
        "interval_endpoints": "inclusive",
        "county_overlay_crs": "EPSG:5070",
        "county_overlay_minimum_share": COUNTY_OVERLAY_SLIVER_SHARE,
        "geography_inputs": {},
        "substate_county_membership": (
            validate_substate_county_membership(substate_membership).to_dict("records")
        ),
        "substate_county_membership_mode": "additive_with_default_nyc",
    }
    for name, path in (
        ("msa_county_membership", msa_county_membership_path(msa_definition_version, data_root)),
        ("coc_geometry", coc_path), ("county_geometry", county_geometry_path),
    ):
        with Path(path).open("rb") as input_file:
            input_ids["geography_inputs"][name] = {
                "path": str(path), "sha256": hashlib.file_digest(input_file, "sha256").hexdigest(),
            }
    if county_population_path is not None:
        weight_file = Path(county_population_path)
        input_ids["weight_input_path"] = str(weight_file)
        input_ids["weight_input_sha256"] = hashlib.sha256(weight_file.read_bytes()).hexdigest()
    if substate_county_membership_path is not None:
        membership_file = Path(substate_county_membership_path)
        input_ids["substate_county_membership_input_path"] = str(membership_file)
        input_ids["substate_county_membership_input_sha256"] = hashlib.sha256(
            membership_file.read_bytes()
        ).hexdigest()
    write_mapping_with_provenance(
        msa, msa_output, geography="msa", coding_rule=coding_rule,
        include_weather_only=include_weather_only, geography_rule=geography_rule,
        boundary_vintage=msa_definition_version, membership_vintage=msa_definition_version,
        extra_provenance=input_ids,
    )
    write_mapping_with_provenance(
        coc, coc_output, geography="coc", coding_rule=coding_rule,
        include_weather_only=include_weather_only, geography_rule=geography_rule,
        boundary_vintage=coc_boundary_vintage, membership_vintage=county_vintage,
        extra_provenance=input_ids,
    )
    return {"msa": msa_output, "coc": coc_output}
