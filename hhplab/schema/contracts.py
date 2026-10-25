"""Artifact-level schema contracts and validation helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

from hhplab.schema.columns import (
    ACS_TRACT_OUTPUT_COLUMNS,
    BLOCK_GEOMETRY_COLUMNS,
    COC_PANEL_COLUMNS,
    COC_URBAN_AREA_DETAIL_COLUMNS,
    COC_URBAN_FRACTION_COLUMNS,
    DRIFT_PRONE_SOURCE_COLUMNS,
    LAUS_METRO_OUTPUT_COLUMNS,
    MSA_COC_COVERAGE_COLUMNS,
    MSA_COC_PANEL_COLUMNS,
    MSA_FRACTIONAL_ROLLUP_COLUMNS,
    MSA_ZORI_YEARLY_COLUMNS,
    PEP_COUNTY_OUTPUT_COLUMNS,
    PIT_CANONICAL_COLUMNS,
    PL_BLOCK_POPULATION_COLUMNS,
    SAE_MEASURE_COLUMNS,
    SAE_OUTPUT_COLUMNS,
    TRACT_MEDIATED_COUNTY_XWALK_COLUMNS,
    URBAN_AREA_GEOMETRY_COLUMNS,
    ZORI_INGEST_OUTPUT_COLUMNS,
)
from hhplab.schema.lineage import population_lineage_columns
from hhplab.schema.measures import ACS1_IMPUTATION_OUTPUT_COLUMNS


class AnnotationGeographies(BaseModel):
    model_config = ConfigDict(extra="forbid")
    include: list[StrictStr] | None = None


class AnnotationScope(BaseModel):
    """Original selectors; persisted bindings describe transformed applicability."""

    model_config = ConfigDict(extra="forbid")
    panels: list[StrictStr] = Field(default_factory=list)
    columns: list[StrictStr] = Field(default_factory=list)
    years: list[StrictInt] = Field(default_factory=list)
    geographies: AnnotationGeographies | None = None


class AnnotationEvidence(BaseModel):
    """Portable evidence locator. A doc reference is a locator, not a bundled copy."""

    model_config = ConfigDict(extra="forbid")
    name: StrictStr = Field(min_length=1)
    ref: StrictStr = Field(min_length=1)
    kind: Literal["doc", "url"] = "doc"
    sha256: StrictStr | None = None

    @model_validator(mode="after")
    def validate_evidence(self) -> AnnotationEvidence:
        if any(ord(char) < 32 for char in self.ref):
            raise ValueError("annotation evidence ref must not contain control characters")
        parsed = urlsplit(self.ref)
        if self.kind == "url":
            if parsed.scheme not in {"https", "http"} or not parsed.hostname:
                raise ValueError("annotation evidence URL must be an absolute http(s) URL")
            if parsed.username or parsed.password:
                raise ValueError("annotation evidence URL must not contain credentials")
        elif (
            parsed.scheme or parsed.netloc or self.ref.startswith("/")
            or "\\" in self.ref or ".." in self.ref.split("/")
        ):
            raise ValueError("annotation evidence doc ref must be a portable relative POSIX path")
        if self.sha256 is not None and re.fullmatch(r"[0-9a-fA-F]{64}", self.sha256) is None:
            raise ValueError("annotation evidence sha256 must be exactly 64 hex characters")
        return self


class DataAnnotationSpec(BaseModel):
    """Immutable, versioned data caveat, separate from its effective cell bindings.

    ``public_text`` is an explicit author assertion that this explanation may
    be redistributed even when its source data cannot be. All other content
    and applicability remain governed by the source's distribution policy.
    """

    model_config = ConfigDict(extra="forbid")
    id: StrictStr = Field(min_length=1)
    revision: StrictInt = Field(default=1, ge=1)
    category: Literal[
        "adjustment", "caveat", "geographic_approximation", "source_substitution",
        "measurement_change", "exclusion", "other",
    ] = "caveat"
    summary: StrictStr = Field(min_length=1)
    text: StrictStr = Field(min_length=1)
    public_text: StrictStr | None = Field(default=None, min_length=1)
    treatment: StrictStr | None = None
    uncertainty: StrictStr | None = None
    evidence: list[AnnotationEvidence] = Field(default_factory=list)
    scope: AnnotationScope = Field(default_factory=AnnotationScope)
    applies_to_masks: list[StrictStr] = Field(default_factory=list)
    supersedes: StrictInt | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_revision(self) -> DataAnnotationSpec:
        if self.supersedes is not None and self.supersedes >= self.revision:
            raise ValueError(
                "annotation supersedes must refer to an earlier revision of the same id"
            )
        return self


class AnnotationSource(BaseModel):
    """Content identity and original vintages of a contributing artifact."""

    model_config = ConfigDict(extra="forbid")
    sha256: StrictStr
    vintages: dict[StrictStr, StrictStr] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_digest(self) -> AnnotationSource:
        if re.fullmatch(r"[0-9a-fA-F]{64}", self.sha256) is None:
            raise ValueError("annotation source sha256 must be exactly 64 hex characters")
        return self


def annotation_source_identity(sha256: str, provenance: Mapping[str, Any]) -> dict[str, Any]:
    """Capture original geography/vintage identity without private filesystem locators."""
    names = {"boundary_vintage", "tract_vintage", "county_vintage", "acs_vintage",
             "geo_type", "definition_version", "weighting", "notation"}
    vintages = {name: str(value) for name, value in provenance.items()
                if name in names and value is not None}
    extra = provenance.get("extra", {})
    if not isinstance(extra, Mapping):
        raise ValueError("annotation source provenance.extra must be a mapping")
    geometry = provenance.get("target_geometry", extra.get("target_geometry", {}))
    if isinstance(geometry, Mapping):
        vintages.update({f"target_geometry.{name}": str(value) for name, value in geometry.items()
                         if name in {"type", "vintage", "source"} and value is not None})
    return AnnotationSource(sha256=sha256, vintages=vintages).model_dump(mode="json")


def validate_annotation_revisions(
    records: Sequence[Mapping[str, Any]], *, require_exists: bool = True,
) -> None:
    identities = {(record["id"], record.get("revision", 1)) for record in records}
    for record in records:
        spec = DataAnnotationSpec.model_validate(record)
        if require_exists and spec.supersedes is not None:
            if (spec.id, spec.supersedes) not in identities:
                raise ValueError(
                    "annotation supersedes a missing revision; retain predecessor records"
                )


ANNOTATION_CONTRACT_SCHEMA_VERSION = 1
PUBLIC_SAMPLE_ANNOTATION_NOTICE = "details available in policy-filtered annotations.json"


def annotation_identity_key(annotation_id: Any, revision: Any) -> str:
    if not isinstance(annotation_id, str) or not annotation_id:
        raise ValueError("annotation id must be a nonempty string")
    if type(revision) is not int or revision < 1:
        raise ValueError("annotation revision must be a positive integer")
    return f"{annotation_id}#r{revision}"


def parse_annotation_contract(value: Any) -> dict[str, Any]:
    """Validate the shared artifact contract, including exact column/cell bindings.

    A bare list is an unbound source declaration. Version 1 binds every record
    explicitly; an empty binding means no affected output cells. Bindings use
    ``{identity: {columns: {column: [[geo, year], ...]}}}``, never a Cartesian
    product of independent column and row selectors.
    """
    legacy = isinstance(value, list)
    if legacy:
        records, bindings, uncertain, sources = value, {}, [], {}
    else:
        if not isinstance(value, dict):
            raise ValueError("annotation container must be a list or versioned contract mapping")
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            raise ValueError("unsupported annotation schema_version; expected integer 1")
        if set(value) - {"schema_version", "records", "bindings", "uncertain_ids", "sources"}:
            raise ValueError("annotation contract contains unsupported fields")
        records = value.get("records")
        bindings = value.get("bindings", {})
        uncertain = value.get("uncertain_ids", [])
        sources = value.get("sources", {})
    if not isinstance(records, list) or not isinstance(bindings, dict):
        raise ValueError("annotation records must be a list and bindings must be a mapping")
    if not isinstance(uncertain, list) or any(not isinstance(key, str) for key in uncertain):
        raise ValueError("annotation uncertain_ids must be a list of identity strings")
    normalized: dict[str, dict[str, Any]] = {}
    for record in records:
        spec = DataAnnotationSpec.model_validate(record)
        canonical = spec.model_dump(mode="json", exclude_none=True)
        key = annotation_identity_key(spec.id, spec.revision)
        if key in normalized and normalized[key] != canonical:
            raise ValueError(f"conflicting annotation content for {key!r}; create a new revision")
        normalized[key] = canonical
    if not isinstance(sources, dict):
        raise ValueError("annotation sources must be a mapping keyed by annotation identity")
    if (set(bindings) | set(uncertain) | set(sources)) - set(normalized):
        raise ValueError("annotation bindings/uncertain_ids reference unknown identities")
    normalized_sources = {}
    for identity, entries in sources.items():
        if not isinstance(entries, list):
            raise ValueError("annotation source identities must be a list")
        normalized_sources[identity] = [
            AnnotationSource.model_validate(entry).model_dump(mode="json") for entry in entries
        ]
    for key, binding in bindings.items():
        if not isinstance(binding, dict) or set(binding) != {"columns"}:
            raise ValueError(f"annotation binding {key!r} must contain only 'columns'")
        columns = binding["columns"]
        if not isinstance(columns, dict):
            raise ValueError(
                "binding columns must map column names to exact cells; regenerate artifact"
            )
        for column, cells in columns.items():
            if not isinstance(column, str) or not column or not isinstance(cells, list):
                raise ValueError(
                    "annotation column bindings require column names and lists of cells"
                )
            for cell in cells:
                if (
                    not isinstance(cell, list) or len(cell) != 2
                    or not isinstance(cell[0], (str, int)) or isinstance(cell[0], bool)
                    or type(cell[1]) is not int
                ):
                    raise ValueError(
                        "annotation cell must be [geography string/integer, integer year]"
                    )
    if not legacy and set(normalized) - set(bindings) - set(uncertain):
        raise ValueError(
            "every persisted annotation needs an explicit binding or uncertain identity"
        )
    return {
        "schema_version": 0 if legacy else 1,
        "records": [normalized[key] for key in sorted(normalized)],
        "bindings": bindings,
        "uncertain_ids": sorted(set(uncertain)),
        "sources": normalized_sources,
    }


def serialize_annotation_contract(
    records: Sequence[Mapping[str, Any]], *, bindings: Mapping[str, Any] | None = None,
    uncertain_ids: Sequence[str] = (),
    sources: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    explicit = {
        annotation_identity_key(record["id"], record.get("revision", 1)): {"columns": {}}
        for record in records
    }
    explicit.update(dict(bindings) if bindings is not None else {})
    return parse_annotation_contract({
        "schema_version": 1, "records": list(records), "bindings": explicit,
        "uncertain_ids": list(uncertain_ids),
        "sources": dict(sources) if sources is not None else {},
    })


def frame_annotation_contract(frame: pd.DataFrame) -> dict[str, Any] | None:
    """Read consistent annotation containers from a frame; malformed presence is an error."""
    containers = []
    if "annotations" in frame.attrs:
        containers.append(frame.attrs["annotations"])
    provenance = frame.attrs.get("results_panel_provenance", {})
    if not isinstance(provenance, Mapping):
        raise ValueError("results_panel_provenance must be a mapping")
    if isinstance(provenance, Mapping):
        if "annotations" in provenance:
            containers.append(provenance["annotations"])
        extra = provenance.get("extra", {})
        if not isinstance(extra, Mapping):
            raise ValueError("results_panel_provenance.extra must be a mapping")
        if extra.get("annotation_status") == PUBLIC_SAMPLE_ANNOTATION_NOTICE:
            raise ValueError(
                "Public sample artifact omits detailed annotation lineage; retrieve the "
                "original full panel/private package before reusing it as an analysis input"
            )
        if isinstance(extra, Mapping) and "annotations" in extra:
            containers.append(extra["annotations"])
    try:
        parsed = [parse_annotation_contract(container) for container in containers]
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "invalid annotation metadata; validate records and bindings with "
            "hhplab.schema.contracts before reusing this source artifact"
        ) from exc
    if any(contract != parsed[0] for contract in parsed[1:]):
        raise ValueError("inconsistent annotation containers; consolidate artifact metadata")
    return parsed[0] if parsed else None


def project_annotation_contract(
    contract: Mapping[str, Any], columns: Sequence[str],
) -> dict[str, Any]:
    """Project exact applicability without changing any immutable record definition."""
    bindings = {identity: {"columns": {
            column: cells for column, cells in binding["columns"].items() if column in columns
        }} for identity, binding in contract["bindings"].items()}
    uncertain = [identity for identity in contract["uncertain_ids"]
                 if not contract["bindings"].get(identity, {}).get("columns")
                 or bindings.get(identity, {}).get("columns")]
    return serialize_annotation_contract(
        contract["records"], bindings=bindings, uncertain_ids=uncertain,
        sources=contract.get("sources", {}),
    )


def annotations_for_source_cell(contract, geography, year, column):
    """Resolve a consumed cell to annotation identities and scope uncertainty."""
    if contract is None:
        return []
    found = []
    for record in contract["records"]:
        identity = annotation_identity_key(record["id"], record["revision"])
        uncertain = identity in contract["uncertain_ids"]
        if contract["schema_version"] == 1:
            columns = contract["bindings"].get(identity, {"columns": {}})["columns"]
            if [geography, year] in columns.get(column, []):
                found.append((identity, uncertain))
            elif uncertain and not columns:
                found.append((identity, True))
            continue
        scope = record["scope"]
        if scope["columns"] and column not in scope["columns"]:
            continue
        if scope["years"] and year not in scope["years"]:
            continue
        include = (scope.get("geographies") or {}).get("include")
        if include is not None and geography not in include:
            continue
        found.append((identity, bool(record["applies_to_masks"])))
    return found


def uncertain_annotation_contract(frames: Sequence[pd.DataFrame]) -> dict[str, Any] | None:
    """Retain source caveats explicitly when an operation cannot prove exact bindings."""
    records: dict[str, dict[str, Any]] = {}
    relevant: set[str] = set()
    sources: dict[str, list[dict[str, Any]]] = {}
    for frame in frames:
        contract = frame_annotation_contract(frame)
        if contract is None:
            continue
        for record in contract["records"]:
            key = annotation_identity_key(record["id"], record["revision"])
            if key in records and records[key] != record:
                raise ValueError(
                    "conflicting annotation content; use a new revision for changed records"
                )
            records[key] = record
            entries = sources.setdefault(key, [])
            for entry in contract["sources"].get(key, []):
                if entry not in entries:
                    entries.append(entry)
            geo_key = next((name for name in ("coc_id", "geo_id", "msa_id") if name in frame), None)
            if not geo_key or "year" not in frame:
                relevant.add(key)  # Unknown row grain: conservative, never silently clean.
                continue
            if key in contract["uncertain_ids"]:
                if not frame.empty:
                    relevant.add(key)
                continue
            if contract["schema_version"] == 1:
                columns = contract["bindings"].get(key, {"columns": {}})["columns"]
                if set(columns) - set(frame.columns):
                    raise ValueError(
                        "source annotation binding names missing columns; remap before aggregation"
                    )
                actual_cells = set(zip(frame[geo_key], frame["year"], strict=True))
                if any(tuple(cell) in actual_cells for cells in columns.values() for cell in cells):
                    relevant.add(key)
                continue
            scope = record["scope"]
            selected = pd.Series(True, index=frame.index)
            if scope.get("years"):
                selected &= frame["year"].isin(scope["years"])
            geography = scope.get("geographies")
            if geography is not None and geography.get("include") is not None:
                selected &= frame[geo_key].isin(geography["include"])
            if selected.any() and (
                not scope["columns"] or set(scope["columns"]) & set(frame.columns)
            ):
                relevant.add(key)
    if not records:
        return None
    validate_annotation_revisions(list(records.values()))
    return serialize_annotation_contract(
        list(records.values()), uncertain_ids=sorted(relevant), sources=sources,
    )


@dataclass(frozen=True)
class ArtifactContract:
    """Canonical schema contract for one artifact family."""

    name: str
    required_columns: tuple[str, ...]
    canonical_measures: tuple[str, ...] = ()
    lineage_measures: tuple[str, ...] = ()
    drift_prone_columns: tuple[str, ...] = DRIFT_PRONE_SOURCE_COLUMNS
    required_any_columns: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True)
class ContractFinding:
    """Structured schema validation finding."""

    severity: Literal["error", "warning"]
    code: str
    message: str
    column: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "column": self.column,
        }


COC_PANEL_CONTRACT = ArtifactContract(
    name="coc_panel",
    required_columns=tuple(COC_PANEL_COLUMNS),
    canonical_measures=("total_population",),
    lineage_measures=("total_population",),
)

MSA_COC_PANEL_CONTRACT = ArtifactContract(
    name="msa_coc_panel",
    required_columns=tuple(MSA_COC_PANEL_COLUMNS),
    canonical_measures=(
        "msa_population",
        "msa_median_rent",
        "msa_vacancy_rate",
        "msa_poverty_rate",
        "msa_unemployment",
        "msa_income",
        "msa_rent_burden",
        "pit_total",
        "pit_sheltered",
        "pit_unsheltered",
        "coc_population",
    ),
    drift_prone_columns=(),
)

MSA_COC_COVERAGE_CONTRACT = ArtifactContract(
    name="msa_coc_coverage",
    required_columns=tuple(MSA_COC_COVERAGE_COLUMNS),
    drift_prone_columns=(),
)

MSA_FRACTIONAL_ROLLUP_CONTRACT = ArtifactContract(
    name="msa_fractional_rollup",
    required_columns=tuple(MSA_FRACTIONAL_ROLLUP_COLUMNS),
    drift_prone_columns=(),
)

ACS_TRACT_CONTRACT = ArtifactContract(
    name="acs_tract",
    required_columns=tuple(ACS_TRACT_OUTPUT_COLUMNS),
)

PEP_COUNTY_CONTRACT = ArtifactContract(
    name="pep_county",
    required_columns=tuple(PEP_COUNTY_OUTPUT_COLUMNS),
)

URBAN_AREA_GEOMETRY_CONTRACT = ArtifactContract(
    name="urban_area_geometry",
    required_columns=tuple(URBAN_AREA_GEOMETRY_COLUMNS),
    drift_prone_columns=(),
)

PL_BLOCK_POPULATION_CONTRACT = ArtifactContract(
    name="pl_block_population",
    required_columns=tuple(PL_BLOCK_POPULATION_COLUMNS),
    canonical_measures=("total_population",),
    drift_prone_columns=(),
)

BLOCK_GEOMETRY_CONTRACT = ArtifactContract(
    name="block_geometry",
    required_columns=tuple(BLOCK_GEOMETRY_COLUMNS),
    drift_prone_columns=(),
)

COC_URBAN_FRACTION_CONTRACT = ArtifactContract(
    name="coc_urban_fraction",
    required_columns=tuple(COC_URBAN_FRACTION_COLUMNS),
    canonical_measures=("coc_total_population", "coc_urban_population", "coc_rural_population"),
    drift_prone_columns=(),
)

COC_URBAN_AREA_DETAIL_CONTRACT = ArtifactContract(
    name="coc_urban_area_detail",
    required_columns=tuple(COC_URBAN_AREA_DETAIL_COLUMNS),
    canonical_measures=("total_population", "urban_population"),
    drift_prone_columns=(),
)

PIT_CONTRACT = ArtifactContract(
    name="pit",
    required_columns=tuple(PIT_CANONICAL_COLUMNS),
)

ZORI_INGEST_CONTRACT = ArtifactContract(
    name="zori_ingest",
    required_columns=tuple(ZORI_INGEST_OUTPUT_COLUMNS),
)

MSA_ZORI_YEARLY_CONTRACT = ArtifactContract(
    name="msa_zori_yearly",
    required_columns=tuple(MSA_ZORI_YEARLY_COLUMNS),
    canonical_measures=("zori",),
    drift_prone_columns=(),
)

LAUS_METRO_CONTRACT = ArtifactContract(
    name="laus_metro",
    required_columns=tuple(LAUS_METRO_OUTPUT_COLUMNS),
)

SAE_OUTPUT_CONTRACT = ArtifactContract(
    name="sae_output",
    required_columns=tuple(SAE_OUTPUT_COLUMNS),
    canonical_measures=tuple(SAE_MEASURE_COLUMNS),
    drift_prone_columns=(),
)

ACS1_IMPUTATION_OUTPUT_CONTRACT = ArtifactContract(
    name="acs1_imputation_output",
    required_columns=tuple(ACS1_IMPUTATION_OUTPUT_COLUMNS),
    canonical_measures=tuple(
        column for column in ACS1_IMPUTATION_OUTPUT_COLUMNS if column.startswith("acs1_imputed_")
    ),
    drift_prone_columns=(),
)

TRACT_MEDIATED_COUNTY_XWALK_CONTRACT = ArtifactContract(
    name="tract_mediated_county_xwalk",
    required_columns=tuple(
        column for column in TRACT_MEDIATED_COUNTY_XWALK_COLUMNS if column != "geo_id"
    ),
    required_any_columns=(("geo_id", "coc_id", "metro_id", "msa_id"),),
)

ARTIFACT_CONTRACTS: dict[str, ArtifactContract] = {
    contract.name: contract
    for contract in (
        ACS_TRACT_CONTRACT,
        ACS1_IMPUTATION_OUTPUT_CONTRACT,
        COC_URBAN_AREA_DETAIL_CONTRACT,
        COC_URBAN_FRACTION_CONTRACT,
        COC_PANEL_CONTRACT,
        LAUS_METRO_CONTRACT,
        MSA_COC_COVERAGE_CONTRACT,
        MSA_COC_PANEL_CONTRACT,
        MSA_FRACTIONAL_ROLLUP_CONTRACT,
        MSA_ZORI_YEARLY_CONTRACT,
        PEP_COUNTY_CONTRACT,
        PIT_CONTRACT,
        BLOCK_GEOMETRY_CONTRACT,
        PL_BLOCK_POPULATION_CONTRACT,
        SAE_OUTPUT_CONTRACT,
        TRACT_MEDIATED_COUNTY_XWALK_CONTRACT,
        URBAN_AREA_GEOMETRY_CONTRACT,
        ZORI_INGEST_CONTRACT,
    )
}


def validate_artifact_contract(
    df: pd.DataFrame,
    contract: ArtifactContract,
) -> list[ContractFinding]:
    """Validate a DataFrame against a canonical artifact contract."""
    findings: list[ContractFinding] = []
    columns = set(df.columns)

    for col in contract.required_columns:
        if col not in columns:
            findings.append(
                ContractFinding(
                    severity="error",
                    code="missing_required_column",
                    message=(
                        f"Missing required column '{col}' for "
                        f"{contract.name}. Add it or update the schema contract."
                    ),
                    column=col,
                )
            )

    for candidates in contract.required_any_columns:
        if columns.isdisjoint(candidates):
            candidate_label = ", ".join(candidates)
            findings.append(
                ContractFinding(
                    severity="error",
                    code="missing_required_column_group",
                    message=(
                        f"Missing required column group for {contract.name}: "
                        f"provide one of {candidate_label}."
                    ),
                    column="|".join(candidates),
                )
            )

    for col in contract.drift_prone_columns:
        if col in columns and col not in contract.required_columns:
            findings.append(
                ContractFinding(
                    severity="warning",
                    code="drift_prone_column",
                    message=(
                        f"Column '{col}' is drift-prone in {contract.name}. "
                        "Use a canonical source-qualified column or an explicit "
                        "artifact contract."
                    ),
                    column=col,
                )
            )

    for measure in contract.lineage_measures:
        if measure not in columns:
            continue
        missing = [col for col in population_lineage_columns(measure) if col not in columns]
        if missing:
            findings.append(
                ContractFinding(
                    severity="warning",
                    code="missing_lineage_columns",
                    message=(
                        f"Measure '{measure}' is missing lineage columns: "
                        f"{missing}. Populate controlled lineage tokens."
                    ),
                    column=measure,
                )
            )

    return findings
