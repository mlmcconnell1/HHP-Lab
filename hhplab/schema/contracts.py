"""Artifact-level schema contracts and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from hhplab.schema.columns import (
    ACS_TRACT_OUTPUT_COLUMNS,
    COC_PANEL_COLUMNS,
    DRIFT_PRONE_SOURCE_COLUMNS,
    LAUS_METRO_OUTPUT_COLUMNS,
    PEP_COUNTY_OUTPUT_COLUMNS,
    PIT_CANONICAL_COLUMNS,
    TRACT_MEDIATED_COUNTY_XWALK_COLUMNS,
    ZORI_INGEST_OUTPUT_COLUMNS,
)
from hhplab.schema.lineage import population_lineage_columns


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

ACS_TRACT_CONTRACT = ArtifactContract(
    name="acs_tract",
    required_columns=tuple(ACS_TRACT_OUTPUT_COLUMNS),
)

PEP_COUNTY_CONTRACT = ArtifactContract(
    name="pep_county",
    required_columns=tuple(PEP_COUNTY_OUTPUT_COLUMNS),
)

PIT_CONTRACT = ArtifactContract(
    name="pit",
    required_columns=tuple(PIT_CANONICAL_COLUMNS),
)

ZORI_INGEST_CONTRACT = ArtifactContract(
    name="zori_ingest",
    required_columns=tuple(ZORI_INGEST_OUTPUT_COLUMNS),
)

LAUS_METRO_CONTRACT = ArtifactContract(
    name="laus_metro",
    required_columns=tuple(LAUS_METRO_OUTPUT_COLUMNS),
)

TRACT_MEDIATED_COUNTY_XWALK_CONTRACT = ArtifactContract(
    name="tract_mediated_county_xwalk",
    required_columns=tuple(
        column
        for column in TRACT_MEDIATED_COUNTY_XWALK_COLUMNS
        if column != "geo_id"
    ),
    required_any_columns=(("geo_id", "coc_id", "metro_id", "msa_id"),),
)

ARTIFACT_CONTRACTS: dict[str, ArtifactContract] = {
    contract.name: contract
    for contract in (
        ACS_TRACT_CONTRACT,
        COC_PANEL_CONTRACT,
        LAUS_METRO_CONTRACT,
        PEP_COUNTY_CONTRACT,
        PIT_CONTRACT,
        TRACT_MEDIATED_COUNTY_XWALK_CONTRACT,
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
        missing = [
            col for col in population_lineage_columns(measure) if col not in columns
        ]
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
