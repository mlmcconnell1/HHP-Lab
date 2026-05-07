"""Artifact-level schema contracts and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from hhplab.schema.columns import COC_PANEL_COLUMNS, DRIFT_PRONE_SOURCE_COLUMNS
from hhplab.schema.lineage import population_lineage_columns


@dataclass(frozen=True)
class ArtifactContract:
    """Canonical schema contract for one artifact family."""

    name: str
    required_columns: tuple[str, ...]
    canonical_measures: tuple[str, ...] = ()
    lineage_measures: tuple[str, ...] = ()
    drift_prone_columns: tuple[str, ...] = DRIFT_PRONE_SOURCE_COLUMNS


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
