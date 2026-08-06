"""Coordinate schema-freshness checks for curated artifacts.

This application-level validator consumes source-owned output schemas and
geography definitions. Canonical column and filename policy remains in
``hhplab.schema`` and ``hhplab.naming``; this module only matches artifacts,
compares their physical schemas, and supplies rebuild guidance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from hhplab.geographies.gf_metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
)
from hhplab.geographies.gf_metro.metro_definitions import (
    DEFINITION_VERSION as GLYNN_FOX_DEFINITION_VERSION,
)
from hhplab.sources.census.acs.variables import TRACT_OUTPUT_COLUMNS
from hhplab.sources.census.acs.variables_acs1 import (
    ACS1_COUNTY_OUTPUT_COLUMNS,
    ACS1_METRO_OUTPUT_COLUMNS,
)

CANONICAL_DEFINITION_BY_COMPACT_TOKEN = {
    "censusmsa2023": CANONICAL_UNIVERSE_DEFINITION_VERSION,
    "glynnfoxv1": GLYNN_FOX_DEFINITION_VERSION,
}


@dataclass(frozen=True)
class CuratedSchemaIssue:
    """A curated artifact whose physical schema differs from the current contract."""

    path: Path
    artifact_family: str
    expected_columns: list[str]
    actual_columns: list[str]
    missing_columns: list[str]
    extra_columns: list[str]
    remediation: str

    @property
    def message(self) -> str:
        """Return a concise actionable schema-staleness message."""
        parts: list[str] = []
        if self.missing_columns:
            preview = ", ".join(self.missing_columns[:8])
            if len(self.missing_columns) > 8:
                preview += ", ..."
            parts.append(f"missing {len(self.missing_columns)} column(s): {preview}")
        if self.extra_columns:
            preview = ", ".join(self.extra_columns[:8])
            if len(self.extra_columns) > 8:
                preview += ", ..."
            parts.append(f"has {len(self.extra_columns)} extra column(s): {preview}")
        detail = "; ".join(parts)
        return f"{self.path.name} schema is stale: {detail}. Re-run: {self.remediation}"

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable issue payload."""
        return {
            "path": str(self.path),
            "artifact_family": self.artifact_family,
            "expected_column_count": len(self.expected_columns),
            "actual_column_count": len(self.actual_columns),
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "remedy": self.remediation,
            "remediation": self.remediation,
            "message": self.message,
        }


@dataclass(frozen=True)
class CuratedSchemaContract:
    """Schema contract for one curated artifact filename family."""

    artifact_family: str
    pattern: re.Pattern[str]
    expected_columns: list[str]
    remediation_template: str

    def remediation(self, path: Path, match: re.Match[str]) -> str:
        """Return the rebuild command for a matching artifact path."""
        values = match.groupdict()
        if "acs_end" in values:
            acs_end = int(values["acs_end"])
            values["acs_range"] = f"{acs_end - 4}-{acs_end}"
        if "definition_version" in values:
            values["definition_version"] = CANONICAL_DEFINITION_BY_COMPACT_TOKEN.get(
                values["definition_version"],
                values["definition_version"],
            )
        return self.remediation_template.format(**values)


CURATED_SCHEMA_CONTRACTS: tuple[CuratedSchemaContract, ...] = (
    CuratedSchemaContract(
        artifact_family="acs5_tracts",
        pattern=re.compile(r"^acs5_tracts__A(?P<acs_end>\d{4})xT(?P<tract>\d{4})\.parquet$"),
        expected_columns=TRACT_OUTPUT_COLUMNS,
        remediation_template=(
            "hhplab ingest acs5-tract --acs {acs_range} --tracts {tract} --force"
        ),
    ),
    CuratedSchemaContract(
        artifact_family="acs1_metro",
        pattern=re.compile(
            r"^acs1_metro__A(?P<vintage>\d{4})@D(?P<definition_version>\w+)\.parquet$"
        ),
        expected_columns=ACS1_METRO_OUTPUT_COLUMNS,
        remediation_template=(
            "hhplab ingest acs1-metro --vintage {vintage} "
            "--definition-version {definition_version}"
        ),
    ),
    CuratedSchemaContract(
        artifact_family="acs1_county",
        pattern=re.compile(r"^acs1_county__A(?P<vintage>\d{4})\.parquet$"),
        expected_columns=ACS1_COUNTY_OUTPUT_COLUMNS,
        remediation_template="hhplab ingest acs1-county --vintage {vintage}",
    ),
)


def validate_curated_schemas(base_dir: Path) -> list[CuratedSchemaIssue]:
    """Return readable curated artifacts whose schema differs from current contracts."""
    acs_dir = base_dir / "acs"
    if not acs_dir.is_dir():
        return []

    issues: list[CuratedSchemaIssue] = []
    for path in sorted(acs_dir.glob("*.parquet")):
        for contract in CURATED_SCHEMA_CONTRACTS:
            match = contract.pattern.match(path.name)
            if not match:
                continue
            try:
                actual_columns = list(pq.read_schema(path).names)
            except Exception:
                break
            expected_columns = list(contract.expected_columns)
            missing_columns = [
                column for column in expected_columns if column not in actual_columns
            ]
            extra_columns = [
                column for column in actual_columns if column not in expected_columns
            ]
            if missing_columns or extra_columns:
                issues.append(
                    CuratedSchemaIssue(
                        path=path,
                        artifact_family=contract.artifact_family,
                        expected_columns=expected_columns,
                        actual_columns=actual_columns,
                        missing_columns=missing_columns,
                        extra_columns=extra_columns,
                        remediation=contract.remediation(path, match),
                    )
                )
            break
    return issues
