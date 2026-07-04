"""Panel enrichment helpers for joining curated artifacts onto panels."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance


class PanelEnrichError(ValueError):
    """Raised when a panel enrichment request is invalid."""


GEO_COLUMN_CANDIDATES: tuple[str, ...] = (
    "geo_id",
    "msa_id",
    "coc_id",
    "metro_id",
    "cbsa_code",
)


@dataclass(frozen=True)
class RateSpec:
    """Derived rate column request."""

    numerator: str
    denominator: str
    rate_per: float = 1000.0
    name: str | None = None

    @property
    def output_name(self) -> str:
        if self.name:
            return self.name
        scale = int(self.rate_per) if float(self.rate_per).is_integer() else self.rate_per
        return f"{self.numerator}_per_{scale}"


def _read_parquet(path: Path, *, label: str) -> pd.DataFrame:
    if not path.exists():
        raise PanelEnrichError(f"{label} parquet not found: {path}")
    return pd.read_parquet(path)


def _detect_geo_column(df: pd.DataFrame, *, label: str) -> str:
    matches = [column for column in GEO_COLUMN_CANDIDATES if column in df.columns]
    if not matches:
        raise PanelEnrichError(
            f"Could not detect {label} geography column. "
            f"Expected one of {list(GEO_COLUMN_CANDIDATES)}; available columns: "
            f"{sorted(df.columns.tolist())}"
        )
    if len(matches) > 1:
        raise PanelEnrichError(
            f"Ambiguous {label} geography columns {matches}; pass an explicit geo column."
        )
    return matches[0]


def _validate_columns(df: pd.DataFrame, columns: list[str], *, label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise PanelEnrichError(
            f"Requested {label} columns are missing: {missing}. "
            f"Available columns: {sorted(df.columns.tolist())}"
        )


def _source_value_columns(
    source_df: pd.DataFrame,
    *,
    source_key_columns: list[str],
    columns: list[str] | None,
) -> list[str]:
    if columns is not None:
        _validate_columns(source_df, columns, label="source")
        key_set = set(source_key_columns)
        return [column for column in columns if column not in key_set]
    return [column for column in source_df.columns if column not in set(source_key_columns)]


def _copy_provenance_fields(source: ProvenanceBlock | None) -> dict[str, Any]:
    if source is None:
        return {}
    return {
        "boundary_vintage": source.boundary_vintage,
        "tract_vintage": source.tract_vintage,
        "county_vintage": source.county_vintage,
        "acs_vintage": source.acs_vintage,
        "notation": source.notation,
        "weighting": source.weighting,
        "geo_type": source.geo_type,
        "definition_version": source.definition_version,
    }


def enrich_panel_file(
    panel_path: Path,
    *,
    source_path: Path,
    output_path: Path,
    columns: list[str] | None = None,
    panel_geo_column: str | None = None,
    source_geo_column: str | None = None,
    include_year: bool | None = None,
    rate_specs: list[RateSpec] | None = None,
) -> dict[str, Any]:
    """Join selected source columns onto a panel and write a provenance-rich parquet."""
    panel_df = _read_parquet(panel_path, label="Panel")
    source_df = _read_parquet(source_path, label="Source")
    panel_geo_column = panel_geo_column or _detect_geo_column(panel_df, label="panel")
    source_geo_column = source_geo_column or _detect_geo_column(source_df, label="source")
    _validate_columns(panel_df, [panel_geo_column], label="panel")
    _validate_columns(source_df, [source_geo_column], label="source")

    join_on_year = (
        "year" in panel_df.columns and "year" in source_df.columns
        if include_year is None
        else include_year
    )
    if include_year and ("year" not in panel_df.columns or "year" not in source_df.columns):
        raise PanelEnrichError(
            "--include-year requires both panel and source to contain a 'year' column."
        )

    panel_key_columns = [panel_geo_column]
    source_key_columns = [source_geo_column]
    if join_on_year:
        panel_key_columns.append("year")
        source_key_columns.append("year")

    value_columns = _source_value_columns(
        source_df,
        source_key_columns=source_key_columns,
        columns=columns,
    )
    if not value_columns:
        raise PanelEnrichError(
            "No source value columns selected for enrichment. "
            "Pass --columns with at least one non-key source column."
        )
    existing = [column for column in value_columns if column in panel_df.columns]
    if existing:
        raise PanelEnrichError(
            f"Source columns would overwrite existing panel columns: {existing}. "
            "Select or rename source columns before enrichment."
        )

    source_subset = source_df.loc[:, source_key_columns + value_columns].copy()
    duplicates = source_subset.duplicated(subset=source_key_columns, keep=False)
    if duplicates.any():
        sample = source_subset.loc[duplicates, source_key_columns].head(5).to_dict(orient="records")
        raise PanelEnrichError(
            f"Source rows are not unique by join keys {source_key_columns}; sample duplicates: "
            f"{sample}. Aggregate or deduplicate the source before enrichment."
        )

    enriched = panel_df.merge(
        source_subset,
        how="left",
        left_on=panel_key_columns,
        right_on=source_key_columns,
        validate="many_to_one",
    )
    if source_geo_column != panel_geo_column and source_geo_column in enriched.columns:
        enriched = enriched.drop(columns=[source_geo_column])

    derived_rates: list[dict[str, Any]] = []
    for spec in rate_specs or []:
        _validate_columns(enriched, [spec.numerator, spec.denominator], label="enriched panel")
        denominator = pd.to_numeric(enriched[spec.denominator], errors="coerce")
        numerator = pd.to_numeric(enriched[spec.numerator], errors="coerce")
        output_name = spec.output_name
        if output_name in enriched.columns and output_name not in {
            spec.numerator,
            spec.denominator,
        }:
            raise PanelEnrichError(f"Rate output column already exists: {output_name}")
        enriched[output_name] = numerator.div(denominator.where(denominator != 0)) * spec.rate_per
        derived_rates.append(
            {
                "name": output_name,
                "numerator": spec.numerator,
                "denominator": spec.denominator,
                "rate_per": spec.rate_per,
            }
        )

    panel_provenance = read_provenance(panel_path)
    source_provenance = read_provenance(source_path)
    provenance = ProvenanceBlock(
        **_copy_provenance_fields(panel_provenance),
        extra={
            "dataset_type": "panel_enrichment",
            "input_panel": str(panel_path),
            "input_source": str(source_path),
            "input_panel_provenance": (
                panel_provenance.to_dict() if panel_provenance is not None else None
            ),
            "input_source_provenance": (
                source_provenance.to_dict() if source_provenance is not None else None
            ),
            "join": {
                "panel_geo_column": panel_geo_column,
                "source_geo_column": source_geo_column,
                "include_year": join_on_year,
                "panel_key_columns": panel_key_columns,
                "source_key_columns": source_key_columns,
            },
            "source_columns": value_columns,
            "derived_rates": derived_rates,
            "input_row_count": int(len(panel_df)),
            "source_row_count": int(len(source_df)),
            "matched_row_count": int(enriched[value_columns].notna().any(axis=1).sum()),
            "output_row_count": int(len(enriched)),
            "output_columns": list(enriched.columns),
        },
    )
    write_parquet_with_provenance(enriched, output_path, provenance)
    return {
        "status": "ok",
        "panel_path": str(panel_path),
        "source_path": str(source_path),
        "output_path": str(output_path),
        "row_count": int(len(enriched)),
        "columns": list(enriched.columns),
        "join": provenance.extra["join"],
        "source_columns": value_columns,
        "derived_rates": derived_rates,
        "matched_row_count": provenance.extra["matched_row_count"],
    }
