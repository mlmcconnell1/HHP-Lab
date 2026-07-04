"""Panel-ready aggregation helpers for expanded covariates."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.covariates.catalog import covariate_source_spec
from hhplab.covariates.ingest import default_covariate_output_path
from hhplab.naming import covariate_panel_filename
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance


def default_covariate_panel_path(
    source_id: str,
    *,
    output_dir: Path | str | None = None,
) -> Path:
    """Return the deterministic panel-ready output path."""
    spec = covariate_source_spec(source_id)
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    return base / covariate_panel_filename(
        source_id,
        spec.first_year,
        spec.last_year,
    )


def aggregate_covariate_source(
    source_id: str,
    *,
    curated_path: Path | str | None = None,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    years: list[int] | None = None,
    force: bool = False,
) -> Path:
    """Materialize a panel-ready covariate table from curated source data.

    County, CoC, and state policy sources are already native to analysis
    geographies, so this aggregation step validates schema, filters years, and
    emits a stable panel artifact with provenance. Cross-geography recipes can
    then join or crosswalk the output explicitly.
    """
    spec = covariate_source_spec(source_id)
    input_path = (
        Path(curated_path)
        if curated_path is not None
        else default_covariate_output_path(source_id, output_dir=output_dir)
    )
    if not input_path.exists():
        raise FileNotFoundError(
            f"Curated covariate file not found: {input_path}. "
            f"Run `hhplab ingest covariate --source {source_id}` first."
        )
    destination = (
        Path(output_path)
        if output_path is not None
        else default_covariate_panel_path(source_id, output_dir=output_dir)
    )
    if destination.exists() and not force:
        return destination

    df = pd.read_parquet(input_path)
    required = ["geo_type", "geo_id", "year", *spec.measure_columns]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Curated covariate file missing required columns: {missing}")
    result = df[required].copy()
    if years is not None:
        result = result[result["year"].isin(years)].copy()
    result = result.sort_values(["geo_id", "year"]).reset_index(drop=True)

    input_provenance = read_provenance(input_path)
    provenance = ProvenanceBlock(
        geo_type=spec.native_geo,
        extra={
            "dataset_type": "expanded_covariate_panel",
            "source_id": source_id,
            "provider": spec.provider,
            "product": spec.product,
            "native_geo": spec.native_geo,
            "years": years,
            "measure_columns": list(spec.measure_columns),
            "input_path": str(input_path),
            "input_provenance": input_provenance.to_dict() if input_provenance else None,
        },
    )
    write_parquet_with_provenance(result, destination, provenance)
    return destination
