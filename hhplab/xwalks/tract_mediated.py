"""Tract-mediated county-to-geography crosswalk weights.

This module derives county-to-analysis-geography allocation weights by
composing a tract crosswalk with tract denominator columns. Denominators can
come from ACS tract estimates or fixed decennial tract populations. It is
intentionally separate from direct county polygon overlays so existing
``area_share`` semantics remain unchanged.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import pandas as pd

from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import (
    TRACT_MEDIATED_COUNTY_XWALK_COLUMNS,
    TRACT_MEDIATED_DENOMINATOR_COLUMNS,
    TRACT_MEDIATED_WEIGHT_COLUMNS,
)

DENOMINATOR_COLUMNS: dict[str, str] = TRACT_MEDIATED_DENOMINATOR_COLUMNS

WEIGHT_COLUMNS: tuple[str, ...] = TRACT_MEDIATED_WEIGHT_COLUMNS
WeightingMode = Literal["area", "population", "household", "renter_household"]
DenominatorSource = Literal["acs", "decennial"]
DEFAULT_WEIGHTING_MODES: tuple[WeightingMode, ...] = (
    "area",
    "population",
    "household",
    "renter_household",
)
WEIGHTING_MODE_TO_COLUMN: dict[WeightingMode, str] = {
    "area": "area_weight",
    "population": "population_weight",
    "household": "household_weight",
    "renter_household": "renter_household_weight",
}
COUNTY_VINTAGE_SEMANTICS = (
    "county_vintage identifies the downstream county-FIPS universe expected "
    "by county-native inputs. Tract-mediated county_fips values are derived "
    "from tract GEOID prefixes; no county geometry is intersected."
)


def normalize_weighting_modes(values: list[str] | None) -> tuple[WeightingMode, ...]:
    """Normalize CLI-style weighting-mode values into supported mode names."""
    if not values:
        return DEFAULT_WEIGHTING_MODES
    valid = set(WEIGHTING_MODE_TO_COLUMN)
    invalid = sorted(set(values) - valid)
    if invalid:
        raise ValueError(
            f"Invalid --weighting-mode value(s): {', '.join(invalid)}. "
            f"Supported values: {', '.join(sorted(valid))}."
        )
    return tuple(values)  # type: ignore[return-value]


def resolve_denominator_vintage(
    *,
    denominator_source: DenominatorSource,
    denominator_vintage: str | None,
    acs_vintage: str,
    tract_vintage: int,
) -> str:
    """Resolve tract-mediated denominator vintage from source-specific options."""
    if denominator_source == "acs":
        return denominator_vintage or acs_vintage
    if denominator_source == "decennial":
        if denominator_vintage is None:
            raise ValueError(
                "--denominator-vintage is required when --denominator-source decennial."
            )
        if denominator_vintage not in {"2010", "2020"}:
            raise ValueError(
                "Unsupported decennial denominator vintage "
                f"{denominator_vintage!r}. Supported vintages: 2010, 2020."
            )
        if str(tract_vintage) != denominator_vintage:
            raise ValueError(
                "Decennial tract-mediated denominators are native to their "
                f"tract era; got --denominator-vintage {denominator_vintage} "
                f"with --tracts {tract_vintage}."
            )
        return denominator_vintage
    raise ValueError(
        f"Invalid denominator source {denominator_source!r}; use 'acs' or 'decennial'."
    )


def input_status(path: Path) -> dict[str, str | bool]:
    """Return a JSON-serializable path existence payload."""
    return {"path": str(path), "exists": path.exists()}


def tract_mediated_paths(
    *,
    boundary: str,
    county_vintage: int,
    tract_vintage: int,
    acs_vintage: str,
    denominator_source: DenominatorSource,
    denominator_vintage: str | int,
) -> dict[str, Path]:
    """Resolve all input and output paths for a tract-mediated county crosswalk."""
    from hhplab.acs.ingest.tract_population import get_output_path as acs_tract_path
    from hhplab.census.ingest.decennial_tract_population import (
        get_output_path as decennial_tract_path,
    )
    from hhplab.naming import (
        county_path,
        tract_mediated_county_xwalk_path,
        tract_xwalk_path,
    )

    denominator_path = (
        acs_tract_path(acs_vintage, str(tract_vintage))
        if denominator_source == "acs"
        else decennial_tract_path(str(denominator_vintage), str(tract_vintage))
    )
    return {
        "tract_crosswalk": tract_xwalk_path(boundary, tract_vintage),
        "denominator_tracts": denominator_path,
        "counties": county_path(county_vintage),
        "output": tract_mediated_county_xwalk_path(
            boundary,
            county_vintage,
            tract_vintage,
            acs_vintage,
            denominator_source=denominator_source,
            denominator_vintage=denominator_vintage,
        ),
    }


def tract_mediated_preflight_payload(
    *,
    boundary: str,
    county_vintage: int,
    tract_vintage: int,
    acs_vintage: str,
    denominator_source: DenominatorSource,
    denominator_vintage: str | int,
    selected_weighting_modes: tuple[WeightingMode, ...],
    force: bool,
    dry_run: bool,
) -> tuple[dict[str, object], dict[str, Path]]:
    """Build a path-aware preflight payload for tract-mediated generation."""
    paths = tract_mediated_paths(
        boundary=boundary,
        county_vintage=county_vintage,
        tract_vintage=tract_vintage,
        acs_vintage=acs_vintage,
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
    )
    inputs = {
        "tract_crosswalk": input_status(paths["tract_crosswalk"]),
        "denominator_tracts": input_status(paths["denominator_tracts"]),
    }
    return (
        {
            "status": "ok",
            "action": "dry_run" if dry_run else "generate",
            "boundary_vintage": boundary,
            "county_vintage": str(county_vintage),
            "tract_vintage": str(tract_vintage),
            "acs_vintage": acs_vintage,
            "denominator_source": denominator_source,
            "denominator_vintage": str(denominator_vintage),
            "weighting_family": "tract_mediated",
            "weighting_modes": list(selected_weighting_modes),
            "inputs": inputs,
            "artifact": str(paths["output"]),
            "will_write": not dry_run,
            "force": force,
        },
        paths,
    )


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"{label} missing required column(s): {', '.join(missing)}. "
            "Provide a tract crosswalk and ACS tract denominator table with the "
            "canonical HHP-Lab schema."
        )


def _standardize_denominator_tracts(
    denominator_tracts: pd.DataFrame,
    *,
    label: str,
) -> pd.DataFrame:
    denominators = denominator_tracts.copy()
    if "GEOID" in denominators.columns and "tract_geoid" not in denominators.columns:
        denominators = denominators.rename(columns={"GEOID": "tract_geoid"})
    _require_columns(denominators, {"tract_geoid", "total_population"}, label=label)
    denominators["tract_geoid"] = denominators["tract_geoid"].astype(str).str.zfill(11)

    keep = ["tract_geoid"]
    for denominator_col in set(DENOMINATOR_COLUMNS.values()) - {"tract_area"}:
        if denominator_col in denominators.columns:
            denominators[denominator_col] = pd.to_numeric(
                denominators[denominator_col],
                errors="coerce",
            )
            keep.append(denominator_col)
    return denominators[keep].drop_duplicates("tract_geoid")


def _normalize_county_fips(values: Iterable[object]) -> set[str]:
    return {str(value).zfill(5) for value in values if pd.notna(value)}


def _validate_county_vintage_semantics(
    *,
    tract_county_fips: Iterable[object],
    county_vintage: str | int,
    tract_vintage: str | int,
    expected_county_fips: Iterable[object] | None = None,
) -> None:
    try:
        county_year = int(county_vintage)
        tract_year = int(tract_vintage)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Tract-mediated county_vintage and tract_vintage must be numeric years "
            f"because county_fips are derived from tract GEOID prefixes; got "
            f"county_vintage={county_vintage!r}, tract_vintage={tract_vintage!r}."
        ) from exc

    if county_year < tract_year:
        raise ValueError(
            "Unsupported tract-mediated county vintage combination: "
            f"county_vintage {county_vintage} is older than tract_vintage {tract_vintage}. "
            f"{COUNTY_VINTAGE_SEMANTICS} Use a county_vintage that matches the "
            "county-native downstream data, or rebuild with a compatible tract vintage."
        )

    if expected_county_fips is None:
        return

    derived = _normalize_county_fips(tract_county_fips)
    expected = _normalize_county_fips(expected_county_fips)
    missing = sorted(derived - expected)
    if missing:
        raise ValueError(
            "Tract-mediated county_fips are incompatible with the requested "
            f"county_vintage {county_vintage}: {missing[:10]}"
            f"{'...' if len(missing) > 10 else ''} are derived from tract GEOID "
            "prefixes but absent from the expected county-FIPS universe. "
            f"{COUNTY_VINTAGE_SEMANTICS}"
        )


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(denominator > 0)
    return numerator / denom


def _format_missing_denominator_coverage(
    coverage: pd.DataFrame,
    *,
    geo_id_col: str,
    max_rows: int = 5,
) -> str:
    missing = coverage[coverage["missing_denominator_tract_count"] > 0]
    examples = [
        (
            f"{row[geo_id_col]}/{row['county_fips']}: "
            f"{int(row['missing_denominator_tract_count'])} of "
            f"{int(row['tract_count'])} tract(s) missing"
        )
        for _, row in missing.head(max_rows).iterrows()
    ]
    suffix = ""
    if len(missing) > max_rows:
        suffix = f"; {len(missing) - max_rows} more geography/county pair(s)"
    return "; ".join(examples) + suffix


def build_tract_mediated_county_crosswalk(
    tract_crosswalk: pd.DataFrame,
    denominator_tracts: pd.DataFrame,
    *,
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int | None = None,
    denominator_source: str = "acs",
    denominator_vintage: str | int | None = None,
    expected_county_fips: Iterable[object] | None = None,
    geo_id_col: str = "coc_id",
    allow_incomplete_denominator_coverage: bool = False,
) -> pd.DataFrame:
    """Build county-to-geography weights mediated through tracts.

    Parameters
    ----------
    tract_crosswalk : pd.DataFrame
        Analysis geography to tract crosswalk with ``geo_id_col``,
        ``tract_geoid``, ``area_share``, ``intersection_area``, and
        ``tract_area`` columns.
    denominator_tracts : pd.DataFrame
        Tract denominator table with ``tract_geoid`` or ``GEOID``,
        ``total_population``, and optionally ``total_households`` and
        ``renter_households``.
    boundary_vintage, county_vintage, tract_vintage
        Vintage metadata carried into output rows and provenance. The
        ``county_vintage`` names the downstream county-FIPS universe expected
        by county-native inputs; this builder derives ``county_fips`` from
        tract GEOID prefixes and does not read county geometry.
    acs_vintage
        ACS denominator vintage for backward-compatible ACS callers.
    denominator_source, denominator_vintage
        Explicit denominator metadata. ``denominator_source`` must be
        ``"acs"`` or ``"decennial"``.
    expected_county_fips
        Optional county-FIPS universe for validating derived tract-prefix
        county codes against the requested ``county_vintage``.
    geo_id_col : str
        Analysis geography identifier column. Defaults to ``"coc_id"``.
    allow_incomplete_denominator_coverage : bool
        If False (default), any missing tract denominator row is an error.
        Set True only for materializing artifacts where unsupported source
        geographies should remain visible through missing-denominator
        diagnostics and null denominator-based weights.

    Returns
    -------
    pd.DataFrame
        One row per geography/county pair with normalized allocation weights
        and denominator diagnostics. Weight denominators are county totals,
        so per-county weight sums are also coverage diagnostics.
    """
    _require_columns(
        tract_crosswalk,
        {geo_id_col, "tract_geoid", "area_share", "intersection_area", "tract_area"},
        label="tract_crosswalk",
    )

    xwalk = tract_crosswalk.copy()
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype(str).str.zfill(11)
    xwalk["county_fips"] = xwalk["tract_geoid"].str[:5]
    for col in ("area_share", "intersection_area", "tract_area"):
        xwalk[col] = pd.to_numeric(xwalk[col], errors="coerce")
    _validate_county_vintage_semantics(
        tract_county_fips=xwalk["county_fips"].unique(),
        county_vintage=county_vintage,
        tract_vintage=tract_vintage,
        expected_county_fips=expected_county_fips,
    )

    resolved_denominator_vintage = _resolve_denominator_vintage(
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
        acs_vintage=acs_vintage,
    )
    denominator_label = f"{denominator_source}_tracts"
    household_available = "total_households" in denominator_tracts.columns
    renter_available = "renter_households" in denominator_tracts.columns
    denominators = _standardize_denominator_tracts(
        denominator_tracts,
        label=denominator_label,
    )
    merged = xwalk.merge(denominators, on="tract_geoid", how="left")

    tract_coverage = merged[
        [geo_id_col, "county_fips", "tract_geoid", "total_population"]
    ].drop_duplicates([geo_id_col, "county_fips", "tract_geoid"])
    group_coverage = (
        tract_coverage.groupby([geo_id_col, "county_fips"], dropna=False)
        .agg(
            tract_count=("tract_geoid", "nunique"),
            denominator_tract_count=("total_population", "count"),
        )
        .reset_index()
    )
    group_coverage["missing_denominator_tract_count"] = (
        group_coverage["tract_count"] - group_coverage["denominator_tract_count"]
    )
    group_coverage["denominator_tract_coverage_ratio"] = _safe_divide(
        group_coverage["denominator_tract_count"],
        group_coverage["tract_count"],
    )
    county_coverage = (
        tract_coverage.drop_duplicates(["county_fips", "tract_geoid"])
        .groupby("county_fips", dropna=False)
        .agg(
            county_tract_count=("tract_geoid", "nunique"),
            county_denominator_tract_count=("total_population", "count"),
        )
        .reset_index()
    )
    county_coverage["county_missing_denominator_tract_count"] = (
        county_coverage["county_tract_count"]
        - county_coverage["county_denominator_tract_count"]
    )
    county_coverage["county_denominator_tract_coverage_ratio"] = _safe_divide(
        county_coverage["county_denominator_tract_count"],
        county_coverage["county_tract_count"],
    )
    if (
        not allow_incomplete_denominator_coverage
        and (group_coverage["missing_denominator_tract_count"] > 0).any()
    ):
        details = _format_missing_denominator_coverage(
            group_coverage,
            geo_id_col=geo_id_col,
        )
        raise ValueError(
            "Tract-mediated denominator coverage is incomplete: "
            f"{details}. Add the missing tract denominator rows to "
            f"{denominator_label} before building county weights."
        )

    # Pair-level raw contributions: tract fraction in geography times tract denominator.
    merged["area_denominator"] = merged["intersection_area"]
    for output_name, denominator_col in DENOMINATOR_COLUMNS.items():
        if output_name == "area":
            continue
        pair_col = f"{output_name}_denominator"
        if denominator_col in merged.columns:
            merged[pair_col] = merged["area_share"] * merged[denominator_col]
        else:
            merged[pair_col] = pd.NA

    pair_denominator_cols = [
        "area_denominator",
        "population_denominator",
        "household_denominator",
        "renter_household_denominator",
    ]

    grouped = (
        merged.groupby([geo_id_col, "county_fips"], dropna=False)
        .agg(
            area_denominator=("area_denominator", "sum"),
            population_denominator=("population_denominator", "sum"),
            household_denominator=("household_denominator", "sum"),
            renter_household_denominator=("renter_household_denominator", "sum"),
            missing_population_tract_count=("total_population", lambda s: int(s.isna().sum())),
            missing_household_tract_count=(
                "total_households",
                lambda s: int(s.isna().sum()),
            )
            if "total_households" in merged.columns
            else ("tract_geoid", lambda s: len(s)),
            missing_renter_household_tract_count=(
                "renter_households",
                lambda s: int(s.isna().sum()),
            )
            if "renter_households" in merged.columns
            else ("tract_geoid", lambda s: len(s)),
        )
        .reset_index()
    )
    grouped = grouped.merge(group_coverage, on=[geo_id_col, "county_fips"], how="left")

    unique_tracts = merged.drop_duplicates("tract_geoid")
    county_totals = unique_tracts.groupby("county_fips", dropna=False).agg(
        county_area_total=("tract_area", "sum"),
        county_population_total=("total_population", "sum"),
        county_household_total=(
            "total_households",
            "sum",
        )
        if "total_households" in unique_tracts.columns
        else ("tract_geoid", lambda s: pd.NA),
        county_renter_household_total=(
            "renter_households",
            "sum",
        )
        if "renter_households" in unique_tracts.columns
        else ("tract_geoid", lambda s: pd.NA),
    )
    grouped = grouped.merge(county_totals.reset_index(), on="county_fips", how="left")
    grouped = grouped.merge(county_coverage, on="county_fips", how="left")

    geo_totals = grouped.groupby(geo_id_col, dropna=False)[pair_denominator_cols].transform("sum")
    geo_totals = geo_totals.rename(
        columns={
            "area_denominator": "geo_area_total",
            "population_denominator": "geo_population_total",
            "household_denominator": "geo_household_total",
            "renter_household_denominator": "geo_renter_household_total",
        }
    )
    grouped = pd.concat([grouped, geo_totals], axis=1)

    grouped["area_weight"] = _safe_divide(
        grouped["area_denominator"],
        grouped["county_area_total"],
    )
    grouped["population_weight"] = _safe_divide(
        grouped["population_denominator"],
        grouped["county_population_total"],
    )
    grouped["household_weight"] = _safe_divide(
        grouped["household_denominator"],
        grouped["county_household_total"],
    )
    grouped["renter_household_weight"] = _safe_divide(
        grouped["renter_household_denominator"],
        grouped["county_renter_household_total"],
    )

    if not household_available:
        for col in (
            "household_denominator",
            "county_household_total",
            "geo_household_total",
            "household_weight",
        ):
            grouped[col] = pd.NA
    if not renter_available:
        for col in (
            "renter_household_denominator",
            "county_renter_household_total",
            "geo_renter_household_total",
            "renter_household_weight",
        ):
            grouped[col] = pd.NA

    county_weight_sums = grouped.groupby("county_fips", dropna=False)[
        list(WEIGHT_COLUMNS)
    ].transform("sum")
    county_weight_sums = county_weight_sums.rename(
        columns={
            "area_weight": "county_area_coverage_ratio",
            "population_weight": "county_population_coverage_ratio",
            "household_weight": "county_household_coverage_ratio",
            "renter_household_weight": "county_renter_household_coverage_ratio",
        }
    )
    grouped = pd.concat([grouped, county_weight_sums], axis=1)

    grouped["boundary_vintage"] = str(boundary_vintage)
    grouped["county_vintage"] = str(county_vintage)
    grouped["tract_vintage"] = str(tract_vintage)
    grouped["acs_vintage"] = str(acs_vintage) if acs_vintage is not None else pd.NA
    grouped["denominator_source"] = denominator_source
    grouped["denominator_vintage"] = str(resolved_denominator_vintage)
    grouped["county_vintage_semantics"] = COUNTY_VINTAGE_SEMANTICS
    grouped["weighting_method"] = "tract_mediated"

    column_order = [
        geo_id_col if column == "geo_id" else column
        for column in TRACT_MEDIATED_COUNTY_XWALK_COLUMNS
    ]
    grouped = grouped[column_order]
    return grouped.sort_values([geo_id_col, "county_fips"]).reset_index(drop=True)


def save_tract_mediated_county_crosswalk(
    crosswalk: pd.DataFrame,
    *,
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int | None = None,
    denominator_source: str = "acs",
    denominator_vintage: str | int | None = None,
    output_dir: Path | str | None = None,
    geo_type: str = "coc",
) -> Path:
    """Save a tract-mediated county crosswalk with embedded provenance."""
    from hhplab.naming import tract_mediated_county_xwalk_filename

    if output_dir is None:
        output_dir = curated_dir("xwalks")
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    resolved_denominator_vintage = _resolve_denominator_vintage(
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
        acs_vintage=acs_vintage,
    )
    output_path = output_dir / tract_mediated_county_xwalk_filename(
        boundary_vintage,
        county_vintage,
        tract_vintage,
        acs_vintage,
        denominator_source=denominator_source,
        denominator_vintage=resolved_denominator_vintage,
    )
    provenance = ProvenanceBlock(
        boundary_vintage=str(boundary_vintage),
        county_vintage=str(county_vintage),
        tract_vintage=str(tract_vintage),
        acs_vintage=str(acs_vintage) if acs_vintage is not None else None,
        weighting="tract_mediated",
        geo_type=geo_type,
        extra={
            "dataset_type": "tract_mediated_county_crosswalk",
            "denominator_source": denominator_source,
            "denominator_vintage": str(resolved_denominator_vintage),
            "county_vintage_semantics": COUNTY_VINTAGE_SEMANTICS,
            "weight_columns": list(WEIGHT_COLUMNS),
        },
    )
    write_parquet_with_provenance(crosswalk, output_path, provenance)
    return output_path


def summarize_tract_mediated_crosswalk(
    crosswalk: pd.DataFrame,
    selected_weighting_modes: tuple[WeightingMode, ...],
) -> dict[str, object]:
    """Summarize tract-mediated county crosswalk validation diagnostics."""
    selected_columns = [
        WEIGHTING_MODE_TO_COLUMN[mode]
        for mode in selected_weighting_modes
        if WEIGHTING_MODE_TO_COLUMN[mode] in crosswalk.columns
    ]
    available_columns = [col for col in WEIGHT_COLUMNS if col in crosswalk.columns]
    county_count = int(crosswalk["county_fips"].nunique()) if "county_fips" in crosswalk else 0
    summary: dict[str, object] = {
        "county_count": county_count,
        "available_weight_columns": available_columns,
        "selected_weight_columns": selected_columns,
    }
    if "county_area_coverage_ratio" in crosswalk.columns:
        coverage = (
            crosswalk[["county_fips", "county_area_coverage_ratio"]]
            .drop_duplicates("county_fips")
            .dropna()
        )
        summary["min_area_coverage_ratio"] = (
            float(coverage["county_area_coverage_ratio"].min()) if not coverage.empty else None
        )
        summary["full_coverage_count"] = int(
            (coverage["county_area_coverage_ratio"] >= 0.999999).sum()
        )
    if "missing_denominator_tract_count" in crosswalk.columns:
        missing_pairs = crosswalk[crosswalk["missing_denominator_tract_count"] > 0]
        summary["missing_denominator_pair_count"] = int(len(missing_pairs))
        summary["missing_denominator_tract_count"] = int(
            missing_pairs["missing_denominator_tract_count"].sum()
        )
    for column in selected_columns:
        non_null = crosswalk[column].dropna()
        summary[f"{column}_non_null_count"] = int(non_null.shape[0])
        summary[f"{column}_max"] = float(non_null.max()) if not non_null.empty else None
    return summary


def _resolve_denominator_vintage(
    *,
    denominator_source: str,
    denominator_vintage: str | int | None,
    acs_vintage: str | int | None,
) -> str | int:
    if denominator_source == "acs":
        vintage = denominator_vintage if denominator_vintage is not None else acs_vintage
        if vintage is None:
            raise ValueError("ACS tract-mediated denominators require acs_vintage.")
        return vintage
    if denominator_source == "decennial":
        if denominator_vintage is None:
            raise ValueError("Decennial tract-mediated denominators require denominator_vintage.")
        return denominator_vintage
    raise ValueError(
        "Unsupported tract-mediated denominator_source "
        f"{denominator_source!r}; expected 'acs' or 'decennial'."
    )
