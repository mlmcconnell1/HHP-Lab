"""Materialize PRISM monthly rasters to county-level parquet."""

from __future__ import annotations

import tempfile
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask, geometry_window
from rasterio.windows import WindowError
from shapely.geometry import mapping

from hhplab.naming import county_path, prism_county_monthly_path
from hhplab.paths import asset_store_root, raw_root
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.raw_snapshot import hash_zip_contents
from hhplab.sources.prism.ingest import (
    default_prism_monthly_filename,
    get_prism_monthly_source_url,
    validate_prism_monthly_request,
)

PRISM_COUNTY_MONTHLY_BASE_COLUMNS: list[str] = [
    "geo_type",
    "geo_id",
    "county_fips",
    "county_vintage",
    "year",
    "month",
    "date",
    "data_source",
    "metric",
    "source_ref",
    "raw_sha256",
    "ingested_at",
    "raster_total_cell_count",
    "raster_valid_cell_count",
    "raster_nodata_cell_count",
    "raster_coverage_ratio",
]


def prism_county_monthly_columns(variable: str) -> list[str]:
    """Return the output schema for a PRISM county-month artifact."""
    validate_prism_monthly_request(variable, 2000, 1)
    return [
        "geo_type",
        "geo_id",
        "county_fips",
        "county_vintage",
        "year",
        "month",
        "date",
        f"{variable}_c",
        "data_source",
        "metric",
        "source_ref",
        "raw_sha256",
        "ingested_at",
        "raster_total_cell_count",
        "raster_valid_cell_count",
        "raster_nodata_cell_count",
        "raster_coverage_ratio",
    ]


def materialize_prism_monthly_counties(
    variable: str,
    year: int,
    month: int,
    county_vintage: int,
    *,
    raw_zip_path: Path | str | None = None,
    county_geometry_path: Path | str | None = None,
    output_path: Path | str | None = None,
    base_dir: Path | str | None = None,
    all_touched: bool = False,
) -> Path:
    """Compute county zonal means for one PRISM monthly temperature raster."""
    validate_prism_monthly_request(variable, year, month)
    data_root = Path(base_dir) if base_dir is not None else asset_store_root()
    raw_zip = (
        Path(raw_zip_path)
        if raw_zip_path is not None
        else raw_root()
        / "prism"
        / variable
        / "monthly"
        / str(year)
        / default_prism_monthly_filename(variable, year, month)
    )
    counties_path = (
        Path(county_geometry_path)
        if county_geometry_path is not None
        else county_path(county_vintage, data_root)
    )
    out_path = (
        Path(output_path)
        if output_path is not None
        else prism_county_monthly_path(variable, year, month, county_vintage, data_root)
    )

    if not raw_zip.exists():
        raise FileNotFoundError(
            f"PRISM raw ZIP not found at {raw_zip}. Run "
            f"`hhplab ingest prism --variable {variable} --year {year} --month {month}` first."
        )
    if not counties_path.exists():
        raise FileNotFoundError(
            f"County geometry artifact not found at {counties_path}. Run "
            f"`hhplab ingest tiger --year {county_vintage} --type counties` first."
        )

    counties = gpd.read_parquet(counties_path)
    if counties.crs is None:
        raise ValueError(f"County geometry artifact {counties_path} has no CRS.")
    raw_sha256 = hash_zip_contents(raw_zip.read_bytes())
    source_url = get_prism_monthly_source_url(variable, year, month)

    with tempfile.TemporaryDirectory() as tmpdir:
        raster_path = _extract_raster(raw_zip, Path(tmpdir))
        with rasterio.open(raster_path) as src:
            if src.crs is None:
                raise ValueError(f"PRISM raster {raster_path} has no CRS.")
            counties_for_raster = counties.to_crs(src.crs)
            rows = _county_zonal_rows(
                counties_for_raster,
                src,
                variable=variable,
                year=year,
                month=month,
                county_vintage=county_vintage,
                source_url=source_url,
                raw_sha256=raw_sha256,
                all_touched=all_touched,
            )

    df = pd.DataFrame(rows, columns=prism_county_monthly_columns(variable))
    provenance = ProvenanceBlock(
        county_vintage=str(county_vintage),
        geo_type="county",
        extra={
            "dataset": "prism_county_monthly",
            "provider": "prism",
            "product": "temperature",
            "variable": variable,
            "year": year,
            "month": month,
            "source_url": source_url,
            "raw_path": str(raw_zip),
            "raw_sha256": raw_sha256,
            "county_geometry_path": str(counties_path),
            "row_count": len(df),
            "all_touched": all_touched,
            "output_columns": prism_county_monthly_columns(variable),
        },
    )
    write_parquet_with_provenance(df, out_path, provenance)
    return out_path


def _extract_raster(raw_zip: Path, destination: Path) -> Path:
    with zipfile.ZipFile(raw_zip, "r") as zf:
        members = zf.namelist()
        raster_members = [
            name
            for suffix in (".bil", ".tif", ".tiff")
            for name in members
            if name.lower().endswith(suffix)
        ]
        if not raster_members:
            raise FileNotFoundError(
                f"No .bil, .tif, or .tiff raster found in PRISM ZIP {raw_zip}."
            )
        zf.extractall(destination)
    return destination / raster_members[0]


def _county_zonal_rows(
    counties: gpd.GeoDataFrame,
    src: rasterio.io.DatasetReader,
    *,
    variable: str,
    year: int,
    month: int,
    county_vintage: int,
    source_url: str,
    raw_sha256: str,
    all_touched: bool,
) -> list[dict[str, object]]:
    value_col = f"{variable}_c"
    ingested_at = datetime.now(UTC)
    rows: list[dict[str, object]] = []
    for county in counties.itertuples(index=False):
        geoid = _county_geoid(county)
        stats = _zonal_mean(src, county.geometry, all_touched=all_touched)
        row = {
            "geo_type": "county",
            "geo_id": geoid,
            "county_fips": geoid,
            "county_vintage": county_vintage,
            "year": year,
            "month": month,
            "date": date(year, month, 1),
            value_col: stats["mean"],
            "data_source": "PRISM Climate Group",
            "metric": variable,
            "source_ref": source_url,
            "raw_sha256": raw_sha256,
            "ingested_at": ingested_at,
            "raster_total_cell_count": stats["total_count"],
            "raster_valid_cell_count": stats["valid_count"],
            "raster_nodata_cell_count": stats["nodata_count"],
            "raster_coverage_ratio": stats["coverage_ratio"],
        }
        rows.append(row)
    return sorted(rows, key=lambda row: str(row["county_fips"]))


def _county_geoid(county: object) -> str:
    for attr in ("geoid", "GEOID", "county_fips", "geo_id"):
        value = getattr(county, attr, None)
        if value is not None:
            return str(value).zfill(5)
    raise ValueError("County geometry rows must include geoid, GEOID, county_fips, or geo_id.")


def _zonal_mean(
    src: rasterio.io.DatasetReader,
    geometry: object,
    *,
    all_touched: bool,
) -> dict[str, float | int]:
    try:
        window = geometry_window(src, [mapping(geometry)])
    except WindowError:
        return _empty_stats()
    data = src.read(1, window=window, masked=False)
    if data.size == 0:
        return _empty_stats()

    transform = src.window_transform(window)
    shapes = [mapping(geometry)]
    inside = _geometry_mask(shapes, data.shape, transform, all_touched=all_touched)
    if int(inside.sum()) == 0 and not all_touched:
        inside = _geometry_mask(shapes, data.shape, transform, all_touched=True)
    total_count = int(inside.sum())
    if total_count == 0:
        return _empty_stats()

    nodata = _nodata_mask(data, src.nodata)
    valid = inside & ~nodata
    valid_count = int(valid.sum())
    nodata_count = total_count - valid_count
    mean = float(np.asarray(data, dtype="float64")[valid].mean()) if valid_count else np.nan
    return {
        "mean": mean,
        "total_count": total_count,
        "valid_count": valid_count,
        "nodata_count": nodata_count,
        "coverage_ratio": valid_count / total_count,
    }


def _geometry_mask(
    shapes: list[dict[str, object]],
    out_shape: tuple[int, int],
    transform: object,
    *,
    all_touched: bool,
) -> np.ndarray:
    return geometry_mask(
        shapes,
        out_shape=out_shape,
        transform=transform,
        invert=True,
        all_touched=all_touched,
    )


def _nodata_mask(data: np.ndarray, nodata: float | int | None) -> np.ndarray:
    if nodata is None:
        return (
            np.isnan(data)
            if np.issubdtype(data.dtype, np.floating)
            else np.zeros_like(data, dtype=bool)
        )
    if np.isnan(nodata):
        return np.isnan(data)
    return np.isclose(data, nodata)


def _empty_stats() -> dict[str, float | int]:
    return {
        "mean": np.nan,
        "total_count": 0,
        "valid_count": 0,
        "nodata_count": 0,
        "coverage_ratio": np.nan,
    }
