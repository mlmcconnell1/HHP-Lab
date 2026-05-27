"""MSA-CoC containment panel assembly for recipe execution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import geopandas as gpd
import pandas as pd

from hhplab.msa.selectors import select_top_msa_ids_by_population
from hhplab.msa.unemployment import resolve_msa_unemployment
from hhplab.recipe.executor_containment import build_msa_coc_membership
from hhplab.recipe.executor_core import ExecutionContext, ExecutorError
from hhplab.recipe.planner import ExecutionPlan
from hhplab.recipe.recipe_schema import ContainmentSpec
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec
from hhplab.schema.columns import MSA_COC_PANEL_COLUMNS


@dataclass(frozen=True)
class MsaCocPanelAssembly:
    """Assembled MSA-CoC panel plus provenance-ready diagnostics."""

    panel: pd.DataFrame
    provenance: dict[str, object]


@dataclass(frozen=True)
class _FrameRecord:
    dataset_id: str
    source_token: str
    geo_type: str
    year: int
    frame: pd.DataFrame


def build_msa_coc_containment_spec(
    panel_spec: Any,
    *,
    selector_ids: list[str] | None = None,
) -> ContainmentSpec:
    """Translate ``MsaCocPanelSpec`` into the containment builder contract."""
    county_vintage = _county_vintage_from_msa_definition_version(
        panel_spec.msa_definition_version
    )
    return ContainmentSpec(
        container=GeometryRef(
            type="msa",
            vintage=county_vintage,
            source=panel_spec.msa_definition_version,
        ),
        candidate=GeometryRef(
            type="coc",
            vintage=panel_spec.coc_boundary_vintage,
        ),
        min_share=panel_spec.containment_min_share,
        denominator=panel_spec.containment_denominator,
        definition_version=panel_spec.msa_definition_version,
        selector_ids=selector_ids,
    )


def assemble_msa_coc_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
    *,
    target: Any,
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
) -> MsaCocPanelAssembly:
    """Assemble one row per ``msa_id x coc_id x year`` for an MSA-CoC target."""
    panel_spec = target.msa_coc_panel
    if panel_spec is None:
        raise ExecutorError(f"Target '{target.id}' does not declare msa_coc_panel.")

    containment_spec = build_msa_coc_containment_spec(panel_spec)
    records = _collect_frame_records(plan, ctx)
    universe_years = expand_year_spec(ctx.recipe.universe)
    ranking_frame = _source_year_frame(
        records,
        source_token=panel_spec.ranking_population_source,
        geo_type="msa",
        year=panel_spec.ranking_reference_year,
        purpose="MSA ranking population",
    )
    ranking_column = _population_column(ranking_frame, panel_spec.ranking_population_source)
    selection = select_top_msa_ids_by_population(
        ranking_frame,
        msa_county_membership,
        ranking_source=panel_spec.ranking_population_source,
        reference_year=panel_spec.ranking_reference_year,
        top_n=panel_spec.top_n,
        population_column=ranking_column,
    )
    containment_spec = build_msa_coc_containment_spec(
        panel_spec,
        selector_ids=list(selection.selector_ids),
    )
    membership = build_msa_coc_membership(
        containment_spec,
        coc_gdf=coc_gdf,
        county_gdf=county_gdf,
        msa_county_membership=msa_county_membership,
    )
    if membership.empty:
        raise ExecutorError(
            "MSA-CoC panel membership is empty after applying top-N selector "
            f"and containment_min_share={panel_spec.containment_min_share}."
        )

    year_frames: list[pd.DataFrame] = []
    for year in universe_years:
        base = membership[["msa_id", "coc_id"]].copy()
        base["year"] = year

        msa_covariates = _msa_covariates_for_year(records, year=year, panel_spec=panel_spec)
        unemployment = _unemployment_for_year(records, year=year, panel_spec=panel_spec)
        coc_measures = _coc_measures_for_year(records, year=year)

        frame = base.merge(msa_covariates, on=["msa_id", "year"], how="left")
        frame = frame.merge(unemployment, on=["msa_id", "year"], how="left")
        frame = frame.merge(coc_measures, on=["coc_id", "year"], how="left")
        year_frames.append(frame)

    panel = pd.concat(year_frames, ignore_index=True)
    panel = _add_msa_coc_metadata(panel, panel_spec)
    panel = _apply_output_aliases(panel, panel_spec.output_aliases)
    panel = _order_columns(panel)

    provenance = {
        "msa_coc_panel": {
            "row_count": len(panel),
            "membership_row_count": len(membership),
            "top_n": panel_spec.top_n,
            "selected_msa_ids": list(selection.selector_ids),
            "ranking_diagnostics": selection.diagnostics.to_dict(),
            "msa_population_source": panel_spec.msa_population_source,
            "unemployment_source": panel_spec.unemployment_source,
            "output_aliases": dict(panel_spec.output_aliases),
        },
        "containment_spec": containment_spec.model_dump(mode="json"),
    }
    return MsaCocPanelAssembly(panel=panel, provenance=provenance)


def _collect_frame_records(plan: ExecutionPlan, ctx: ExecutionContext) -> list[_FrameRecord]:
    records: list[_FrameRecord] = []
    for task in plan.resample_tasks:
        key = (task.dataset_id, task.year)
        if key not in ctx.intermediates:
            continue
        ds = ctx.recipe.datasets[task.dataset_id]
        geo_type = task.to_geometry.type
        source_token = _source_token(ds.provider, ds.product)
        records.append(
            _FrameRecord(
                dataset_id=task.dataset_id,
                source_token=source_token,
                geo_type=geo_type,
                year=task.year,
                frame=_normalize_geo_frame(ctx.intermediates[key], geo_type=geo_type),
            )
        )

    for year in expand_year_spec(ctx.recipe.universe):
        joined = ctx.intermediates.get(("__joined__", year))
        if joined is not None:
            records.append(
                _FrameRecord(
                    dataset_id="__joined__",
                    source_token="joined",
                    geo_type="coc",
                    year=year,
                    frame=_normalize_geo_frame(joined, geo_type="coc"),
                )
            )
    return records


def _normalize_geo_frame(frame: pd.DataFrame, *, geo_type: str) -> pd.DataFrame:
    normalized = frame.copy()
    target_col = _geo_column_for_type(geo_type)
    if target_col not in normalized.columns and "geo_id" in normalized.columns:
        normalized = normalized.rename(columns={"geo_id": target_col})
    if target_col in normalized.columns:
        raw_id = normalized[target_col].astype(str)
        normalized[target_col] = raw_id.mask(raw_id.str.fullmatch(r"\d+"), raw_id.str.zfill(5))
    if "year" in normalized.columns:
        normalized["year"] = pd.to_numeric(normalized["year"], errors="raise").astype(int)
    return normalized


def _source_year_frame(
    records: list[_FrameRecord],
    *,
    source_token: str,
    geo_type: str,
    year: int,
    purpose: str,
) -> pd.DataFrame:
    for record in records:
        if (
            record.source_token == source_token
            and record.geo_type == geo_type
            and record.year == year
        ):
            return record.frame
    raise ExecutorError(
        f"No {purpose} frame found for source '{source_token}', "
        f"geometry '{geo_type}', year {year}. Add a recipe resample step "
        "that materializes this source at MSA geometry."
    )


def _msa_covariates_for_year(
    records: list[_FrameRecord],
    *,
    year: int,
    panel_spec: Any,
) -> pd.DataFrame:
    acs5 = _source_year_frame(
        records,
        source_token="acs5",
        geo_type="msa",
        year=year,
        purpose="ACS5 MSA covariates",
    )
    population = _source_year_frame(
        records,
        source_token=panel_spec.msa_population_source,
        geo_type="msa",
        year=year,
        purpose="MSA population",
    )

    result = acs5[["msa_id", "year"]].copy()
    result["msa_median_rent"] = _first_available(acs5, ["msa_median_rent", "median_gross_rent"])
    result["msa_vacancy_rate"] = _first_available(acs5, ["msa_vacancy_rate", "vacancy_rate"])
    result["msa_poverty_rate"] = _first_available(acs5, ["msa_poverty_rate", "poverty_rate"])
    result["msa_income"] = _first_available(acs5, ["msa_income", "median_household_income"])
    result["msa_rent_burden"] = _first_available(acs5, ["msa_rent_burden", "rent_burden_30_plus"])
    population_column = _population_column(population, panel_spec.msa_population_source)
    population_values = population[["msa_id", "year", population_column]].rename(
        columns={population_column: "msa_population"}
    )
    result = result.merge(population_values, on=["msa_id", "year"], how="left")
    result[f"msa_population_{panel_spec.msa_population_source}"] = result["msa_population"]
    return result


def _unemployment_for_year(
    records: list[_FrameRecord],
    *,
    year: int,
    panel_spec: Any,
) -> pd.DataFrame:
    if panel_spec.unemployment_source == "acs5":
        source = _source_year_frame(
            records,
            source_token="acs5",
            geo_type="msa",
            year=year,
            purpose="ACS5 MSA unemployment",
        )
        return resolve_msa_unemployment("acs5", acs5_msa=source, years=[year])

    source = _source_year_frame(
        records,
        source_token="laus",
        geo_type="msa",
        year=year,
        purpose="BLS LAUS MSA unemployment",
    )
    return resolve_msa_unemployment("laus", laus_msa=source, years=[year])


def _coc_measures_for_year(records: list[_FrameRecord], *, year: int) -> pd.DataFrame:
    coc_frames = [
        record.frame
        for record in records
        if record.geo_type == "coc" and record.year == year and "coc_id" in record.frame.columns
    ]
    if not coc_frames:
        raise ExecutorError(
            f"No CoC-year measures found for MSA-CoC panel year {year}. "
            "Add recipe steps that join PIT counts and CoC population at CoC geometry."
        )

    merged = coc_frames[0]
    for frame in coc_frames[1:]:
        extra_cols = [
            column
            for column in frame.columns
            if column not in merged.columns or column in {"coc_id", "year"}
        ]
        merged = merged.merge(frame[extra_cols], on=["coc_id", "year"], how="outer")

    result = merged[["coc_id", "year"]].copy()
    for column in ("pit_total", "pit_sheltered", "pit_unsheltered"):
        result[column] = _first_available(merged, [column])
    result["coc_population"] = _first_available(
        merged,
        [
            "coc_population",
            "total_population",
            "total_population_acs5",
            "total_population_pep",
            "population",
        ],
    )
    return result


def _add_msa_coc_metadata(panel: pd.DataFrame, panel_spec: Any) -> pd.DataFrame:
    result = panel.copy()
    result["ranking_population_source"] = panel_spec.ranking_population_source
    result["ranking_reference_year"] = panel_spec.ranking_reference_year
    result["containment_min_share"] = panel_spec.containment_min_share
    result["containment_denominator"] = panel_spec.containment_denominator
    result["coc_boundary_vintage_used"] = panel_spec.coc_boundary_vintage
    result["msa_definition_version_used"] = panel_spec.msa_definition_version
    result["msa_population_source"] = panel_spec.msa_population_source
    result["unemployment_source"] = panel_spec.unemployment_source
    result["source"] = "msa_coc_panel"
    return result


def _apply_output_aliases(panel: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    result = panel.copy()
    for source, alias in aliases.items():
        if source not in result.columns:
            raise ExecutorError(
                f"MSA-CoC panel output_aliases references missing column '{source}'. "
                f"Available columns: {list(result.columns)}"
            )
        if alias != source:
            result[alias] = result[source]
    return result


def _order_columns(panel: pd.DataFrame) -> pd.DataFrame:
    required = list(MSA_COC_PANEL_COLUMNS)
    missing = [column for column in required if column not in panel.columns]
    if missing:
        raise ExecutorError(f"MSA-CoC panel is missing required columns: {missing}")
    extra = [column for column in panel.columns if column not in required]
    return panel[required + extra].sort_values(["msa_id", "coc_id", "year"]).reset_index(drop=True)


def _population_column(frame: pd.DataFrame, source_token: str) -> str:
    candidates = (
        ["population", "total_population", f"total_population_{source_token}"]
        if source_token == "pep"
        else ["total_population", "population", f"total_population_{source_token}"]
    )
    for column in candidates:
        if column in frame.columns:
            return column
    raise ExecutorError(
        f"MSA population source '{source_token}' is missing a population column. "
        f"Expected one of {candidates}; available columns: {list(frame.columns)}"
    )


def _first_available(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            return pd.to_numeric(frame[column], errors="coerce").astype("Float64")
    return pd.Series(pd.NA, index=frame.index, dtype="Float64")


def _source_token(provider: str, product: str) -> str:
    if provider == "census" and product in {"acs", "acs5"}:
        return "acs5"
    if provider == "census" and product == "pep":
        return "pep"
    if provider == "bls" and product == "laus":
        return "laus"
    if provider == "hud" and product == "pit":
        return "pit"
    return product


def _geo_column_for_type(geo_type: str) -> str:
    return {
        "coc": "coc_id",
        "msa": "msa_id",
        "metro": "msa_id",
    }.get(geo_type, "geo_id")


def _county_vintage_from_msa_definition_version(definition_version: str) -> int:
    matches = re.findall(r"(?:19|20)\d{2}", definition_version)
    if not matches:
        raise ExecutorError(
            "MSA-CoC panel requires an MSA definition version containing a county "
            "geometry year, for example 'census_msa_2023'."
        )
    return int(matches[-1])
