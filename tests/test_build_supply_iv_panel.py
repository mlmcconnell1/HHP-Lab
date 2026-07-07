"""Tests for the tracked supply-IV panel construction script."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_supply_iv_panel.py"
YEARS = (2015, 2016, 2017, 2018, 2019, 2020, 2022, 2023, 2024, 2025)
MSAS = (
    ("10000", "Alpha, AA", 0),
    ("20000", "Beta, BB", 1),
    ("30000", "Gamma, CC", 0),
)


def _load_builder():
    spec = importlib.util.spec_from_file_location("build_supply_iv_panel", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_base_panel(path: Path) -> None:
    rows = []
    for msa_index, (msa_id, msa_name, _sanctuary) in enumerate(MSAS, start=1):
        for year_index, year in enumerate(YEARS, start=1):
            population = 100_000 + msa_index * 10_000 + year_index * 100
            zori = 1_000 + msa_index * 50 + year_index * 10
            unshelt_per_1000 = 0.2 + msa_index * 0.05 + year_index * 0.01
            rows.append(
                {
                    "msa_id": msa_id,
                    "msa_name": msa_name,
                    "year": year,
                    "pit_unsheltered": unshelt_per_1000 * population / 1000,
                    "pit_sheltered": 100 + msa_index,
                    "pit_total": 150 + msa_index,
                    "zori": zori,
                    "zori_coverage_ratio": 0.9,
                    "population": population,
                    "unshelt_per_1000": unshelt_per_1000,
                    "log_unshelt_per_1000": np.log(unshelt_per_1000),
                    "log_zori": np.log(zori),
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_bps(path: Path) -> None:
    rows = []
    for msa_index, (msa_id, _msa_name, _sanctuary) in enumerate(MSAS[:2], start=1):
        for year in range(2010, 2015):
            rows.append(
                {
                    "msa_id": msa_id,
                    "year": year,
                    "permitted_units": 100 + msa_index * 10 + year - 2010,
                    "population_weight_denominator": 100_000 + msa_index * 10_000,
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_zori(path: Path) -> None:
    rows = []
    for msa_index, (msa_id, _msa_name, _sanctuary) in enumerate(MSAS, start=1):
        for year_index, year in enumerate(YEARS, start=1):
            rows.append(
                {
                    "msa_id": msa_id,
                    "year": year,
                    "zori": 1_000 + msa_index * 25 + year_index * 5,
                    "total_population": 100_000 + msa_index * 20_000,
                }
            )
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_sanctuary(path: Path) -> None:
    rows = [
        {"msa_id": msa_id, "doj_sanctuary_msa": sanctuary}
        for msa_id, _msa_name, sanctuary in MSAS
    ]
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_top150_style_builder_writes_completecase_manifest(tmp_path: Path) -> None:
    builder = _load_builder()
    base_path = tmp_path / "base.parquet"
    bps_path = tmp_path / "bps.parquet"
    zori_path = tmp_path / "zori.parquet"
    sanctuary_path = tmp_path / "sanctuary.parquet"
    out_dir = tmp_path / "supply_iv"
    _write_base_panel(base_path)
    _write_bps(bps_path)
    _write_zori(zori_path)
    _write_sanctuary(sanctuary_path)

    paths = builder.InputPaths(
        bps_msa=bps_path,
        zori_msa=zori_path,
        sanctuary_panel=sanctuary_path,
    )
    spec = builder.CohortSpec(
        name="top150_test",
        requested_msa_count=3,
        base_panel=base_path,
        output_prefix="top150_msa_supply_iv",
        complete_case_exclusions=("30000",),
    )

    fd, longdiff, manifest = builder.build_supply_iv_panel(
        spec,
        paths=paths,
        out_dir=out_dir,
        suffix="_completecase",
        exclude_msa_ids=spec.complete_case_exclusions,
    )

    assert len(fd) == 16
    assert len(longdiff) == 2
    assert manifest["included_msa_count"] == 2
    assert manifest["excluded_missing_bps_2010_2014"] == [
        {"msa_id": "30000", "msa_name": "Gamma, CC"}
    ]
    assert (out_dir / "top150_msa_supply_iv_fd_completecase.parquet").exists()
    assert (out_dir / "top150_msa_supply_iv_longdiff_completecase.parquet").exists()
    manifest_path = out_dir / "top150_msa_supply_iv_completecase_manifest.json"
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert longdiff.set_index("msa_id").loc["20000", "sanctuary"] == 1
