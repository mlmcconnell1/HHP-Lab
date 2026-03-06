"""Offline end-to-end smoke test using a committed synthetic fixture."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from typer.testing import CliRunner

from coclab.acs.ingest.tract_population import get_output_path
from coclab.cli.main import app
from coclab.naming import county_filename, pit_filename, tract_filename
from coclab.panel import AlignmentPolicy, build_panel, save_panel
from coclab.provenance import read_provenance
from coclab.registry.registry import register_vintage

runner = CliRunner()

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "offline_smoke"


def _same_year(year: int) -> str:
    return str(year)


def _load_wkt_csv(path: Path, id_column: str) -> gpd.GeoDataFrame:
    """Load a WKT CSV fixture into a GeoDataFrame."""
    df = pd.read_csv(path, dtype={id_column: str})
    geometry = gpd.GeoSeries.from_wkt(df.pop("wkt"), crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")


def _seed_fixture_data() -> None:
    """Seed curated inputs from committed fixture files."""
    from coclab.geo.normalize import normalize_boundaries

    # Boundary fixture + registry entry
    boundaries = _load_wkt_csv(FIXTURE_DIR / "boundaries.csv", "coc_id")
    boundaries = normalize_boundaries(boundaries)
    boundary_path = Path("data/curated/coc_boundaries/coc__B2024.parquet")
    boundary_path.parent.mkdir(parents=True, exist_ok=True)
    boundaries.to_parquet(boundary_path, index=False)

    register_vintage(
        boundary_vintage="2024",
        source="fixture",
        path=boundary_path,
        feature_count=len(boundaries),
        _allow_temp_path=True,
    )

    # TIGER-like geometry fixtures
    tracts = _load_wkt_csv(FIXTURE_DIR / "tracts.csv", "GEOID")
    counties = _load_wkt_csv(FIXTURE_DIR / "counties.csv", "GEOID")

    tiger_dir = Path("data/curated/tiger")
    tiger_dir.mkdir(parents=True, exist_ok=True)
    tracts.to_parquet(tiger_dir / tract_filename(2020), index=False)
    counties.to_parquet(tiger_dir / county_filename(2020), index=False)

    # ACS tract fixture (cached ingest location)
    acs_path = get_output_path("2020-2024", "2020")
    acs_path.parent.mkdir(parents=True, exist_ok=True)
    acs_df = pd.read_csv(FIXTURE_DIR / "acs_2020_2024_tracts_2020.csv", dtype={"tract_geoid": str})
    acs_df.to_parquet(acs_path, index=False)

    # PIT fixture (canonical curated PIT location)
    pit_dir = Path("data/curated/pit")
    pit_dir.mkdir(parents=True, exist_ok=True)
    pit_df = pd.read_csv(FIXTURE_DIR / "pit_2024.csv", dtype={"coc_id": str})
    pit_df.to_parquet(pit_dir / pit_filename(2024), index=False)


def test_offline_fixture_full_pipeline(tmp_path: Path) -> None:
    """Run fixture-based ingest -> xwalk -> aggregate -> panel pipeline fully offline."""
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # Suppress root warning prompt by ensuring expected project markers exist.
        Path("coclab").mkdir(parents=True, exist_ok=True)
        Path("pyproject.toml").write_text("[project]\nname='offline-smoke'\nversion='0.0.0'\n")

        _seed_fixture_data()

        create = runner.invoke(
            app,
            ["build", "create", "--name", "offline", "--years", "2024"],
        )
        assert create.exit_code == 0, create.output

        xwalk = runner.invoke(
            app,
            [
                "generate",
                "xwalks",
                "--build",
                "offline",
                "--boundary",
                "2024",
                "--tracts",
                "2020",
                "--counties",
                "2020",
                "--type",
                "all",
            ],
        )
        assert xwalk.exit_code == 0, xwalk.output

        aggregate_acs = runner.invoke(
            app,
            [
                "aggregate",
                "acs",
                "--build",
                "offline",
                "--years",
                "2024",
                "--tracts",
                "2020",
                "--weighting",
                "area",
            ],
        )
        assert aggregate_acs.exit_code == 0, aggregate_acs.output

        aggregate_pit = runner.invoke(
            app,
            [
                "aggregate",
                "pit",
                "--build",
                "offline",
                "--years",
                "2024",
            ],
        )
        assert aggregate_pit.exit_code == 0, aggregate_pit.output

        build_curated = Path("builds/offline/data/curated")
        tract_xwalk = build_curated / "xwalks" / "xwalk__B2024xT2020.parquet"
        county_xwalk = build_curated / "xwalks" / "xwalk__B2024xC2020.parquet"
        measures = build_curated / "measures" / "measures__A2024@B2024xT2020.parquet"
        pit_agg = build_curated / "pit" / "pit__P2024@B2024.parquet"

        assert tract_xwalk.exists()
        assert county_xwalk.exists()
        assert measures.exists()
        assert pit_agg.exists()

        measures_prov = read_provenance(measures)
        assert measures_prov is not None
        assert measures_prov.boundary_vintage == "2024"
        assert measures_prov.acs_vintage == "2020-2024"

        # Panel step: use build-local measures and canonical PIT fixture input.
        policy = AlignmentPolicy(
            boundary_vintage_func=_same_year,
            acs_vintage_func=_same_year,
            weighting_method="area",
        )
        panel_df = build_panel(
            2024,
            2024,
            policy=policy,
            pit_dir=Path("data/curated/pit"),
            measures_dir=build_curated / "measures",
        )
        assert not panel_df.empty
        assert set(panel_df["coc_id"]) == {"CO-500", "CO-501"}

        panel_path = save_panel(
            panel_df,
            start_year=2024,
            end_year=2024,
            output_dir=build_curated / "panel",
            policy=policy,
        )
        assert panel_path.exists()

        panel_prov = read_provenance(panel_path)
        assert panel_prov is not None
        assert panel_prov.boundary_vintage == "2024"
