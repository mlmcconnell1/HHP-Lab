"""Status report coordination and stable top-level payload assembly."""

from __future__ import annotations

from pathlib import Path

from hhplab.sources.census.census.api import (
    CENSUS_API_KEY_MISSING_MESSAGE,
    census_api_credentials_status,
    probe_census_api_reachability,
)
from hhplab.storage.config import load_config

from .contracts import STATUS_GUIDANCE
from .geography import scan_boundaries, scan_census, scan_crosswalks, scan_metro, scan_msa
from .homelessness import scan_hic, scan_pit
from .prerequisites import check_prerequisites
from .recipes import scan_recipe_outputs
from .sources import scan_acs, scan_laus, scan_measures, scan_medsl, scan_zori

__all__ = ["collect_status_report"]


def collect_status_report(
    *,
    data_dir: Path,
    output_root: Path | None = None,
    project_root: Path | None = None,
    probe_census_api: bool = False,
) -> dict:
    """Scan configured storage roots and return the stable status payload."""
    curated = data_dir / "curated"
    storage_cfg = load_config(
        asset_store_root=data_dir,
        output_root=output_root,
        project_root=project_root or Path.cwd(),
    )
    assets = {
        "boundaries": scan_boundaries(curated),
        "census": scan_census(curated),
        "crosswalks": scan_crosswalks(curated),
        "pit": scan_pit(curated),
        "hic": scan_hic(curated),
        "metro": scan_metro(curated),
        "msa": scan_msa(curated),
        "measures": scan_measures(curated),
        "acs": scan_acs(curated),
        "zori": scan_zori(curated),
        "laus": scan_laus(curated),
        "medsl": scan_medsl(data_dir, curated),
    }
    census_credentials = census_api_credentials_status()
    credentials = {"census_api_key": census_credentials}
    if probe_census_api:
        census_credentials["reachability"] = probe_census_api_reachability()

    recipe_outputs = scan_recipe_outputs(storage_cfg.output_root)
    issues = check_prerequisites(assets)
    if assets["acs"]["schema_staleness_count"]:
        issues.append(
            {
                "severity": "warning",
                "area": "curated_schema",
                "message": (
                    f"{assets['acs']['schema_staleness_count']} curated ACS artifact(s) "
                    "have stale schemas."
                ),
                "hint": "Run `hhplab validate curated-layout --json` for rebuild commands.",
            }
        )
    if not census_credentials["present"]:
        issues.append(
            {
                "severity": "error",
                "area": "credentials",
                "message": CENSUS_API_KEY_MISSING_MESSAGE,
                "hint": STATUS_GUIDANCE["census_api_key"],
            }
        )
    has_errors = any(issue["severity"] == "error" for issue in issues)
    health = "degraded" if has_errors else ("healthy" if not issues else "ok")
    return {
        "status": health,
        "credentials": credentials,
        "assets": assets,
        "recipe_outputs": recipe_outputs,
        "guidance": STATUS_GUIDANCE,
        "issues": issues,
    }
