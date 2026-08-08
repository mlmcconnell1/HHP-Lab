"""Focused contracts for the bounded status scanners."""

from pathlib import Path

import pytest

from hhplab.diagnostics.status.contracts import (
    ASSET_PAYLOAD_KEYS,
    STATUS_PAYLOAD_KEYS,
)
from hhplab.diagnostics.status.geography import scan_crosswalks
from hhplab.diagnostics.status.homelessness import scan_pit
from hhplab.diagnostics.status.recipes import scan_recipe_outputs


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("xwalk__B2025xT2020.parquet", "B2025xT2020"),
        ("xwalk__B2025xC2020.parquet", "B2025xC2020"),
        ("coc_tract_xwalk__2025__2020.parquet", "B2025xT2020"),
        ("coc_county_xwalk__2025.parquet", "B2025"),
    ],
    ids=["tract", "county", "legacy-tract", "legacy-county"],
)
def test_crosswalk_scanner_preserves_filename_contract(
    tmp_path: Path,
    filename: str,
    expected: str,
) -> None:
    directory = tmp_path / "curated" / "xwalks"
    directory.mkdir(parents=True)
    (directory / filename).touch()

    result = scan_crosswalks(tmp_path / "curated")

    values = result["tract"] + result["county"]
    assert expected in values


def test_pit_scanner_deduplicates_boundary_scoped_years(tmp_path: Path) -> None:
    directory = tmp_path / "curated" / "pit"
    directory.mkdir(parents=True)
    for filename in (
        "pit__P2024.parquet",
        "pit__P2024@B2024.parquet",
        "pit__P2024@B2025.parquet",
        "pit__P2025.parquet",
    ):
        (directory / filename).touch()

    assert scan_pit(tmp_path / "curated")["years"] == [2024, 2025]


def test_recipe_scanner_ignores_empty_namespaces(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    (root / "empty").mkdir(parents=True)
    populated = root / "recipe-a"
    populated.mkdir()
    (populated / "panel__Y2020-2021@B2025.parquet").touch()

    result = scan_recipe_outputs(root)

    assert result["count"] == 1
    assert result["recipes"][0]["name"] == "recipe-a"
    assert result["panel_count"] == 1


def test_status_payload_contract_keys_are_canonical() -> None:
    assert STATUS_PAYLOAD_KEYS == (
        "status",
        "credentials",
        "assets",
        "recipe_outputs",
        "guidance",
        "issues",
    )
    assert ASSET_PAYLOAD_KEYS == (
        "boundaries",
        "census",
        "crosswalks",
        "pit",
        "hic",
        "metro",
        "msa",
        "measures",
        "acs",
        "zori",
        "laus",
        "medsl",
    )
