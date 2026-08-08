"""Stable payload keys and user guidance for the status report."""

from __future__ import annotations

STATUS_GUIDANCE = {
    "recipe_preflight": "hhplab build recipe-preflight --recipe <file> --json",
    "recipe_execute": "hhplab build recipe --recipe <file> --json",
    "census_api_key": (
        "Get a free key at https://api.census.gov/data/key_signup.html and export CENSUS_API_KEY."
    ),
}

STATUS_PAYLOAD_KEYS = (
    "status",
    "credentials",
    "assets",
    "recipe_outputs",
    "guidance",
    "issues",
)
ASSET_PAYLOAD_KEYS = (
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

__all__ = ["ASSET_PAYLOAD_KEYS", "STATUS_GUIDANCE", "STATUS_PAYLOAD_KEYS"]
