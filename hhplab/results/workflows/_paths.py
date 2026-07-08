"""Shared repository-relative paths for result workflow modules."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = REPO_ROOT / "data"
OUTPUTS_ROOT = REPO_ROOT / "outputs"

__all__ = ["DATA_ROOT", "OUTPUTS_ROOT", "REPO_ROOT"]
