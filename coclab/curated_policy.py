"""Curated data layout policy enforcement.

Validates the ``data/curated/`` directory tree against canonical naming
and structure rules defined in devdocs/curated-policy-schema-plan.md.

Checks:
- Non-canonical filenames within known artifact subdirectories
- Nested subdirectories under artifact folders (curated dirs must be flat)
- Unknown top-level subdirectories under ``data/curated/``
- Unexpected files at the curated root level
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from coclab.paths import curated_root


@dataclass
class CuratedViolation:
    """A single curated layout policy violation."""

    path: Path
    category: str  # "non_canonical", "nested_path", "unknown_subdir"
    message: str


# ---- Known curated subdirectories ----

CURATED_SUBDIRS: set[str] = {
    "coc_boundaries",
    "tiger",
    "xwalks",
    "acs",
    "measures",
    "zori",
    "pep",
    "pit",
    "panel",
    "metro",
    "maps",
}

# ---- Canonical filename patterns per subdirectory ----

CANONICAL_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "coc_boundaries": [
        re.compile(r"^coc__B\d{4}\.parquet$"),
        re.compile(r"^boundaries__B\d{4}\.parquet$"),  # legacy but recognised
    ],
    "tiger": [
        re.compile(r"^tracts__T\d{4}\.parquet$"),
        re.compile(r"^counties__C\d{4}\.parquet$"),
        re.compile(r"^tract_relationship__T\d{4}xT\d{4}\.parquet$"),
    ],
    "xwalks": [
        re.compile(r"^xwalk__B\d{4}xT\d{4}\.parquet$"),
        re.compile(r"^xwalk__B\d{4}xC\d{4}\.parquet$"),
    ],
    "acs": [
        re.compile(r"^acs5_tracts__A\d{4}xT\d{4}\.parquet$"),
        re.compile(r"^acs1_metro__A\d{4}@D\w+\.parquet$"),
        re.compile(r"^county_weights__A\d{4}__w\w+\.parquet$"),
    ],
    "measures": [
        re.compile(r"^measures__A\d{4}(\(\d{4}\))?@B\d{4}(xT\d{4})?\.parquet$"),
        re.compile(r"^measures__metro__A\d{4}@D\w+(xT\d{4})?\.parquet$"),
        re.compile(r"^measures__metro__acs1__A\d{4}@D\w+\.parquet$"),
    ],
    "zori": [
        re.compile(r"^zori__A\d{4}@B\d{4}xC\d{4}__w\w+\.parquet$"),
        re.compile(r"^zori_yearly__A\d{4}@B\d{4}xC\d{4}__w\w+__m\w+\.parquet$"),
        re.compile(r"^zori__\w+__Z\d{4}\.parquet$"),  # ingest files
        re.compile(r"^zori__metro__A\d{4}@D\w+xC\d{4}__w\w+\.parquet$"),
    ],
    "pep": [
        re.compile(r"^pep_county__v\d{4}\.parquet$"),
        re.compile(r"^pep_county__v\d{4}__y\d{4}-\d{4}\.parquet$"),
        re.compile(r"^coc_pep__B\d{4}xC\d{4}__w\w+__\d{4}_\d{4}\.parquet$"),
        re.compile(r"^pep__metro__D\w+xC\d{4}__w\w+__\d{4}_\d{4}\.parquet$"),
    ],
    "pit": [
        re.compile(r"^pit__P\d{4}(@B\d{4})?\.parquet$"),
        re.compile(r"^pit__metro__P\d{4}@D\w+\.parquet$"),
        re.compile(r"^pit_vintage__P\d{4}\.parquet$"),
        re.compile(r"^pit_vintage_registry\.parquet$"),
        re.compile(r"^pit_registry\.parquet$"),
    ],
    "panel": [
        re.compile(r"^panel__Y\d{4}-\d{4}@B\d{4}\.parquet$"),
        re.compile(r"^panel__metro__Y\d{4}-\d{4}@D\w+\.parquet$"),
        # Sidecar files generated alongside panels
        re.compile(r"^panel__.+\.manifest\.json$"),
        re.compile(r"^panel__.+__diagnostics\.json$"),
    ],
    "metro": [
        re.compile(r"^metro_definitions__\w+\.parquet$"),
        re.compile(r"^metro_coc_membership__\w+\.parquet$"),
        re.compile(r"^metro_county_membership__\w+\.parquet$"),
    ],
    "maps": [
        re.compile(r"^.+\.html$"),
    ],
}

# Files to always ignore everywhere
IGNORED_FILES: set[str] = {".DS_Store"}

# Patterns for files acceptable at the curated root level
IGNORED_ROOT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\w+_registry\.parquet$"),  # any registry file
]


def validate_curated_layout(
    base_dir: Path | None = None,
) -> list[CuratedViolation]:
    """Validate the curated data directory for policy violations.

    Parameters
    ----------
    base_dir : Path or None
        Path to the curated data directory.  Defaults to ``data/curated``.

    Returns
    -------
    list[CuratedViolation]
        All detected violations, sorted by path.
    """
    if base_dir is None:
        base_dir = curated_root()

    violations: list[CuratedViolation] = []

    if not base_dir.is_dir():
        return violations

    for entry in sorted(base_dir.iterdir()):
        # --- Files at the curated root ---
        if not entry.is_dir():
            name = entry.name
            if name in IGNORED_FILES:
                continue
            if any(p.match(name) for p in IGNORED_ROOT_PATTERNS):
                continue
            violations.append(
                CuratedViolation(
                    path=entry,
                    category="non_canonical",
                    message=f"Unexpected file at curated root: {name}",
                )
            )
            continue

        # --- Subdirectories ---
        subdir_name = entry.name
        if subdir_name.startswith("."):
            continue

        if subdir_name not in CURATED_SUBDIRS:
            violations.append(
                CuratedViolation(
                    path=entry,
                    category="unknown_subdir",
                    message=f"Unknown curated subdirectory: {subdir_name}",
                )
            )
            continue

        patterns = CANONICAL_PATTERNS.get(subdir_name, [])

        # Walk all items to detect nested paths and non-canonical names
        for item in sorted(entry.rglob("*")):
            if item.is_dir():
                # Flag any nested subdirectory (curated folders must be flat)
                rel = item.relative_to(entry)
                violations.append(
                    CuratedViolation(
                        path=item,
                        category="nested_path",
                        message=f"Nested directory under {subdir_name}/: {rel}",
                    )
                )
                continue

            rel = item.relative_to(entry)

            # Skip files in nested directories — parent dir violation covers them
            if len(rel.parts) > 1:
                continue

            name = item.name
            if name in IGNORED_FILES:
                continue

            # Check against canonical patterns
            if patterns and not any(p.match(name) for p in patterns):
                violations.append(
                    CuratedViolation(
                        path=item,
                        category="non_canonical",
                        message=f"Non-canonical filename in {subdir_name}/: {name}",
                    )
                )

    return violations
