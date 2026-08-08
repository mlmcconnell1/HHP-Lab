"""Curated data migration utility for rename and dedupe.

Scans curated artifact directories, proposes canonical renames for
legacy-named files, detects duplicate temporal identities, and can
apply changes deterministically.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from hhplab.storage.paths import curated_root


@dataclass
class MigrationAction:
    """A proposed rename or conflict."""

    source: Path
    target: Path | None  # None if no canonical equivalent can be determined
    action: str  # "rename", "duplicate", "unknown"
    message: str


@dataclass
class MigrationPlan:
    """Result of scanning curated dirs for migration candidates."""

    renames: list[MigrationAction] = field(default_factory=list)
    duplicates: list[MigrationAction] = field(default_factory=list)
    unknown: list[MigrationAction] = field(default_factory=list)


# ---- Legacy -> Canonical rename rules ----
# Each rule: (subdir, legacy_pattern, canonical_builder)

_LEGACY_RULES: list[tuple[str, re.Pattern[str], Callable[[re.Match[str]], str]]] = [
    # measures: coc_measures__{boundary}__{acs}.parquet
    #        -> measures__A{acs}@B{boundary}.parquet
    (
        "measures",
        re.compile(r"^coc_measures__(\d{4})__(\d{4})\.parquet$"),
        lambda m: f"measures__A{m.group(2)}@B{m.group(1)}.parquet",
    ),
]


def scan_curated_for_migration(
    base_dir: Path | None = None,
) -> MigrationPlan:
    """Scan curated data directory and propose migration actions.

    Parameters
    ----------
    base_dir : Path or None
        Path to curated data root.  Defaults to ``data/curated``.

    Returns
    -------
    MigrationPlan
        Proposed renames, detected duplicates, and unknown files.
    """
    if base_dir is None:
        base_dir = curated_root()

    plan = MigrationPlan()

    if not base_dir.is_dir():
        return plan

    # Import canonical patterns for reference
    from hhplab.artifacts.curated.policy import CANONICAL_PATTERNS, CURATED_SUBDIRS, IGNORED_FILES

    for entry in sorted(base_dir.iterdir()):
        if not entry.is_dir():
            continue
        subdir_name = entry.name
        if subdir_name not in CURATED_SUBDIRS:
            continue

        patterns = CANONICAL_PATTERNS.get(subdir_name, [])
        existing_names = {f.name for f in entry.iterdir() if f.is_file()}

        for item in sorted(entry.iterdir()):
            if not item.is_file():
                continue
            name = item.name
            if name in IGNORED_FILES:
                continue

            # Already canonical — skip
            if patterns and any(p.match(name) for p in patterns):
                continue

            # Try legacy rename rules
            matched = False
            for rule_subdir, legacy_pat, builder in _LEGACY_RULES:
                if rule_subdir != subdir_name:
                    continue
                m = legacy_pat.match(name)
                if m:
                    canonical_name = builder(m)
                    target = entry / canonical_name
                    if canonical_name in existing_names:
                        plan.duplicates.append(
                            MigrationAction(
                                source=item,
                                target=target,
                                action="duplicate",
                                message=(
                                    f"Legacy '{name}' has canonical equivalent "
                                    f"'{canonical_name}' which already exists."
                                ),
                            )
                        )
                    else:
                        plan.renames.append(
                            MigrationAction(
                                source=item,
                                target=target,
                                action="rename",
                                message=(
                                    f"Rename '{name}' -> '{canonical_name}'"
                                ),
                            )
                        )
                    matched = True
                    break

            if not matched:
                plan.unknown.append(
                    MigrationAction(
                        source=item,
                        target=None,
                        action="unknown",
                        message=(
                            f"Non-canonical file '{name}' in {subdir_name}/ "
                            f"has no known migration rule."
                        ),
                    )
                )

    return plan


def apply_migration(plan: MigrationPlan, *, dry_run: bool = True) -> list[str]:
    """Apply migration actions from a plan.

    Parameters
    ----------
    plan : MigrationPlan
        The migration plan from ``scan_curated_for_migration()``.
    dry_run : bool
        If True, only report what would be done.  Default is True (safe).

    Returns
    -------
    list[str]
        Log of actions taken (or would be taken in dry-run mode).
    """
    log: list[str] = []
    prefix = "[DRY-RUN] " if dry_run else ""

    for action in plan.renames:
        if action.target is None:
            raise ValueError(
                f"Rename action for {action.source} has no target path."
            )
        msg = f"{prefix}Rename: {action.source} -> {action.target}"
        log.append(msg)
        if not dry_run:
            action.source.rename(action.target)

    for action in plan.duplicates:
        msg = f"{prefix}SKIP (duplicate): {action.message}"
        log.append(msg)

    for action in plan.unknown:
        msg = f"{prefix}SKIP (unknown): {action.message}"
        log.append(msg)

    return log
