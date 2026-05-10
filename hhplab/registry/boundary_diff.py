"""Reusable boundary vintage comparison helpers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BoundaryVintageDiff:
    """Boundary comparison result grouped by record status."""

    added_ids: list[str]
    removed_ids: list[str]
    changed_ids: list[str]
    unchanged_ids: list[str]
    geom_hashes_v1: dict[str, object]
    geom_hashes_v2: dict[str, object]

    @property
    def v1_count(self) -> int:
        return len(self.geom_hashes_v1)

    @property
    def v2_count(self) -> int:
        return len(self.geom_hashes_v2)

    def to_frame(self, vintage1: str, vintage2: str) -> pd.DataFrame:
        """Return a CSV-ready diff frame with vintage metadata columns."""
        records: list[dict[str, object]] = []
        for status, coc_ids in (
            ("added", self.added_ids),
            ("removed", self.removed_ids),
            ("changed", self.changed_ids),
            ("unchanged", self.unchanged_ids),
        ):
            for coc_id in coc_ids:
                records.append(
                    {
                        "coc_id": coc_id,
                        "status": status,
                        "geom_hash_v1": self.geom_hashes_v1.get(coc_id),
                        "geom_hash_v2": self.geom_hashes_v2.get(coc_id),
                    }
                )

        diff_df = pd.DataFrame(records)
        if not diff_df.empty:
            diff_df = diff_df.sort_values("coc_id")
        diff_df.insert(0, "vintage1", vintage1)
        diff_df.insert(1, "vintage2", vintage2)
        return diff_df


def compare_boundary_records(vintage1: pd.DataFrame, vintage2: pd.DataFrame) -> BoundaryVintageDiff:
    """Compare boundary records by ``coc_id`` and ``geom_hash``."""
    required_cols = {"coc_id", "geom_hash"}
    for label, frame in (("vintage1", vintage1), ("vintage2", vintage2)):
        missing = required_cols - set(frame.columns)
        if missing:
            raise ValueError(f"{label} missing required columns: {sorted(missing)}")

    v1_hashes = dict(zip(vintage1["coc_id"], vintage1["geom_hash"], strict=True))
    v2_hashes = dict(zip(vintage2["coc_id"], vintage2["geom_hash"], strict=True))
    v1_ids = set(v1_hashes)
    v2_ids = set(v2_hashes)
    common_ids = v1_ids & v2_ids

    return BoundaryVintageDiff(
        added_ids=sorted(v2_ids - v1_ids),
        removed_ids=sorted(v1_ids - v2_ids),
        changed_ids=sorted(
            coc_id for coc_id in common_ids if v1_hashes[coc_id] != v2_hashes[coc_id]
        ),
        unchanged_ids=sorted(
            coc_id for coc_id in common_ids if v1_hashes[coc_id] == v2_hashes[coc_id]
        ),
        geom_hashes_v1=v1_hashes,
        geom_hashes_v2=v2_hashes,
    )
