"""Coverage QA for HUD HIC artifacts against PIT CoC-year coverage."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from hhplab.schema.columns import HIC_PANEL_MEASURE_COLUMNS, HIC_PROJECT_TYPES
from hhplab.sources.hud.pit.qa import QAReport
from hhplab.storage.paths import curated_dir

_PIT_FILE_RE = re.compile(
    r"^(?:pit__P(?P<year>\d{4})(?:@B\d{4})?|pit_vintage__P(?P<vintage>\d{4}))\.parquet$"
)
_HIC_FILE_RE = re.compile(r"^hic__H(?P<year>\d{4})\.parquet$")


@dataclass(frozen=True)
class HICCoverageResult:
    """HIC/PIT coverage QA report and per-year coverage table."""

    report: QAReport
    coverage: pd.DataFrame
    pit_files: list[Path]
    hic_files: list[Path]

    def to_dict(self) -> dict:
        """Return a JSON-serializable coverage QA payload."""
        return {
            "passed": self.report.passed,
            "summary": self.report.summary,
            "coverage": self.coverage.to_dict(orient="records"),
            "issues": [issue.to_dict() for issue in self.report.issues],
            "pit_files": [str(path) for path in self.pit_files],
            "hic_files": [str(path) for path in self.hic_files],
        }


def validate_hic_pit_coverage(
    *,
    pit_dir: Path | str | None = None,
    hic_dir: Path | str | None = None,
    years: list[int] | None = None,
    yoy_threshold: float = 0.75,
) -> HICCoverageResult:
    """Compare canonical HIC CoC-year coverage against canonical PIT coverage."""
    resolved_pit_dir = Path(pit_dir) if pit_dir is not None else curated_dir("pit")
    resolved_hic_dir = Path(hic_dir) if hic_dir is not None else curated_dir("hic")
    requested_years = set(years) if years is not None else None

    pit_files = _discover_pit_files(resolved_pit_dir, requested_years)
    hic_files = _discover_year_files(resolved_hic_dir, _HIC_FILE_RE, requested_years)
    if not pit_files:
        raise FileNotFoundError(
            f"No PIT files found in {resolved_pit_dir}. Run `hhplab ingest pit --year <YEAR>` "
            "or place canonical PIT parquet files under data/curated/pit/."
        )
    if not hic_files:
        raise FileNotFoundError(
            f"No HIC files found in {resolved_hic_dir}. Place HUD HIC files under "
            "data/raw/hic/<YEAR>/, then run "
            "`hhplab ingest hic --year <YEAR> --parse-only`."
        )

    pit = _load_year_frames(pit_files, required={"pit_year", "coc_id"}, year_column="pit_year")
    hic = _load_year_frames(
        hic_files,
        required={"hic_year", "coc_id", "total_beds", "total_units"},
        year_column="hic_year",
    )
    if requested_years is not None:
        pit = pit[pit["pit_year"].isin(requested_years)].copy()
        hic = hic[hic["hic_year"].isin(requested_years)].copy()

    report = QAReport()
    _check_duplicates(report, pit, year_column="pit_year", check_name="duplicate_pit_coc_year")
    _check_duplicates(report, hic, year_column="hic_year", check_name="duplicate_hic_coc_year")
    coverage = _compare_coverage(report, pit, hic)
    _check_hic_yoy_swings(report, hic, threshold=yoy_threshold)
    return HICCoverageResult(
        report=report,
        coverage=coverage,
        pit_files=pit_files,
        hic_files=hic_files,
    )


def validate_expanded_hic_artifacts(
    *,
    hic_dir: Path | str | None = None,
    years: list[int] | None = None,
) -> QAReport:
    """Validate expanded HIC columns and total/component consistency."""
    resolved_hic_dir = Path(hic_dir) if hic_dir is not None else curated_dir("hic")
    requested_years = set(years) if years is not None else None
    hic_files = _discover_year_files(resolved_hic_dir, _HIC_FILE_RE, requested_years)
    if not hic_files:
        raise FileNotFoundError(
            f"No HIC files found in {resolved_hic_dir}. Place HUD HIC files under "
            "data/raw/hic/<YEAR>/, then run "
            "`hhplab ingest hic --year <YEAR> --parse-only`."
        )

    report = QAReport()
    for path in hic_files:
        frame = pd.read_parquet(path)
        _check_expanded_hic_frame(report, frame, path=path)
    return report


def _discover_pit_files(directory: Path, years: set[int] | None) -> list[Path]:
    if not directory.exists():
        return []

    vintage_files: dict[int, Path] = {}
    for path in sorted(directory.glob("*.parquet")):
        match = _PIT_FILE_RE.match(path.name)
        if match is None or match.group("vintage") is None:
            continue
        vintage_files[int(match.group("vintage"))] = path
    if vintage_files:
        return [vintage_files[max(vintage_files)]]

    return _discover_year_files(directory, _PIT_FILE_RE, years)


def _discover_year_files(
    directory: Path,
    pattern: re.Pattern[str],
    years: set[int] | None,
) -> list[Path]:
    if not directory.exists():
        return []

    by_year: dict[int, list[Path]] = {}
    for path in sorted(directory.glob("*.parquet")):
        match = pattern.match(path.name)
        if match is None:
            continue
        year = int(match.group("year") or match.group("vintage"))
        if years is not None and year not in years:
            continue
        by_year.setdefault(year, []).append(path)

    selected: list[Path] = []
    for year in sorted(by_year):
        paths = by_year[year]
        base_paths = [path for path in paths if "@" not in path.stem]
        selected.append(sorted(base_paths or paths)[-1])
    return selected


def _load_year_frames(files: list[Path], *, required: set[str], year_column: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in files:
        frame = pd.read_parquet(path)
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{path} missing required column(s): {', '.join(sorted(missing))}.")
        normalized = frame.copy()
        normalized[year_column] = pd.to_numeric(normalized[year_column], errors="raise").astype(int)
        normalized["coc_id"] = normalized["coc_id"].astype("string").str.strip()
        frames.append(normalized)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _check_expanded_hic_frame(report: QAReport, frame: pd.DataFrame, *, path: Path) -> None:
    required = {"hic_year", "coc_id", "total_beds", "total_units", *HIC_PANEL_MEASURE_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        report.add_error(
            "missing_expanded_hic_columns",
            f"HIC artifact {path.name} is missing expanded column(s): {', '.join(missing)}.",
            details={"path": str(path), "missing_columns": missing},
        )
        return

    working = frame.copy()
    working["hic_year"] = pd.to_numeric(working["hic_year"], errors="coerce")
    for column in ["total_beds", "total_units", *HIC_PANEL_MEASURE_COLUMNS]:
        working[column] = pd.to_numeric(working[column], errors="coerce")

    _check_equal_columns(
        report,
        working,
        path=path,
        left="total_beds",
        right="hic_total_beds",
        check_name="hic_total_beds_alias_mismatch",
    )
    _check_equal_columns(
        report,
        working,
        path=path,
        left="total_units",
        right="hic_total_units",
        check_name="hic_total_units_alias_mismatch",
    )

    modern = working[working["hic_year"] >= 2014].copy()
    if modern.empty:
        return

    bed_components = [f"hic_{project_type}_year_round_beds" for project_type in HIC_PROJECT_TYPES]
    unit_components = [f"hic_{project_type}_family_units" for project_type in HIC_PROJECT_TYPES]
    modern["__component_beds"] = modern[bed_components].sum(axis=1)
    modern["__component_units"] = modern[unit_components].sum(axis=1)
    modern["__shelter_beds"] = modern[
        ["hic_es_year_round_beds", "hic_th_year_round_beds", "hic_sh_year_round_beds"]
    ].sum(axis=1)
    modern["__shelter_units"] = modern[
        ["hic_es_family_units", "hic_th_family_units", "hic_sh_family_units"]
    ].sum(axis=1)

    _check_equal_columns(
        report,
        modern,
        path=path,
        left="hic_total_beds",
        right="__component_beds",
        check_name="hic_total_beds_component_mismatch",
    )
    _check_equal_columns(
        report,
        modern,
        path=path,
        left="hic_total_units",
        right="__component_units",
        check_name="hic_total_units_component_mismatch",
    )
    _check_equal_columns(
        report,
        modern,
        path=path,
        left="hic_shelter_year_round_beds",
        right="__shelter_beds",
        check_name="hic_shelter_beds_component_mismatch",
    )
    _check_equal_columns(
        report,
        modern,
        path=path,
        left="hic_shelter_family_units",
        right="__shelter_units",
        check_name="hic_shelter_units_component_mismatch",
    )


def _check_equal_columns(
    report: QAReport,
    frame: pd.DataFrame,
    *,
    path: Path,
    left: str,
    right: str,
    check_name: str,
) -> None:
    mismatch = frame[left].fillna(-1) != frame[right].fillna(-1)
    for _, row in frame.loc[mismatch, ["hic_year", "coc_id", left, right]].iterrows():
        report.add_error(
            check_name,
            f"{left} does not match {right}.",
            coc_id=str(row["coc_id"]),
            year=int(row["hic_year"]) if pd.notna(row["hic_year"]) else None,
            details={
                "path": str(path),
                left: None if pd.isna(row[left]) else float(row[left]),
                right: None if pd.isna(row[right]) else float(row[right]),
            },
        )


def _check_duplicates(
    report: QAReport,
    df: pd.DataFrame,
    *,
    year_column: str,
    check_name: str,
) -> None:
    duplicates = df[df.duplicated([year_column, "coc_id"], keep=False)]
    for (year, coc_id), group in duplicates.groupby([year_column, "coc_id"], dropna=False):
        report.add_error(
            check_name,
            f"CoC-year appears {len(group)} times.",
            coc_id=str(coc_id),
            year=int(year),
            details={"occurrence_count": int(len(group))},
        )


def _compare_coverage(report: QAReport, pit: pd.DataFrame, hic: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    years = sorted(set(pit["pit_year"].astype(int)) | set(hic["hic_year"].astype(int)))
    for year in years:
        pit_cocs = set(pit.loc[pit["pit_year"] == year, "coc_id"].dropna().astype(str))
        hic_cocs = set(hic.loc[hic["hic_year"] == year, "coc_id"].dropna().astype(str))
        missing_hic = sorted(pit_cocs - hic_cocs)
        unexpected_hic = sorted(hic_cocs - pit_cocs)

        for coc_id in missing_hic:
            report.add_warning(
                "missing_hic_coc_year",
                "CoC-year exists in PIT but is missing from HIC.",
                coc_id=coc_id,
                year=year,
            )
        for coc_id in unexpected_hic:
            report.add_warning(
                "unexpected_hic_coc_year",
                "CoC-year exists in HIC but is missing from PIT.",
                coc_id=coc_id,
                year=year,
            )

        rows.append(
            {
                "year": year,
                "pit_coc_count": len(pit_cocs),
                "hic_coc_count": len(hic_cocs),
                "matched_coc_count": len(pit_cocs & hic_cocs),
                "missing_hic_count": len(missing_hic),
                "unexpected_hic_count": len(unexpected_hic),
            }
        )
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def _check_hic_yoy_swings(report: QAReport, hic: pd.DataFrame, *, threshold: float) -> None:
    if threshold <= 0 or hic.empty:
        return
    working = hic[["hic_year", "coc_id", "total_beds"]].copy()
    working["total_beds"] = pd.to_numeric(working["total_beds"], errors="coerce")
    grouped = (
        working.dropna(subset=["total_beds"])
        .groupby(["hic_year", "coc_id"], as_index=False)["total_beds"]
        .sum()
        .sort_values(["coc_id", "hic_year"])
    )
    for coc_id, group in grouped.groupby("coc_id"):
        previous_year: int | None = None
        previous_beds: float | None = None
        for row in group.itertuples(index=False):
            year = int(row.hic_year)
            beds = float(row.total_beds)
            if (
                previous_year is not None
                and year - previous_year == 1
                and previous_beds is not None
                and previous_beds > 0
            ):
                ratio = abs(beds - previous_beds) / previous_beds
                if ratio > threshold:
                    report.add_warning(
                        "large_hic_bed_yoy_swing",
                        f"HIC total beds changed by {ratio:.1%} from {previous_year} to {year}.",
                        coc_id=str(coc_id),
                        year=year,
                        details={
                            "previous_year": previous_year,
                            "previous_total_beds": previous_beds,
                            "current_total_beds": beds,
                            "relative_change": ratio,
                            "threshold": threshold,
                        },
                    )
            previous_year = year
            previous_beds = beds
