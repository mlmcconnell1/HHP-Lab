"""Panel policy appliers: ZORI, ACS1, and BLS LAUS branches.

Translates ``target.panel_policy`` into concrete mutations on a
partially assembled panel DataFrame and surfaces any policy-specific
metadata needed downstream.  Each concrete applier is an independently
unit-testable strategy object; ``DEFAULT_APPLIERS`` captures the
ZORI-before-ACS1-before-LAUS ordering invariant in a single place.

Also exposes ``collect_conformance_flags`` so the persistence step
reads the same policy surface as assembly, eliminating the drift
between the two inline policy reads that existed before the split.

This module is one leg of the executor panel/persistence split tracked
in coclab-anb0; the step-by-step extraction plan lives in
``background/executor_panel_split_design.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import numpy as np
import pandas as pd

from coclab.recipe.recipe_schema import PanelPolicy, RecipeV1


@dataclass(frozen=True)
class PolicyApplication:
    """The result of applying one PanelPolicyApplier to a panel.

    Attributes
    ----------
    name:
        Applier identifier (``"zori"`` / ``"acs1"`` / ``"laus"``).  Used
        as the key in ``AssembledPanel.policy_artifacts`` once the
        strategy pattern is fully wired up.
    panel:
        The possibly-mutated DataFrame to pass to the next applier (or
        to ``finalize_panel``).  Appliers that decide to skip should
        return the input frame unchanged.
    extra_columns:
        Columns to append to the shared ``finalize_panel`` ``extra_columns``
        argument.  Empty when the applier does not contribute extras.
    provenance:
        Policy-specific provenance object (today: ``ZoriProvenance`` for
        the ZORI applier, ``None`` for ACS1 / LAUS).  Flows through to
        ``executor_persistence`` via ``AssembledPanel.zori_provenance``.
    notes:
        Progress messages the caller should emit via ``_echo`` after the
        applier returns.
    """

    name: str
    panel: pd.DataFrame
    extra_columns: tuple[str, ...] = ()
    provenance: object | None = None
    notes: tuple[str, ...] = ()


class PanelPolicyApplier(Protocol):
    """Strategy protocol for per-policy panel mutations.

    Implementations live alongside this protocol in
    ``executor_panel_policies`` and are assembled into the
    ``DEFAULT_APPLIERS`` tuple so ``assemble_panel`` can loop over them
    in a single, order-explicit pass.
    """

    name: str

    def applies_to(
        self,
        *,
        target_geo_type: str,
        policy: PanelPolicy | None,
    ) -> bool:
        ...

    def apply(
        self,
        panel: pd.DataFrame,
        *,
        policy: PanelPolicy,
        target_geo_type: str,
    ) -> PolicyApplication:
        ...


@dataclass
class ZoriPolicyApplier:
    """Apply ZORI eligibility, rent_to_income, and provenance to a panel.

    Mirrors the ZORI branch previously inlined at the top of
    ``assemble_panel``.  The ``zori_eligibility`` import stays inside
    ``apply`` so recipes without a ZORI policy do not pay the import
    cost.
    """

    name: str = field(default="zori", init=False)

    def applies_to(
        self,
        *,
        target_geo_type: str,
        policy: PanelPolicy | None,
    ) -> bool:
        return policy is not None and policy.zori is not None

    def apply(
        self,
        panel: pd.DataFrame,
        *,
        policy: PanelPolicy,
        target_geo_type: str,
    ) -> PolicyApplication:
        from coclab.panel.zori_eligibility import (
            ZoriProvenance,
            add_provenance_columns,
            apply_zori_eligibility,
            compute_rent_to_income,
        )

        # Canonicalize recipe-native ZORI measure → canonical panel column.
        # Recipe aggregation (county→target) produces a column named "zori"
        # (the recipe measure name); the eligibility logic expects "zori_coc".
        if "zori" in panel.columns and "zori_coc" not in panel.columns:
            panel = panel.rename(columns={"zori": "zori_coc"})

        if "zori_coc" not in panel.columns:
            # ZORI policy declared but no data arrived; emit a skipped
            # application so the caller still records that the applier ran.
            return PolicyApplication(name=self.name, panel=panel)

        zori_policy = policy.zori  # type: ignore[union-attr]

        # Detect rent alignment from resampled data (column injected by
        # the ZORI resample step when the source has a "method" column).
        rent_alignment = "pit_january"
        if "method" in panel.columns:
            methods = panel["method"].dropna().unique()
            if len(methods) == 1:
                rent_alignment = str(methods[0])

        panel = apply_zori_eligibility(
            panel,
            min_coverage=zori_policy.min_coverage,
        )
        panel = compute_rent_to_income(panel)

        prov = ZoriProvenance(
            rent_alignment=rent_alignment,
            zori_min_coverage=zori_policy.min_coverage,
        )
        panel = add_provenance_columns(panel, prov)

        # Drop temporary columns that leak from resample intermediates.
        for leak in ("method", "geo_count"):
            if leak in panel.columns:
                panel = panel.drop(columns=[leak])

        extra_columns: tuple[str, ...] = ()
        if "zori_max_geo_contribution" in panel.columns:
            extra_columns = ("zori_max_geo_contribution",)

        return PolicyApplication(
            name=self.name,
            panel=panel,
            extra_columns=extra_columns,
            provenance=prov,
        )


@dataclass
class Acs1PolicyApplier:
    """Add ACS 1-year provenance columns to a metro panel.

    Mirrors the ACS1 branch previously inlined in ``assemble_panel``.
    The recipe pipeline uses the analysis year for joins, but an
    identity-resampled ACS1 dataset may also retain ``acs1_vintage`` so
    the panel can report the actual input vintage when recipes
    intentionally load a lagged artifact.
    """

    name: str = field(default="acs1", init=False)

    def applies_to(
        self,
        *,
        target_geo_type: str,
        policy: PanelPolicy | None,
    ) -> bool:
        return (
            target_geo_type == "metro"
            and policy is not None
            and policy.acs1 is not None
            and policy.acs1.include
        )

    def apply(
        self,
        panel: pd.DataFrame,
        *,
        policy: PanelPolicy,
        target_geo_type: str,
    ) -> PolicyApplication:
        has_acs1_data = (
            "unemployment_rate_acs1" in panel.columns
            and panel["unemployment_rate_acs1"].notna().any()
        )
        if has_acs1_data:
            vintage_source = "acs1_vintage" if "acs1_vintage" in panel.columns else "year"
            panel["acs1_vintage_used"] = panel[vintage_source].astype("string")
            panel["acs_products_used"] = "acs5,acs1"
            # Null out vintage for rows where ACS1 data is missing.
            acs1_missing = panel["unemployment_rate_acs1"].isna()
            if acs1_missing.any():
                panel.loc[acs1_missing, "acs1_vintage_used"] = pd.NA
        else:
            panel["acs1_vintage_used"] = pd.NA
            panel["acs_products_used"] = "acs5"
            if "unemployment_rate_acs1" not in panel.columns:
                panel["unemployment_rate_acs1"] = np.nan
        if "acs1_vintage" in panel.columns:
            panel = panel.drop(columns=["acs1_vintage"])
        return PolicyApplication(name=self.name, panel=panel)


@dataclass
class LausPolicyApplier:
    """Add BLS LAUS provenance columns to a metro panel.

    Mirrors the LAUS branch previously inlined in ``assemble_panel``.
    LAUS is year-aligned: each panel row's year is the LAUS reference
    year, so ``laus_vintage_used`` is a direct copy of ``year``.
    """

    name: str = field(default="laus", init=False)

    def applies_to(
        self,
        *,
        target_geo_type: str,
        policy: PanelPolicy | None,
    ) -> bool:
        return (
            target_geo_type == "metro"
            and policy is not None
            and policy.laus is not None
            and policy.laus.include
        )

    def apply(
        self,
        panel: pd.DataFrame,
        *,
        policy: PanelPolicy,
        target_geo_type: str,
    ) -> PolicyApplication:
        has_laus_data = (
            "unemployment_rate" in panel.columns
            and panel["unemployment_rate"].notna().any()
        )
        if has_laus_data:
            panel["laus_vintage_used"] = panel["year"].astype(str)
            laus_missing = panel["unemployment_rate"].isna()
            if laus_missing.any():
                panel.loc[laus_missing, "laus_vintage_used"] = pd.NA
        else:
            panel["laus_vintage_used"] = pd.NA
            for col in ("labor_force", "employed", "unemployed", "unemployment_rate"):
                if col not in panel.columns:
                    panel[col] = np.nan
        return PolicyApplication(name=self.name, panel=panel)


# Applier order is load-bearing: the ZORI applier renames and drops
# columns (zori → zori_coc, drops method/geo_count) that later appliers
# must not inspect, and ACS1 / LAUS each stamp their own vintage column
# without interfering with one another.  Keep this tuple in sync with
# the pre-split inline ordering; reordering changes the set of columns
# visible at finalize_panel time.
DEFAULT_APPLIERS: tuple[PanelPolicyApplier, ...] = (
    ZoriPolicyApplier(),
    Acs1PolicyApplier(),
    LausPolicyApplier(),
)


@dataclass(frozen=True)
class ConformanceFlags:
    """Policy-derived conformance configuration for a persisted panel.

    This is the one-stop translation of ``target.panel_policy`` for
    persistence-time conformance, replacing the ~45 lines previously
    inlined inside ``persist_outputs``.  Assembly and persistence now
    share the single policy-read path in ``collect_conformance_flags``.
    """

    include_zori: bool
    include_laus: bool
    acs_products: tuple[str, ...]
    measure_columns: list[str] | None


def collect_conformance_flags(
    *,
    recipe: RecipeV1,
    target: object,
    panel: pd.DataFrame,
) -> ConformanceFlags:
    """Resolve panel-policy-driven conformance flags for the given target.

    Mirrors the logic previously inlined in ``persist_outputs``: derives
    ``measure_columns`` from recipe datasets, translates them through
    any active column aliases (including LAUS columns when LAUS is
    active), and decides which ACS products and which optional policy
    slices conformance should check.  Kept byte-equivalent to the
    pre-split code so the produced ``PanelRequest`` is identical.
    """
    from coclab.panel.conformance import ACS_MEASURE_COLUMNS, LAUS_MEASURE_COLUMNS

    # Derive measure_columns from recipe datasets so non-ACS schemas
    # (e.g. PEP) get correct conformance checking (coclab-d0qm).
    recipe_products = {ds.product for ds in recipe.datasets.values()}
    if recipe_products & {"acs", "acs5"}:
        measure_columns: list[str] | None = None  # ACS default
    else:
        # Non-ACS schema: check whichever known measures are in the panel.
        known = set(ACS_MEASURE_COLUMNS) | {"population"}
        measure_columns = [c for c in panel.columns if c in known] or None

    policy: PanelPolicy | None = getattr(target, "panel_policy", None)

    # LAUS-aware conformance: determine include_laus before alias translation
    # so that LAUS columns are included in the alias-translated measure_columns
    # list (coclab-xt72).
    include_laus = (
        policy is not None
        and policy.laus is not None
        and policy.laus.include
    )

    # Translate measure_columns through any active column aliases so that
    # conformance checks look for the renamed names in the finalized panel.
    # When include_laus is True, LAUS columns are appended to base_cols before
    # translation so they are not silently dropped by the early-return path in
    # _effective_measure_columns (coclab-xt72).
    aliases: dict[str, str] = {}
    if policy is not None and policy.column_aliases:
        aliases = dict(policy.column_aliases)
    if aliases:
        base_cols = list(ACS_MEASURE_COLUMNS if measure_columns is None else measure_columns)
        if include_laus:
            base_cols += [c for c in LAUS_MEASURE_COLUMNS if c not in base_cols]
        measure_columns = [aliases.get(c, c) for c in base_cols]
    elif include_laus and measure_columns is not None:
        # No aliases, but non-ACS path explicitly set measure_columns.
        # _effective_measure_columns() returns measure_columns directly when it
        # is non-None, so LAUS columns must be added here for LAUS-only recipes
        # that have no column aliases (coclab-d9d3).
        for col in LAUS_MEASURE_COLUMNS:
            if col not in measure_columns:
                measure_columns.append(col)

    # ACS1-aware conformance (coclab-gude.3): include acs1 product when
    # the panel policy requests it and the column is present.
    acs_products: tuple[str, ...] = ("acs5",)
    if (
        policy is not None
        and policy.acs1 is not None
        and policy.acs1.include
        and "unemployment_rate_acs1" in panel.columns
    ):
        acs_products = ("acs5", "acs1")

    # ZORI-aware conformance (coclab-gude.2).
    include_zori = policy is not None and policy.zori is not None

    return ConformanceFlags(
        include_zori=include_zori,
        include_laus=include_laus,
        acs_products=acs_products,
        measure_columns=measure_columns,
    )
