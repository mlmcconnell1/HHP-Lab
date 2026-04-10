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

import pandas as pd

from coclab.recipe.recipe_schema import PanelPolicy


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
