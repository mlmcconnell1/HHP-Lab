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
