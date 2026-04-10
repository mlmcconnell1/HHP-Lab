"""Panel assembly from recipe execution intermediates.

Owns the pure transformation from per-year joined intermediates onto a
fully-canonicalized panel DataFrame: year-frame gathering, target
metadata stamping, ZORI/ACS1/LAUS panel policy application, shared
``finalize_panel`` shaping, and the cohort selector.  No parquet, no
JSON, no manifest, no conformance — those all live in
``executor_persistence``.

This module is one leg of the executor panel/persistence split tracked
in coclab-anb0; the step-by-step extraction plan lives in
``background/executor_panel_split_design.md``.
"""

from __future__ import annotations
