"""Parquet, manifest, and diagnostics persistence for recipe execution.

Consumes a pre-assembled ``AssembledPanel`` from ``executor_panel`` and
writes the canonical outputs: the panel parquet with embedded
provenance metadata, the sidecar ``*.manifest.json`` file, and the
``*__diagnostics.json`` report.  Reads ``target.panel_policy`` only
through the conformance-flag helper in ``executor_panel_policies`` so
assembly and persistence share a single policy-read path.

This module is one leg of the executor panel/persistence split tracked
in coclab-anb0; the step-by-step extraction plan lives in
``background/executor_panel_split_design.md``.
"""

from __future__ import annotations
