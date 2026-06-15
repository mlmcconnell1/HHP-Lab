"""Canonical HIC parser contract declarations.

Full raw-file parsing is implemented with the HIC ingest workflow. This module
declares the stable parser output columns so schema and downstream contracts can
depend on HIC as a first-class curated artifact family.
"""

from hhplab.schema.columns import HIC_CANONICAL_COLUMNS

CANONICAL_COLUMNS: list[str] = HIC_CANONICAL_COLUMNS
