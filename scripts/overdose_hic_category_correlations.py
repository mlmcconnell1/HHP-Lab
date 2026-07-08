"""Legacy wrapper for the tracked overdose/HIC correlations workflow module."""

from __future__ import annotations

from hhplab.results.workflows.overdose_hic_category_correlations import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
