"""Legacy wrapper for the tracked Vera/HIC/PIT correlations workflow module."""

from __future__ import annotations

from hhplab.results.workflows.vera_hic_pit_correlations import main as _workflow_main


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
