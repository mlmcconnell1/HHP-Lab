"""Legacy wrapper for the tracked housing-cost-burden workflow."""

from __future__ import annotations

from hhplab.results.workflows.build_housing_cost_burden_composition_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()

if __name__ == "__main__":
    main()
