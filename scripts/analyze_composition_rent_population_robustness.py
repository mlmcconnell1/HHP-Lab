"""Legacy wrapper for the tracked composition robustness workflow module."""

from __future__ import annotations

from hhplab.results.workflows.analyze_composition_rent_population_robustness import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
