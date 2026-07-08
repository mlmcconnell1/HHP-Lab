"""Legacy wrapper for the tracked non-compositional panel workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_noncompositional_rent_population_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
