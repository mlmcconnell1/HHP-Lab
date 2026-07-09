"""Legacy wrapper for the tracked employment/labor-force workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_employment_labor_force_composition_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
