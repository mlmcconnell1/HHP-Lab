"""Legacy wrapper for the tracked household-size composition workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_household_size_composition_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
