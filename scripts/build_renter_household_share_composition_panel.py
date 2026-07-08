"""Legacy wrapper for the tracked renter-share composition workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_renter_household_share_composition_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
