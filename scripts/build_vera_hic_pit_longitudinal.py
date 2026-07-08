"""Legacy wrapper for the tracked Vera/HIC/PIT longitudinal workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_vera_hic_pit_longitudinal import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
