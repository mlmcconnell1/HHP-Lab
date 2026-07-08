"""Legacy wrapper for the tracked pooled Vera/HIC/PIT longitudinal workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_vera_hic_pit_longitudinal_pooled import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
