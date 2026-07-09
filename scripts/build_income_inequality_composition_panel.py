"""Legacy wrapper for the tracked income-inequality workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_income_inequality_composition_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
