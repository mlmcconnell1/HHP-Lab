"""Legacy wrapper for the tracked QCEW labor-market workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_qcew_labor_market_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
