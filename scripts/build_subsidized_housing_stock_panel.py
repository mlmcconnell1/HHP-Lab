"""Legacy wrapper for the tracked subsidized-housing-stock workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_subsidized_housing_stock_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
