"""Legacy wrapper for the tracked supply-IV workflow module."""

from __future__ import annotations

import sys

from hhplab.results.workflows.build_supply_iv_panel import main as _workflow_main


def main(argv: list[str] | None = None) -> None:
    _workflow_main(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    main()
