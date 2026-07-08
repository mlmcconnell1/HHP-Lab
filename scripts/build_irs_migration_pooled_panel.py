"""Legacy wrapper for the tracked IRS migration pooled workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_irs_migration_pooled_panel import main as _workflow_main


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
