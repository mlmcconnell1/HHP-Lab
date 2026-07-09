"""Legacy wrapper for the tracked eviction-rate timing workflow module."""

from __future__ import annotations

from hhplab.results.workflows.build_eviction_rate_timing_panel import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
