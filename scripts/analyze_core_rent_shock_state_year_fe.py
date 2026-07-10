"""Legacy wrapper for the tracked core rent-shock robustness workflow."""

from hhplab.results.workflows.analyze_core_rent_shock_state_year_fe import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
