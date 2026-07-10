"""Legacy wrapper for the overdose-PSH state-year robustness workflow."""

from hhplab.results.workflows.analyze_overdose_psh_state_year_robustness import (
    main as _workflow_main,
)


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
