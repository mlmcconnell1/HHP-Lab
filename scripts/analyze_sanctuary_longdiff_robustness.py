"""Legacy wrapper for the sanctuary long-difference robustness workflow."""

from hhplab.results.workflows.analyze_sanctuary_longdiff_robustness import main as _workflow_main


def main() -> None:
    _workflow_main()


if __name__ == "__main__":
    main()
