#!/usr/bin/env python3
"""Replay a raw PTY log and print its final rendered screen.

Usage: render.py RAW_LOG [COLS] [ROWS]

Install the optional dependency in the Python environment used to run this
script, for example: ``python3 -m pip install pyte``.
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_log")
    parser.add_argument("cols", nargs="?", type=int, default=200)
    parser.add_argument("rows", nargs="?", type=int, default=50)
    args = parser.parse_args()
    if args.cols <= 0 or args.rows <= 0:
        parser.error("COLS and ROWS must be positive")

    try:
        import pyte
    except ImportError:
        print(
            "render.py requires Python package 'pyte'; install it with "
            "python3 -m pip install pyte (preferably in a virtual environment)",
            file=sys.stderr,
        )
        return 2

    try:
        screen = pyte.Screen(args.cols, args.rows)
        stream = pyte.ByteStream(screen)
        with open(args.raw_log, "rb") as raw_log:
            while chunk := raw_log.read(64 * 1024):
                stream.feed(chunk)
    except OSError as exc:
        print(f"render.py: cannot read {args.raw_log!r}: {exc}", file=sys.stderr)
        return 1

    lines = [line.rstrip() for line in screen.display]
    while lines and not lines[-1]:
        lines.pop()
    print("\n".join(line for line in lines if line.strip()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
