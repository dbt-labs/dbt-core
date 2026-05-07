"""Placeholder entrypoint for the dbt-kernel package.

The real engine ships in 2.0.0.
"""

from __future__ import annotations

import sys

from . import __version__

_MESSAGE = (
    f"dbt-kernel {__version__} (placeholder)\n"
    "The real engine ships in 2.0.0, starting with the 2.0.0a1 alpha.\n"
)


def main() -> int:
    sys.stdout.write(_MESSAGE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
