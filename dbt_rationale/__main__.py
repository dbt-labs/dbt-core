"""Allow running as `python -m dbt_rationale`."""

import sys

from dbt_rationale.cli import main

sys.exit(main())
