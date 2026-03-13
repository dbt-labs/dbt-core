"""dbt Rationale Analyzer — structured rationale metadata for dbt projects.

A lightweight, portable tool that parses dbt YAML files for `meta.rationale`
blocks, validates them against a structured schema, and reports on coverage
and quality. Designed for local use and CI enforcement via GitHub Actions.

Usage:
    python -m dbt_rationale /path/to/dbt/project
    python -m dbt_rationale /path/to/dbt/project --format json
    python -m dbt_rationale /path/to/dbt/project --enforce soft
"""

__version__ = "0.1.0"
