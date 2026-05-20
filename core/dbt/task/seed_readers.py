"""Loaders that convert seed files into agate tables.

The CSV path delegates to dbt_common.clients.agate_helper. The JSONL path
implements object-per-line parsing with nested-value preservation.
"""

import json
import os
from typing import Dict, List, Optional, Sequence

import agate

from dbt_common.exceptions import DbtInternalError


# ---------------------------------------------------------------------------
# JSONL loader
# ---------------------------------------------------------------------------

_JSONL_EXTENSIONS = frozenset({".jsonl", ".ndjson"})


class JSONLSeedError(Exception):
    """Raised when a JSONL seed file contains invalid content."""

    def __init__(self, path: str, line_number: int, message: str) -> None:
        self.path = path
        self.line_number = line_number
        self.message = message
        super().__init__(f"Invalid JSONL seed at {path}:{line_number}. {message}")


def load_jsonl_seed_agate_table(
    path: str,
    column_types: Optional[Dict[str, str]] = None,
) -> agate.Table:
    """Parse a JSONL seed file into an agate table.

    Each non-empty line must be a JSON object. Top-level keys across all
    records become columns (first-seen ordering). Nested dicts and lists are
    serialised as compact JSON strings. Missing keys become None.
    """
    column_names: List[str] = []
    column_names_set: set = set()
    rows: List[tuple] = []

    with open(path, "r", encoding="utf-8-sig") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise JSONLSeedError(
                    path,
                    line_number,
                    f"Could not parse JSON: {exc.msg}.",
                ) from exc

            if not isinstance(value, dict):
                type_name = type(value).__name__
                if isinstance(value, list):
                    type_name = "list"
                raise JSONLSeedError(
                    path,
                    line_number,
                    f"Expected each non-empty line to be a JSON object; got {type_name}.",
                )

            # Discover new columns in first-seen order.
            for key in value:
                if key not in column_names_set:
                    column_names.append(key)
                    column_names_set.add(key)

            # Build a row tuple in column_names order.
            row = tuple(_normalize_jsonl_value(value.get(col)) for col in column_names)
            rows.append(row)

    if not column_names:
        raise JSONLSeedError(
            path,
            0,
            "JSONL seed file is empty or contains no valid JSON objects.",
        )

    # Determine which columns ever held a nested JSON value so we can force
    # them to agate.Text. Scan the normalised rows rather than re-reading.
    nested_columns = _detect_nested_columns(rows, column_names)

    # Build explicit column types.
    agate_column_types = _build_agate_column_types(rows, column_names, nested_columns)

    return agate.Table(rows, column_names, column_types=agate_column_types)


def _normalize_jsonl_value(value):
    """Convert a JSON value to a Python value suitable for an agate cell."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    raise DbtInternalError(f"Unexpected JSON value type: {type(value)}")


def _detect_nested_columns(
    rows: Sequence[tuple],
    column_names: List[str],
) -> set:
    """Return the set of column indices that contain serialised JSON strings."""
    nested = set()
    for row in rows:
        for i, val in enumerate(row):
            if i in nested:
                continue
            if isinstance(val, str) and _looks_like_json(val):
                nested.add(i)
    return nested


def _looks_like_json(value: str) -> bool:
    """Quick check whether a string starts with a JSON object or array."""
    return len(value) > 0 and value[0] in ("{", "[")


def _build_agate_column_types(
    rows: Sequence[tuple],
    column_names: List[str],
    nested_column_indices: set,
) -> List:
    """Build an explicit list of agate column types.

    Columns containing nested JSON are forced to Text. Other columns get a
    TypeTester that will infer number, boolean, date, or text.
    """
    from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER

    types: List = []
    for i, name in enumerate(column_names):
        if i in nested_column_indices:
            types.append(agate.Text())
        else:
            types.append(DEFAULT_TYPE_TESTER)
    return types
