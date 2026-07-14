"""Compat shim: `from dbt.contracts.graph.manifest import Manifest`.

Re-exports the Rust-backed pyclass so the legacy import path resolves and
`isinstance(m, Manifest)` holds for results returned by `dbtRunner().invoke()`.
"""

from __future__ import annotations

from dbt._core import Manifest

__all__ = ["Manifest"]
