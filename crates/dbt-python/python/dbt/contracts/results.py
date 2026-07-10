"""Compat shim: `from dbt.contracts.results import RunResultsArtifact, ...`.

Re-exports the Rust-backed artifact pyclasses so legacy import paths resolve and
`isinstance` checks hold for results returned by `dbtRunner().invoke()`.
"""

from __future__ import annotations

from dbt._core import (
    CatalogArtifact,
    FreshnessResultsArtifact,
    RunResultsArtifact,
)

__all__ = [
    "RunResultsArtifact",
    "CatalogArtifact",
    "FreshnessResultsArtifact",
]
