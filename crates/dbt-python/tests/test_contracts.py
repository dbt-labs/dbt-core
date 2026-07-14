"""Contract pyclass exposure + dbtRunnerResult.result population.

Verifies the canonical import paths resolve, `isinstance` parity holds, and a
`parse` invocation returns a `Manifest` on `.result`. parse is hermetic (no
warehouse connection), so these run in the default tier.
"""

from __future__ import annotations

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import (
    CatalogArtifact,
    FreshnessResultsArtifact,
    RunResultsArtifact,
)


def _invoke(proj, *args):
    return dbtRunner().invoke([*args, "--project-dir", str(proj), "--profiles-dir", str(proj)])


def test_contract_import_paths_resolve():
    # The legacy import paths must resolve to the Rust-backed pyclasses.
    assert Manifest.__module__ == "dbt.contracts.graph.manifest"
    assert RunResultsArtifact.__module__ == "dbt.contracts.results"
    assert CatalogArtifact.__module__ == "dbt.contracts.results"
    assert FreshnessResultsArtifact.__module__ == "dbt.contracts.results"


def test_runner_result_has_result_attr():
    from dbt.cli.main import dbtRunnerResult

    res = dbtRunnerResult(success=True)
    assert hasattr(res, "result")
    assert res.result is None


def test_parse_result_is_manifest(tmp_project):
    proj = tmp_project("hello_world")
    res = _invoke(proj, "parse")

    assert res.success, res.exception
    # parse produces a manifest and no run_results, so .result is a Manifest.
    assert isinstance(res.result, Manifest), type(res.result)
    nodes = res.result.nodes
    assert isinstance(nodes, dict)
    # hello_world ships a single model.
    assert any(uid.startswith("model.") for uid in nodes), list(nodes)


def test_manifest_getters_pythonize(tmp_project):
    proj = tmp_project("hello_world")
    res = _invoke(proj, "parse")
    assert res.success, res.exception

    manifest = res.result
    # Nested data comes back as plain Python containers via pythonize.
    assert isinstance(manifest.sources, dict)
    assert isinstance(manifest.macros, dict)
    assert isinstance(manifest.metadata, dict)
    full = manifest.to_dict()
    assert isinstance(full, dict)
    assert "nodes" in full
