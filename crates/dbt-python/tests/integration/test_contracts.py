"""The artifact shapes callers get back from an invocation."""

import pytest
from dbt.artifacts.schemas.base import BaseArtifactMetadata
from dbt.artifacts.schemas.catalog import CatalogTable
from dbt.artifacts.schemas.manifest import ManifestMetadata
from dbt.artifacts.schemas.run import RunResultOutput
from dbt.artifacts.schemas.sources import (
    FreshnessPeriod,
    FreshnessResultsArtifact,
    FreshnessStatus,
)
from dbt.contracts.graph.manifest import Manifest


def test_parse_result_is_manifest(tmp_project, invoke):
    res = invoke(tmp_project("layered"), "parse")

    assert res.success, res.exception
    assert isinstance(res.result, Manifest), type(res.result)
    assert any(uid.startswith("model.") for uid in res.result.nodes)


def test_manifest_shape(tmp_project, invoke):
    res = invoke(tmp_project("layered"), "parse")
    assert res.success, res.exception

    manifest = res.result
    # Collections stay plain dicts on purpose; see manifest.py.
    assert isinstance(manifest.metadata, ManifestMetadata)
    for name in ("nodes", "sources", "macros", "exposures", "groups"):
        assert isinstance(getattr(manifest, name), dict), name

    full = manifest.to_dict()
    assert isinstance(full, dict)
    assert "nodes" in full


def test_run_results_shape(tmp_project, invoke):
    res = invoke(tmp_project("layered"), "build")
    assert res.success, res.exception

    rr = res.result
    assert isinstance(rr.metadata, BaseArtifactMetadata)
    assert isinstance(rr.results, list)
    assert all(isinstance(r, RunResultOutput) for r in rr.results)
    assert isinstance(rr.args, dict)
    assert isinstance(rr.elapsed_time, float)


def test_catalog_shape(tmp_project, invoke):
    proj = tmp_project("layered")
    assert invoke(proj, "build").success
    res = invoke(proj, "compile", "--write-catalog")
    assert res.success, res.exception

    catalog = res.catalog
    assert isinstance(catalog.metadata, BaseArtifactMetadata)
    assert isinstance(catalog.nodes, dict)
    assert all(isinstance(t, CatalogTable) for t in catalog.nodes.values())
    assert isinstance(catalog.sources, dict)
    assert isinstance(catalog.to_dict(), dict)


def test_freshness_schema_decodes_a_sources_payload():
    """The schema exists ahead of the wiring, so exercise it directly."""
    artifact = FreshnessResultsArtifact.from_dict(
        {
            "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/sources/v3.json"},
            "elapsed_time": 0.0,
            "results": [
                {
                    "unique_id": "source.p.raw.events",
                    "max_loaded_at": "2020-01-02T00:00:00Z",
                    "snapshotted_at": "2026-01-01T00:00:00Z",
                    "max_loaded_at_time_ago_in_s": 1.0,
                    "status": "Pass",
                    "criteria": {
                        "error_after": {"count": 24, "period": "hour"},
                        "warn_after": {"count": 12, "period": "hour"},
                    },
                    "adapter_response": {},
                    "timing": [],
                    "thread_id": "main",
                    "execution_time": 0.5,
                }
            ],
        }
    )

    assert len(artifact) == 1
    node = artifact[0]
    assert node.unique_id == "source.p.raw.events"
    assert node.status is FreshnessStatus.PASS
    assert node.criteria.error_after.period is FreshnessPeriod.HOUR
    assert node.criteria.error_after.count == 24


@pytest.mark.xfail(
    reason="`source freshness` builds its artifact inside a hook that exits at a "
    "phase checkpoint before any capture point runs, so the engine never hands it "
    "back from invoke(); the schema above is ready for when it does",
    strict=True,
)
def test_source_freshness_result_is_freshness_artifact(tmp_project, invoke):
    res = invoke(tmp_project("layered"), "source", "freshness")
    assert isinstance(res.result, FreshnessResultsArtifact), type(res.result)
