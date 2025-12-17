import shutil
from pathlib import Path

import pytest

from dbt.events.types import PackageNodeDependsOnRootProjectNode
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher


class BaseInvertedRefDependencyTest(object):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "a.sql": "select 1 as id",
        }

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        shutil.copytree(
            project.test_dir / Path("inverted_ref_dependency"),
            project.project_root / Path("inverted_ref_dependency"),
        )

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "inverted_ref_dependency"}]}


class TestInvertedRefDependency(BaseInvertedRefDependencyTest):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_ref_searches_node_package_before_root": True,
            }
        }

    def test_inverted_ref_dependency(self, project):
        event_catcher = EventCatcher(PackageNodeDependsOnRootProjectNode)
        run_dbt(["deps"])

        manifest = run_dbt(["parse"], callbacks=[event_catcher.catch])

        assert len(manifest.nodes) == 4
        # Correct behavior - package node depends on node from same package
        assert manifest.nodes["model.inverted_ref_dependency.b"].depends_on.nodes == [
            "model.inverted_ref_dependency.a"
        ]
        # If a package explicitly references a root project node, it still resolves to root project
        manifest.nodes["model.inverted_ref_dependency.b_root_package_in_ref"].depends_on.nodes == [
            "model.test.a"
        ]

        # No inverted ref warning raised
        assert len(event_catcher.caught_events) == 0


class TestInvertedRefDependencyLegacy(BaseInvertedRefDependencyTest):
    def test_inverted_ref_dependency(self, project):
        event_catcher = EventCatcher(PackageNodeDependsOnRootProjectNode)
        run_dbt(["deps"])

        manifest = run_dbt(["parse"], callbacks=[event_catcher.catch])

        assert len(manifest.nodes) == 4
        # Legacy behavior - package node depends on node from root project
        assert manifest.nodes["model.inverted_ref_dependency.b"].depends_on.nodes == [
            "model.test.a"
        ]
        assert manifest.nodes[
            "model.inverted_ref_dependency.b_root_package_in_ref"
        ].depends_on.nodes == ["model.test.a"]

        # Inverted ref warning raised - only for b, not b_root_package_in_ref
        assert len(event_catcher.caught_events) == 1
        assert event_catcher.caught_events[0].data.node_name == "b"
        assert event_catcher.caught_events[0].data.package_name == "inverted_ref_dependency"
