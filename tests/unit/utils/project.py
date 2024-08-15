from unittest.mock import MagicMock

import pytest

from dbt.adapters.contracts.connection import QueryComment
from dbt.config import RuntimeConfig
from dbt.config.project import Project, RenderComponents, VarProvider
from dbt.config.selectors import SelectorConfig
from dbt.contracts.project import PackageConfig
from dbt_common.semver import VersionSpecifier


@pytest.fixture(scope="function")
def selector_config() -> SelectorConfig:
    return SelectorConfig.selectors_from_dict(
        data={
            "selectors": [
                {
                    "name": "my_selector",
                    "definition": "give me cats",
                    "default": True,
                }
            ]
        }
    )


@pytest.fixture(scope="function")
def project(selector_config: SelectorConfig) -> Project:
    return Project(
        project_name="test_project",
        version=1.0,
        project_root="doesnt/actually/exist",
        profile_name="test_profile",
        model_paths=["models"],
        macro_paths=["macros"],
        seed_paths=["seeds"],
        test_paths=["tests"],
        analysis_paths=["analyses"],
        docs_paths=["docs"],
        asset_paths=["assets"],
        target_path="target",
        snapshot_paths=["snapshots"],
        clean_targets=["target"],
        log_path="path/to/project/logs",
        packages_install_path="dbt_packages",
        packages_specified_path="packages.yml",
        quoting={},
        models={},
        on_run_start=[],
        on_run_end=[],
        dispatch=[{"macro_namespace": "dbt_utils", "search_order": ["test_project", "dbt_utils"]}],
        seeds={},
        snapshots={},
        sources={},
        data_tests={},
        unit_tests={},
        metrics={},
        semantic_models={},
        saved_queries={},
        exposures={},
        vars=VarProvider({}),
        dbt_version=[VersionSpecifier.from_version_string("0.0.0")],
        packages=PackageConfig([]),
        manifest_selectors={},
        selectors=selector_config,
        query_comment=QueryComment(),
        config_version=1,
        unrendered=RenderComponents({}, {}, {}),
        project_env_vars={},
        restrict_access=False,
        dbt_cloud={},
    )


@pytest.fixture
def mock_project():
    mock_project = MagicMock(RuntimeConfig)
    mock_project.cli_vars = {}
    mock_project.args = MagicMock()
    mock_project.args.profile = "test"
    mock_project.args.target = "test"
    mock_project.project_env_vars = {}
    mock_project.profile_env_vars = {}
    mock_project.project_target_path = "mock_target_path"
    mock_project.credentials = MagicMock()
    mock_project.clear_dependencies = MagicMock()
    return mock_project
