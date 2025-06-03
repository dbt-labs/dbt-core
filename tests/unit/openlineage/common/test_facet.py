from unittest.mock import Mock

import pytest

from core.dbt.events.types import FoundStats, NodeFinished, NodeStart
from core.dbt.openlineage.common.facets import (
    OPENLINEAGE_DBT_JOB_NAME_ENV_VAR,
    OPENLINEAGE_PARENT_ID_ENV_VAR,
    DbtOpenlineageJobType,
    ParentRunMetadata,
    _get_job_type,
    get_job_name,
    get_parent_run_facet,
)
from dbt.adapters.events.types import SQLQuery
from tests.openlineage_utils import dict_to_event_msg

DUMMY_UUID = "e3d27061-5519-4529-b357-93cd3b452245"


@pytest.mark.parametrize(
    "env_var, expected_parent_run_metadata_data",
    [
        (
            f"my-parent-job-namespace/my-parent-job-name/{DUMMY_UUID}",
            {
                "parent": ParentRunMetadata(
                    run_id=DUMMY_UUID,
                    job_name="my-parent-job-name",
                    job_namespace="my-parent-job-namespace",
                ).to_openlineage()
            },
        ),
        ("", {}),
    ],
    ids=["with_env_var", "without_env_var"],
)
def test_get_parent_run_facet(env_var, expected_parent_run_metadata_data, monkeypatch):
    monkeypatch.setenv(OPENLINEAGE_PARENT_ID_ENV_VAR, env_var)
    actual_parent_run_metadata_data = get_parent_run_facet()
    assert actual_parent_run_metadata_data == expected_parent_run_metadata_data


@pytest.mark.parametrize(
    "event, expected_job_type",
    [
        (
            {
                "info": {
                    "name": FoundStats.__name__,
                }
            },
            DbtOpenlineageJobType.job.value,
        ),
        ({"info": {"name": SQLQuery.__name__}}, DbtOpenlineageJobType.sql.value),
        (
            {
                "data": {
                    "node_info": {
                        "resource_type": "model",
                    }
                },
                "info": {"name": NodeStart.__name__},
            },
            DbtOpenlineageJobType.model.value,
        ),
        (
            {
                "data": {
                    "node_info": {
                        "resource_type": "seed",
                    }
                },
                "info": {"name": NodeStart.__name__},
            },
            DbtOpenlineageJobType.seed.value,
        ),
        (
            {
                "data": {
                    "node_info": {
                        "resource_type": "snapshot",
                    }
                },
                "info": {"name": NodeStart.__name__},
            },
            DbtOpenlineageJobType.snapshot.value,
        ),
    ],
    ids=["job", "sql", "model", "seed", "snapshot"],
)
def test_get_job_type(event, expected_job_type):
    event_message = dict_to_event_msg(event)

    actual_job_type = _get_job_type(event_message)

    assert actual_job_type == expected_job_type


@pytest.mark.parametrize(
    "env_var, event, expected_job_name",
    [
        (
            "my-dbt-job",
            {
                "info": {
                    "name": FoundStats.__name__,
                }
            },
            "my-dbt-job",
        ),
        (
            "",
            {
                "info": {
                    "name": FoundStats.__name__,
                }
            },
            "dbt-run--project=my-dbt-project--profile=my-profile",
        ),
        (
            "",
            {
                "info": {
                    "name": NodeFinished.__name__,
                },
                "data": {"node_info": {"unique_id": "model.jaffle_shop.customers"}},
            },
            "model.jaffle_shop.customers",
        ),
    ],
    ids=["with_env_var", "without_env_var", "with_node_unique_id"],
)
def test_get_job_name(env_var, event, expected_job_name, monkeypatch):
    ctx, project, profile, flags = Mock(), Mock(), Mock(), Mock()
    event_message = dict_to_event_msg(event)
    project.project_name = "my-dbt-project"
    profile.profile_name = "my-profile"
    flags.select = flags.exclude = flags.selector = None

    monkeypatch.setenv(OPENLINEAGE_DBT_JOB_NAME_ENV_VAR, env_var)

    ctx.obj = {"project": project, "profile": profile, "flags": flags}

    actual_job_name = get_job_name(event_message, ctx)

    assert actual_job_name == expected_job_name
