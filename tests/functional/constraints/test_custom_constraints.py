import os
import shutil

import pytest

from dbt.tests.util import run_dbt
from tests.functional.constraints.fixtures import (
    model_custom_column_constraint_schema_yml,
)


class TestCustomConstraintStateModified:
    """Regression test: state:modified selection must not raise KeyError for custom constraints.

    custom constraints are not in CONSTRAINT_SUPPORT because they are user-defined
    SQL expressions. Previously, same_contract() did a raw dict lookup that crashed
    with KeyError: <ConstraintType.custom: 'custom'> when comparing state manifests.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "{{ config(materialized='table') }} select 1 as id",
            "schema.yml": model_custom_column_constraint_schema_yml,
        }

    def test_state_modified_does_not_raise_on_custom_constraint(self, project):
        # Parse to produce a manifest, then save it as the "previous state"
        run_dbt(["parse"])
        os.makedirs("state", exist_ok=True)
        shutil.copyfile("target/manifest.json", "state/manifest.json")

        # state:modified selection triggers same_contract(); must not KeyError
        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 0
