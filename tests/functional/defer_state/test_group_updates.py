import os
import shutil

import pytest

from dbt.tests.util import (
    run_dbt,
    write_file,
)

from tests.functional.defer_state.fixtures import (
    seed_csv,
)

model_1_sql = """
select * from {{ ref('seed') }}
"""

modified_model_1_sql = """
select * from  {{ ref('seed') }}
order by 1
"""

model_2_sql = """
select id from  {{ ref('model_1') }}
"""


schema_yml = """
groups:
  - name: finance
    owner:
      email: finance@jaffleshop.com

models:
  - name: model_1
    config:
      group: finance
  - name: model_2
"""


modified_schema_yml = """
groups:
  - name: accounting
    owner:
      email: finance@jaffleshop.com
models:
  - name: model_1
    config:
      group: accounting
  - name: model_2
"""


class TestModifiedGroups:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": model_1_sql,
            "model_2.sql": model_2_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    def test_changed_groups(self, project):
        # save initial state
        run_dbt(["seed"])
        results = run_dbt(["compile"])

        # add sanity checks for first result
        assert len(results) == 3
        seed_result = results[0].node
        assert seed_result.unique_id == "seed.test.seed"
        model_1_result = results[1].node
        assert model_1_result.unique_id == "model.test.model_1"
        assert model_1_result.group == "finance"
        model_2_result = results[2].node
        assert model_2_result.unique_id == "model.test.model_2"
        assert model_2_result.group is None

        os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")

        # update group name, modify model so it gets picked up
        write_file(modified_model_1_sql, "models", "model_1.sql")
        write_file(modified_schema_yml, "models", "schema.yml")

        # only thing in results should be model_1
        results = run_dbt(["build", "-s", "state:modified", "--defer", "--state", "./state"])

        assert len(results) == 1
        model_1_result = results[0].node
        assert model_1_result.unique_id == "model.test.model_1"
        assert model_1_result.group == "accounting"  # new group name!
