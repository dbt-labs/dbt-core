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
        run_dbt(["compile"])

        os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")

        # update group name everywhere, modify model so it gets picked up
        write_file(modified_model_1_sql, "models", "model_1.sql")
        write_file(modified_schema_yml, "models", "schema.yml")

        results = run_dbt(["build", "-s", "state:modified", "--defer", "--state", "./state"])
        breakpoint()
        assert len(results) == 1
