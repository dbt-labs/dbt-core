import pytest
from dbt.tests.util import run_dbt, write_file, get_manifest

model_one_sql = """
select 1 as fun
"""

metricflow_time_spine_sql = """
SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day
"""

schema1_yml = """
version: 2

models:
    - name: model_one

semantic_models:
  - name: semantic_people
    model: ref('model_one')
    dimensions:
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: people
        agg: count
        expr: fun
    entities:
      - name: fun
        type: primary
    defaults:
      agg_time_dimension: created_at

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: people
    meta:
        my_meta: 'testing'

exposures:
  - name: proxy_for_dashboard
    description: "My Exposure"
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
"""

schema2_yml = """
version: 2

models:
    - name: model_one

semantic_models:
  - name: semantic_people
    model: ref('model_one')
    dimensions:
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: people
        agg: count
        expr: fun
    entities:
      - name: fun
        type: primary
    defaults:
      agg_time_dimension: created_at

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    config:
        enabled: false
    type: simple
    type_params:
      measure: people
    meta:
        my_meta: 'testing'

exposures:
  - name: proxy_for_dashboard
    description: "My Exposure"
    config:
        enabled: false
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
"""

schema3_yml = """
version: 2

models:
    - name: model_one

semantic_models:
  - name: semantic_people
    model: ref('model_one')
    dimensions:
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: people
        agg: count
        expr: fun
    entities:
      - name: fun
        type: primary
    defaults:
      agg_time_dimension: created_at

metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: people
    meta:
        my_meta: 'testing'
"""

schema4_yml = """
version: 2

models:
    - name: model_one

exposures:
  - name: proxy_for_dashboard
    description: "My Exposure"
    config:
        enabled: false
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
"""


class TestDisabled:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "schema.yml": schema1_yml,
        }

    def test_pp_disabled(self, project):
        expected_exposure = "exposure.test.proxy_for_dashboard"
        expected_metric = "metric.test.number_of_people"

        run_dbt(["seed"])
        manifest = run_dbt(["parse"])

        assert expected_exposure in manifest.exposures
        assert expected_metric in manifest.metrics
        assert expected_exposure not in manifest.disabled
        assert expected_metric not in manifest.disabled

        # Update schema file with disabled metric and exposure
        write_file(schema2_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert expected_exposure not in manifest.exposures
        assert expected_metric not in manifest.metrics
        assert expected_exposure in manifest.disabled
        assert expected_metric in manifest.disabled

        # Update schema file with enabled metric and exposure
        write_file(schema1_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert expected_exposure in manifest.exposures
        assert expected_metric in manifest.metrics
        assert expected_exposure not in manifest.disabled
        assert expected_metric not in manifest.disabled

        # Update schema file - remove exposure, enable metric
        write_file(schema3_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert expected_exposure not in manifest.exposures
        assert expected_metric in manifest.metrics
        assert expected_exposure not in manifest.disabled
        assert expected_metric not in manifest.disabled

        # Update schema file - add back exposure, remove metric
        write_file(schema4_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        assert expected_exposure not in manifest.exposures
        assert expected_metric not in manifest.metrics
        assert expected_exposure in manifest.disabled
        assert expected_metric not in manifest.disabled
