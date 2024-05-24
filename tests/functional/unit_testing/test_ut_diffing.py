import pytest

from dbt.tests.util import run_dbt

my_input_model = """
SELECT 1 as id, 'some string' as status
"""

my_model = """
SELECT * FROM {{ ref("my_input_model") }}
"""

test_my_model_order_insensitive = """
unit_tests:
  - name: unordered_no_nulls
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id": 1, "status": 'B'}
          - {"id": 2, "status": 'B'}
          - {"id": 3, "status": 'A'}
    expect:
        rows:
          - {"id": 3, "status": 'A'}
          - {"id": 2, "status": 'B'}
          - {"id": 1, "status": 'B'}

  - name: unordered_with_nulls
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}
          - {"id": 3, "status": 'A'}
    expect:
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}

  - name: unordered_with_nulls_2
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}
    expect:
        rows:
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}
          - {"id": 3, "status": 'A'}

  - name: unordered_with_nulls_mixed_columns
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
          - {"id": 1, "status": }
    expect:
        rows:
          - {"id": 1, "status": }
          - {"id":  , "status": 'B'}
          - {"id": 3, "status": 'A'}

  - name: unordered_with_null
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
    expect:
        rows:
          - {"id":  , "status": 'B'}
          - {"id": 3, "status": 'A'}

  - name: ordered_with_nulls
    model: my_model
    given:
      - input: ref("my_input_model")
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}
    expect:
        rows:
          - {"id": 3, "status": 'A'}
          - {"id":  , "status": 'B'}
          - {"id":  , "status": 'B'}
"""


class TestUnitTestingDiffIsOrderAgnostic:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_input_model.sql": my_input_model,
            "my_model.sql": my_model,
            "test_my_model.yml": test_my_model_order_insensitive,
        }

    def test_unit_testing_diff_is_order_insensitive(self, project):
        run_dbt(["run"])

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=True)
        assert len(results) == 6
