import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

seeds__my_seed_csv = """id,name,value
1,john,100
2,jane,200
3,bob,300
"""

my_seed_yml = """
version: 2
seeds:
  - name: my_seed
    config:
      column_types:
        id: integer
        name: text
        value: float
        # This column doesn't exist in the CSV
        non_existent_column: integer
"""

seeds__other_seed_csv = """id,name
1,john
2,jane
"""

other_seed_yml = """
version: 2
seeds:
  - name: other_seed
    config:
      column_types:
        id: integer
        name: text
"""

seeds__mismatched_columns_csv = """col_a,col_b,col_c
1,2,3
4,5,6
"""

mismatched_columns_yml = """
version: 2
seeds:
  - name: mismatched_columns
    config:
      column_types:
        col_a: integer
        col_b: text
        col_d: integer  # This doesn't exist
"""


class TestSeedColumnTypeValidation:
    """Test that column type validation works for seeds."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_seed.yml": my_seed_yml,
            "other_seed.yml": other_seed_yml,
            "mismatched_columns.yml": mismatched_columns_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": seeds__my_seed_csv,
            "other_seed.csv": seeds__other_seed_csv,
            "mismatched_columns.csv": seeds__mismatched_columns_csv,
        }

    def test_seed_with_invalid_column_type(self, project):
        results = run_dbt(["seed"])
        my_seed_result = next((r for r in results if r.node.name == "my_seed"), None)
        assert my_seed_result is not None
        assert my_seed_result.agate_table is not None

        column_names = [col.name for col in my_seed_result.agate_table.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "value" in column_names
        # non_existent_column should not be in the table
        assert "non_existent_column" not in column_names
        rows = list(my_seed_result.agate_table.rows)
        assert len(rows) == 3
        assert int(rows[0]["id"]) == 1
        assert rows[0]["name"] == "john"
        assert int(rows[0]["value"]) == 100


class TestSeedColumnTypesBasic:
    """Test basic seed column types functionality."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "other_seed.yml": other_seed_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "other_seed.csv": seeds__other_seed_csv,
        }

    def test_seed_basic_column_types(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        other_seed_result = next((r for r in results if r.node.name == "other_seed"), None)
        assert other_seed_result is not None

        assert other_seed_result.agate_table is not None

        column_names = [col.name for col in other_seed_result.agate_table.columns]
        assert "id" in column_names
        assert "name" in column_names


class TestSeedColumnTypesWithMismatchedColumns:
    """Test seed with column types for columns that don't exist in CSV."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mismatched_columns.yml": mismatched_columns_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "mismatched_columns.csv": seeds__mismatched_columns_csv,
        }

    def test_seed_invalid_column_type_warning(self, project):
        results, log_output = run_dbt_and_capture(["seed"])
        assert len(results) == 1

        result = next((r for r in results if r.node.name == "mismatched_columns"), None)
        assert result is not None
        assert result.agate_table is not None

        column_names = [col.name for col in result.agate_table.columns]
        assert "col_a" in column_names
        assert "col_b" in column_names
        assert "col_c" in column_names
        assert "col_d" not in column_names

        rows = list(result.agate_table.rows)
        assert len(rows) == 2
        assert int(rows[0]["col_a"]) == 1
        assert rows[0]["col_b"] == "2"
        assert int(rows[0]["col_c"]) == 3
        expected_msg_part = (
            "Column types specified for non-existent columns in seed 'mismatched_columns'"
        )

        assert expected_msg_part in log_output
        assert "col_d" in log_output
