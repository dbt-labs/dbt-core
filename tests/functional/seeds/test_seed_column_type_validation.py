

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

# Model yaml with column_types config
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

# Empty model yaml for no column_types
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

# CSV with different column names
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
        # Run dbt seed - should succeed with a warning
        results = run_dbt(["seed"])

        # Check that we have 2 results (my_seed and other_seed) if run together,
        # but here we have 3 seeds defined.
        # run_dbt(["seed"]) runs all of them.

        # Find the my_seed result
        my_seed_result = next((r for r in results if r.node.name == "my_seed"), None)
        assert my_seed_result is not None

        # Check that the agate table was created
        assert my_seed_result.agate_table is not None

        # Verify the table has the correct columns (excluding the invalid one)
        column_names = [col.name for col in my_seed_result.agate_table.columns]
        print(f"DEBUG: Columns in 'my_seed' table: {column_names}")
        assert "id" in column_names
        assert "name" in column_names
        assert "value" in column_names
        # non_existent_column should not be in the table
        assert "non_existent_column" not in column_names
        print("DEBUG: Verified 'non_existent_column' was excluded from 'my_seed'.")

        # Check the rows
        rows = list(my_seed_result.agate_table.rows)
        assert len(rows) == 3
        # Agate rows are accessed by column name or index
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
        # Run dbt seed - should succeed
        results = run_dbt(["seed"])

        # Check that we have 1 result
        assert len(results) == 1

        # Find the other_seed result
        other_seed_result = next((r for r in results if r.node.name == "other_seed"), None)
        assert other_seed_result is not None

        # Check that the agate table was created
        assert other_seed_result.agate_table is not None

        # Verify the table has the correct columns
        column_names = [col.name for col in other_seed_result.agate_table.columns]
        print(f"DEBUG: Columns in 'other_seed' table: {column_names}")
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
        # Run dbt seed - should succeed with a warning
        results, log_output = run_dbt_and_capture(["seed"])

        # Check that we have 1 result
        assert len(results) == 1

        # Find the mismatched_columns result
        result = next((r for r in results if r.node.name == "mismatched_columns"), None)
        assert result is not None

        # Check that the agate table was created
        assert result.agate_table is not None

        # Verify the table has the correct columns (col_a, col_b, col_c only)
        column_names = [col.name for col in result.agate_table.columns]
        assert "col_a" in column_names
        assert "col_b" in column_names
        assert "col_c" in column_names
        # col_d should not be in the table
        assert "col_d" not in column_names

        # Check rows
        rows = list(result.agate_table.rows)
        assert len(rows) == 2
        assert int(rows[0]["col_a"]) == 1
        assert rows[0]["col_b"] == "2"
        assert int(rows[0]["col_c"]) == 3

        # Verify warning message using captured logs
        print("DEBUG: Checking logs for warning message...")
        if "Column type specified for non-existent column 'col_d'" in log_output:
            print("DEBUG: ✅ Found expected warning message regarding 'col_d' in logs.")
        else:
            print(f"DEBUG: ❌ Warning message NOT found. Logs:\n{log_output}")

        assert "Column type specified for non-existent column 'col_d'" in log_output
        assert "in seed 'mismatched_columns'" in log_output
