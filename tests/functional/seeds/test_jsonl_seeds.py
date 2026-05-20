import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


# ---------------------------------------------------------------------------
# Shared seed content
# ---------------------------------------------------------------------------

seeds__basic_jsonl = """\
{"id":1,"name":"alpha","metadata":{"source":"manual","rank":1}}
{"id":2,"active":true,"metadata":{"source":"api","flags":["x","y"]}}
{"id":3,"name":null}
"""

seeds__basic_ndjson = """\
{"id":1,"name":"alpha"}
{"id":2,"name":"beta"}
"""

seeds__simple_csv = """\
id,name
1,alice
2,bob
"""

models__query_jsonl_sql = """
select * from {{ ref('basic') }}
"""

models__query_ndjson_sql = """
select * from {{ ref('basic_ndjson') }}
"""


# ---------------------------------------------------------------------------
# Discovery and basic loading
# ---------------------------------------------------------------------------


class TestJSONLSeedDiscovery:
    """JSONL and NDJSON seeds are discovered and loaded."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "basic.jsonl": seeds__basic_jsonl,
        }

    def test_jsonl_seed_discovered(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].node.name == "basic"

    def test_jsonl_seed_columns(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        column_names = [col.name for col in table.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "metadata" in column_names
        assert "active" in column_names

    def test_jsonl_seed_row_count(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        assert len(table.rows) == 3


class TestNDJSONSeedDiscovery:
    """NDJSON extension is also supported."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "basic_ndjson.ndjson": seeds__basic_ndjson,
        }

    def test_ndjson_seed_discovered(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].node.name == "basic_ndjson"


# ---------------------------------------------------------------------------
# Selective seed loading
# ---------------------------------------------------------------------------


class TestJSONLSeedSelect:
    """dbt seed --select works for JSONL seeds."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "basic.jsonl": seeds__basic_jsonl,
            "other.csv": seeds__simple_csv,
        }

    def test_select_jsonl_seed(self, project):
        results = run_dbt(["seed", "--select", "basic"])
        assert len(results) == 1
        assert results[0].node.name == "basic"


# ---------------------------------------------------------------------------
# Downstream ref()
# ---------------------------------------------------------------------------


class TestJSONLSeedRef:
    """Downstream models can ref() a JSONL seed."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "query_jsonl.sql": models__query_jsonl_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "basic.jsonl": seeds__basic_jsonl,
        }

    def test_ref_jsonl_seed(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


# ---------------------------------------------------------------------------
# Missing keys become null
# ---------------------------------------------------------------------------


class TestJSONLMissingKeys:
    """Keys missing from some rows become null."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "missing.jsonl": seeds__basic_jsonl,
        }

    def test_missing_keys_null(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        # Row 0 (id=1) should have active=None since row 1 introduces it
        # Row 2 (id=3) should have metadata=None and active=None
        rows = list(table.rows)
        # Row with id=3 is the third row
        assert rows[2]["active"] is None


# ---------------------------------------------------------------------------
# New columns discovered after row 1
# ---------------------------------------------------------------------------


class TestJSONLNewColumnsAfterFirstRow:
    """Columns that first appear in later rows are included."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "new_cols.jsonl": seeds__basic_jsonl,
        }

    def test_new_columns_included(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        column_names = [col.name for col in table.columns]
        # "active" first appears on row 2 — should still be a column
        assert "active" in column_names
        # Row 1 should have active=None
        rows = list(table.rows)
        assert rows[0]["active"] is None


# ---------------------------------------------------------------------------
# Nested objects and arrays become compact JSON strings
# ---------------------------------------------------------------------------


class TestJSONLNestedValues:
    """Nested objects and arrays are preserved as compact JSON strings."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "nested.jsonl": seeds__basic_jsonl,
        }

    def test_nested_object_compact_json(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        rows = list(table.rows)
        # Row 0: metadata should be a sorted compact JSON string
        metadata_0 = rows[0]["metadata"]
        assert isinstance(metadata_0, str)
        assert '"rank":1' in metadata_0
        assert '"source":"manual"' in metadata_0

    def test_nested_array_compact_json(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        rows = list(table.rows)
        # Row 1: metadata contains "flags": ["x", "y"]
        metadata_1 = rows[1]["metadata"]
        assert isinstance(metadata_1, str)
        assert '"flags":["x","y"]' in metadata_1

    def test_null_nested_value(self, project):
        results = run_dbt(["seed"])
        table = results[0].agate_table
        rows = list(table.rows)
        # Row 2: metadata is null
        assert rows[2]["metadata"] is None


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestJSONLTopLevelArrayError:
    """A top-level JSON array should produce a clear error."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "bad_array.jsonl": '[{"id": 1}]\n',
        }

    def test_top_level_array_error(self, project):
        results = run_dbt(["seed"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        msg = results[0].message
        assert "JSON object" in msg
        assert "list" in msg.lower() or "array" in msg.lower() or "got list" in msg


class TestJSONLTopLevelScalarError:
    """A top-level JSON scalar should produce a clear error."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "bad_scalar.jsonl": '"hello"\n',
        }

    def test_top_level_scalar_error(self, project):
        results = run_dbt(["seed"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        msg = results[0].message
        assert "JSON object" in msg


class TestJSONLMalformedError:
    """Malformed JSON should produce a clear error with line number."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "bad_malformed.jsonl": '{"id": 1}\n{"bad": true\n',
        }

    def test_malformed_json_error(self, project):
        results = run_dbt(["seed"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        msg = results[0].message
        assert "Could not parse JSON" in msg


class TestJSONLEmptyFile:
    """An empty JSONL file should produce a clear error."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "empty.jsonl": "",
        }

    def test_empty_file_error(self, project):
        results = run_dbt(["seed"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


class TestJSONLEmptyLinesIgnored:
    """Empty and whitespace-only lines are ignored."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "empty_lines.jsonl": '{"id":1}\n\n  \n{"id":2}\n',
        }

    def test_empty_lines_ignored(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        table = results[0].agate_table
        assert len(table.rows) == 2


# ---------------------------------------------------------------------------
# CSV seeds still work alongside JSONL
# ---------------------------------------------------------------------------


class TestCSVStillWorks:
    """Existing CSV seed behavior is unchanged."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "basic.jsonl": seeds__basic_jsonl,
            "simple.csv": seeds__simple_csv,
        }

    def test_csv_seed_loaded(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        csv_result = next(r for r in results if r.node.name == "simple")
        table = csv_result.agate_table
        rows = list(table.rows)
        assert len(rows) == 2

    def test_jsonl_seed_loaded_alongside_csv(self, project):
        results = run_dbt(["seed"])
        jsonl_result = next(r for r in results if r.node.name == "basic")
        assert jsonl_result.agate_table is not None


# ---------------------------------------------------------------------------
# column_types for JSONL seeds
# ---------------------------------------------------------------------------


class TestJSONLColumnTypes:
    """column_types config works for JSONL seeds."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": """
version: 2
seeds:
  - name: typed
    config:
      column_types:
        id: integer
        name: text
""",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "typed.jsonl": '{"id":1,"name":"alice"}\n{"id":2,"name":"bob"}\n',
        }

    def test_column_types_jsonl(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].agate_table is not None


class TestJSONLColumnTypesNonExistent:
    """column_types for non-existent JSONL columns produces a warning."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": """
version: 2
seeds:
  - name: typed_warn
    config:
      column_types:
        id: integer
        nonexistent_column: text
""",
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "typed_warn.jsonl": '{"id":1,"name":"alice"}\n',
        }

    def test_nonexistent_column_warning(self, project):
        results, log_output = run_dbt_and_capture(["seed"])
        assert len(results) == 1
        assert "nonexistent_column" in log_output
