"""
Tests for ref(), source(), and this resolution in set_sql_header blocks.

This addresses the core issue (#2793) where ref(), source(), this, and is_incremental()
resolve incorrectly at parse time within set_sql_header blocks, causing runtime errors.

The fix implements two-phase rendering:
1. Parse time: Extract and render the sql_header template to capture dependencies for the DAG
2. Runtime: Re-render the template with runtime context for correct SQL generation

Note: These tests only cover set_sql_header blocks. Other Jinja patterns like
{% set %} blocks with pre_hooks have different resolution behavior and are out of scope.
"""

import pytest

from dbt.tests.util import get_manifest, run_dbt
from tests.functional.configs.fixtures import (
    macros__custom_alias,
    macros__custom_ref_macro,
    models__base_model,
    models__combination_header,
    models__conditional_header,
    models__custom_schema_model,
    models__ephemeral_with_header,
    models__incremental_header,
    models__multiple_refs_header,
    models__nested_macro_header,
    models__ref_a,
    models__ref_b,
    models__ref_custom_schema,
    models__source_in_header,
    models__test_tmp_1,
    models__test_tmp_2,
    models__this_with_alias,
    models__view_with_header,
    seeds__source_seed,
    sources__schema_yml,
)


class TestBasicRefInSqlHeader:
    """Test for issue #2793: ref() in set_sql_header resolves to wrong model"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_tmp_1.sql": models__test_tmp_1,
            "test_tmp_2.sql": models__test_tmp_2,
        }

    def test_ref_resolves_correctly(self, project):
        """Verify ref('test_tmp_1') in set_sql_header resolves to test_tmp_1, not test_tmp_2"""
        # Before the fix, this would fail with "relation does not exist" error
        # because ref('test_tmp_1') would incorrectly resolve to test_tmp_2
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

        # Verify dependencies are tracked correctly
        manifest = get_manifest(project.project_root)
        model_id = "model.test.test_tmp_2"
        assert model_id in manifest.nodes

        # test_tmp_2 should depend on test_tmp_1
        deps = manifest.nodes[model_id].depends_on.nodes
        assert "model.test.test_tmp_1" in deps


class TestIsIncrementalInSqlHeader:
    """Test for issue #3264: is_incremental() in set_sql_header"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_header.sql": models__incremental_header,
        }

    def test_is_incremental_evaluates_correctly(self, project):
        """Verify is_incremental() evaluates correctly in set_sql_header"""
        # First run - should not be incremental
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run - should be incremental
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # The fact that both runs succeeded means is_incremental() is working
        # If it wasn't, the SQL in sql_header would be malformed


class TestNestedMacroInSqlHeader:
    """Test for issue #4692: Nested macro calls in set_sql_header"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": models__base_model,
            "nested_macro_header.sql": models__nested_macro_header,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_ref_macro.sql": macros__custom_ref_macro,
        }

    def test_nested_macro_ref_resolves(self, project):
        """Verify macro that calls ref() works in set_sql_header"""
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

        # Verify macro dependency is tracked
        manifest = get_manifest(project.project_root)
        model_id = "model.test.nested_macro_header"
        assert "macro.test.get_ref_in_macro" in manifest.nodes[model_id].depends_on.macros

        # Verify ref dependency is tracked
        assert "model.test.base_model" in manifest.nodes[model_id].depends_on.nodes


class TestSourceInSqlHeader:
    """Test for issue #6058: source() in set_sql_header"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source_seed.csv": seeds__source_seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources__schema_yml,
            "source_in_header.sql": models__source_in_header,
            # Note: source_in_set_block is NOT using set_sql_header, so it's out of scope for this fix
            # "source_in_set_block.sql": models__source_in_set_block,
        }

    def test_source_in_header_resolves(self, project):
        """Verify source() in set_sql_header resolves correctly"""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1  # Changed from 2
        assert all(r.status == "success" for r in results)

        # Verify source dependency is tracked
        manifest = get_manifest(project.project_root)
        model_id = "model.test.source_in_header"
        assert "source.test.test_source.source_seed" in manifest.nodes[model_id].depends_on.nodes

    # Removed test_source_in_set_block_resolves - it doesn't use set_sql_header so it's out of scope


class TestThisWithCustomAlias:
    """Test for issue #7151: this with custom generate_alias_name"""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "custom_alias.sql": macros__custom_alias,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "this_with_alias.sql": models__this_with_alias,
        }

    def test_this_uses_custom_alias(self, project):
        """Verify {{ this }} in set_sql_header uses custom alias"""
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        manifest = get_manifest(project.project_root)
        model_id = "model.test.this_with_alias"
        node = manifest.nodes[model_id]

        # Verify custom alias is used
        assert node.alias == "custom_this_with_alias"


class TestMultipleRefsInSqlHeader:
    """Test multiple ref() calls in set_sql_header"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ref_a.sql": models__ref_a,
            "ref_b.sql": models__ref_b,
            "multiple_refs_header.sql": models__multiple_refs_header,
        }

    def test_multiple_refs_resolve(self, project):
        """Verify multiple ref() calls in set_sql_header all resolve correctly"""
        results = run_dbt(["run"])
        assert len(results) == 3
        assert all(r.status == "success" for r in results)

        # Verify dependencies are tracked
        manifest = get_manifest(project.project_root)
        model_id = "model.test.multiple_refs_header"
        deps = manifest.nodes[model_id].depends_on.nodes
        assert "model.test.ref_a" in deps
        assert "model.test.ref_b" in deps


class TestCombinationRefSourceThis:
    """Test combination of ref, source, and this in set_sql_header"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source_seed.csv": seeds__source_seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": sources__schema_yml,
            "base_model.sql": models__base_model,
            "combination_header.sql": models__combination_header,
        }

    def test_combination_resolves_correctly(self, project):
        """Verify ref, source, and this all work together in set_sql_header"""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

        # Verify dependencies are tracked
        manifest = get_manifest(project.project_root)
        model_id = "model.test.combination_header"
        deps = manifest.nodes[model_id].depends_on.nodes
        assert "model.test.base_model" in deps
        assert "source.test.test_source.source_seed" in deps


class TestDifferentMaterializations:
    """Test set_sql_header with different materializations"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": models__base_model,
            "view_with_header.sql": models__view_with_header,
            "ephemeral_with_header.sql": models__ephemeral_with_header,
        }

    def test_materializations_with_header(self, project):
        """Verify set_sql_header works with view and ephemeral materializations"""
        results = run_dbt(["run"])
        # base_model and view_with_header should create relations
        # ephemeral models don't show up in run results (they're only compiled)
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

        manifest = get_manifest(project.project_root)

        # Check view
        view_id = "model.test.view_with_header"
        assert manifest.nodes[view_id].config.materialized == "view"
        assert "model.test.base_model" in manifest.nodes[view_id].depends_on.nodes

        # Check ephemeral
        ephemeral_id = "model.test.ephemeral_with_header"
        assert manifest.nodes[ephemeral_id].config.materialized == "ephemeral"
        assert "model.test.base_model" in manifest.nodes[ephemeral_id].depends_on.nodes


class TestRefWithCustomDatabaseSchema:
    """Test for issue #2921: ref() with custom database/schema in set_sql_header"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_schema_model.sql": models__custom_schema_model,
            "ref_custom_schema.sql": models__ref_custom_schema,
        }

    def test_ref_with_custom_schema_resolves(self, project):
        """Verify ref() resolves correctly when referencing model with custom schema"""
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

        manifest = get_manifest(project.project_root)

        # Check that custom_schema_model has custom schema
        custom_model_id = "model.test.custom_schema_model"
        assert manifest.nodes[custom_model_id].schema != project.test_schema

        # Check that ref_custom_schema depends on custom_schema_model
        ref_model_id = "model.test.ref_custom_schema"
        assert "model.test.custom_schema_model" in manifest.nodes[ref_model_id].depends_on.nodes


class TestComparisonAndBooleanOperators:
    """Test comparison (>, <, ==) and boolean (and, or, not) operators in set_sql_header"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "conditional_header.sql": models__conditional_header,
        }

    def test_comparison_and_boolean_operators(self, project):
        """Verify comparison and boolean operators work in set_sql_header blocks"""
        # First run (full refresh) - should succeed
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run (incremental) - should also succeed
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Run with var overrides to test different branches
        results = run_dbt(["run", "--vars", '{"enable_optimization": true, "threshold": 100}'])
        assert len(results) == 1
        assert results[0].status == "success"

        manifest = get_manifest(project.project_root)
        model_id = "model.test.conditional_header"
        assert model_id in manifest.nodes
        assert manifest.nodes[model_id].config.materialized == "incremental"
