import pytest

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import CompilationError, ParsingError
from dbt.tests.util import check_relations_equal, get_manifest, run_dbt
from tests.functional.concepts.fixtures import (
    base_only_model_sql,
    basic_concept_yml,
    invalid_concept_yml,
    multi_join_concept_yml,
    orders_report_sql,
    partial_join_model_sql,
    raw_customers_csv,
    raw_orders_csv,
    raw_products_csv,
    simple_concept_yml,
    stg_customers_sql,
    stg_orders_sql,
    stg_products_sql,
)


class TestBasicConcepts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "concept_schema.yml": basic_concept_yml,
            "stg_orders.sql": stg_orders_sql,
            "stg_customers.sql": stg_customers_sql,
            "orders_report.sql": orders_report_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_orders.csv": raw_orders_csv,
            "raw_customers.csv": raw_customers_csv,
        }

    def test_parse_basic_concept(self, project):
        """Test that a basic concept definition can be parsed."""
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)

        manifest = get_manifest(project.project_root)

        # Check that concept was parsed and stored in manifest
        assert "concept.test.orders" in manifest.concepts
        concept = manifest.concepts["concept.test.orders"]

        # Verify concept properties
        assert concept.name == "orders"
        assert concept.base_model == "stg_orders"
        assert concept.primary_key == "order_id"
        assert len(concept.columns) == 4  # order_id, customer_id, order_date, status
        assert len(concept.joins) == 1  # stg_customers join

        # Verify join properties
        join = concept.joins[0]
        assert join.name == "stg_customers"
        assert join.base_key == "customer_id"
        assert join.foreign_key == "id"
        assert join.alias == "customer"
        assert len(join.columns) == 2  # customer_name, email

    def test_compile_cref_usage(self, project):
        """Test that models using cref can be compiled."""
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success

        # Compile the project
        result = runner.invoke(["compile"])
        assert result.success

        manifest = get_manifest(project.project_root)

        # Check that the orders_report model was compiled
        assert "model.test.orders_report" in manifest.nodes
        compiled_node = manifest.nodes["model.test.orders_report"]

        # Verify that dependencies were tracked
        expected_deps = {"model.test.stg_orders", "model.test.stg_customers"}
        assert set(compiled_node.depends_on.nodes) == expected_deps

    def test_cref_sql_generation(self, project):
        """Test that cref generates correct SQL."""
        runner = dbtRunner()
        result = runner.invoke(["compile"])
        assert result.success

        manifest = get_manifest(project.project_root)
        compiled_node = manifest.nodes["model.test.orders_report"]

        # The compiled SQL should contain JOIN logic
        compiled_sql = compiled_node.compiled_code

        # Basic checks that the SQL was expanded
        assert "SELECT" in compiled_sql.upper()
        assert "FROM" in compiled_sql.upper()
        assert "LEFT JOIN" in compiled_sql.upper()

        # Should reference the base and joined models
        assert "stg_orders" in compiled_sql
        assert "stg_customers" in compiled_sql


class TestSimpleConcepts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_concept_schema.yml": simple_concept_yml,
            "stg_orders.sql": stg_orders_sql,
            "base_only.sql": base_only_model_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_orders.csv": raw_orders_csv,
        }

    def test_concept_with_no_joins(self, project):
        """Test concept that has no joins (only base columns)."""
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success

        manifest = get_manifest(project.project_root)
        assert "concept.test.simple_orders" in manifest.concepts

        concept = manifest.concepts["concept.test.simple_orders"]
        assert len(concept.joins) == 0
        assert len(concept.columns) == 4

    def test_base_only_cref_compilation(self, project):
        """Test that cref with only base columns compiles without joins."""
        runner = dbtRunner()
        result = runner.invoke(["compile"])
        assert result.success

        manifest = get_manifest(project.project_root)
        compiled_node = manifest.nodes["model.test.base_only"]

        # Should only depend on base model
        assert compiled_node.depends_on.nodes == ["model.test.stg_orders"]

        # Compiled SQL should not contain JOIN
        compiled_sql = compiled_node.compiled_code
        assert "JOIN" not in compiled_sql.upper()


class TestConceptErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_concept_schema.yml": invalid_concept_yml,
        }

    def test_invalid_concept_parsing(self, project):
        """Test that invalid concept definitions raise parsing errors."""
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert not result.success
        # Should fail because base_model is missing
        assert isinstance(result.exception, (ParsingError, Exception))


class TestMultiJoinConcepts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "multi_join_schema.yml": multi_join_concept_yml,
            "stg_orders.sql": stg_orders_sql,
            "stg_customers.sql": stg_customers_sql,
            "stg_products.sql": stg_products_sql,
            "partial_join.sql": partial_join_model_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_orders.csv": raw_orders_csv,
            "raw_customers.csv": raw_customers_csv,
            "raw_products.csv": raw_products_csv,
        }

    def test_multi_join_concept_parsing(self, project):
        """Test parsing concept with multiple joins."""
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success

        manifest = get_manifest(project.project_root)
        concept = manifest.concepts["concept.test.enriched_orders"]

        assert len(concept.joins) == 2
        join_names = [join.name for join in concept.joins]
        assert "stg_customers" in join_names
        assert "stg_products" in join_names

    def test_partial_join_compilation(self, project):
        """Test that only needed joins are included in compilation."""
        runner = dbtRunner()
        result = runner.invoke(["compile"])
        assert result.success

        manifest = get_manifest(project.project_root)
        compiled_node = manifest.nodes["model.test.partial_join"]

        # Should depend on base and both joined models
        # (conservative dependency tracking)
        expected_deps = {
            "model.test.stg_orders",
            "model.test.stg_customers",
            "model.test.stg_products",
        }
        assert set(compiled_node.depends_on.nodes) == expected_deps

        # Compiled SQL should contain both joins since we requested
        # columns from both (customer_name and product_name)
        compiled_sql = compiled_node.compiled_code
        assert "LEFT JOIN" in compiled_sql.upper()
        assert "stg_customers" in compiled_sql
        assert "stg_products" in compiled_sql
