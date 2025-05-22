from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.unparsed import (
    UnparsedConcept,
    UnparsedConceptColumn,
    UnparsedConceptJoin,
)
from dbt.exceptions import ParsingError
from dbt.parser.schema_yaml_readers import ConceptParser


class TestConceptParser:
    @pytest.fixture
    def mock_schema_parser(self):
        """Mock schema parser for testing."""
        schema_parser = Mock()
        schema_parser.manifest = Mock()
        schema_parser.manifest.add_concept = Mock()
        schema_parser.project = Mock()
        schema_parser.project.project_name = "test_project"
        schema_parser.get_fqn_prefix = Mock(return_value=["test_project"])
        return schema_parser

    @pytest.fixture
    def mock_yaml_block(self):
        """Mock YAML block for testing."""
        yaml_block = Mock()
        yaml_block.path = Mock()
        yaml_block.path.relative_path = "models/schema.yml"
        return yaml_block

    @pytest.fixture
    def concept_parser(self, mock_schema_parser, mock_yaml_block):
        """Create a ConceptParser instance for testing."""
        parser = ConceptParser(schema_parser=mock_schema_parser, yaml=mock_yaml_block)
        return parser

    def test_parse_basic_concept(self, concept_parser, mock_schema_parser):
        """Test parsing a basic concept definition."""
        # Create test concept data
        concept_data = {
            "name": "orders",
            "description": "Orders concept",
            "base_model": "stg_orders",
            "primary_key": "order_id",
            "columns": [
                {"name": "order_id", "description": "Primary key"},
                {"name": "customer_id", "description": "Foreign key"},
            ],
            "joins": [
                {
                    "name": "stg_customers",
                    "base_key": "customer_id",
                    "foreign_key": "id",
                    "alias": "customer",
                    "columns": [{"name": "customer_name"}, {"name": "email"}],
                }
            ],
        }

        # Create unparsed concept
        unparsed = UnparsedConcept(
            name=concept_data["name"],
            description=concept_data["description"],
            base_model=concept_data["base_model"],
            primary_key=concept_data["primary_key"],
            columns=[
                UnparsedConceptColumn(name=col["name"], description=col.get("description"))
                for col in concept_data["columns"]
            ],
            joins=[
                UnparsedConceptJoin(
                    name=join["name"],
                    base_key=join["base_key"],
                    foreign_key=join["foreign_key"],
                    alias=join["alias"],
                    columns=[UnparsedConceptColumn(name=col["name"]) for col in join["columns"]],
                )
                for join in concept_data["joins"]
            ],
        )

        # Parse the concept
        concept_parser.parse_concept(unparsed=unparsed)

        # The parse_concept method doesn't return the concept, it adds it to the manifest
        # So we'll verify it was called correctly
        mock_schema_parser.manifest.add_concept.assert_called_once()

        # Get the parsed concept from the call arguments
        call_args = mock_schema_parser.manifest.add_concept.call_args[0]
        parsed_concept = call_args[1]  # Second argument is the concept

        # Verify the parsed concept
        assert parsed_concept.name == "orders"
        assert parsed_concept.description == "Orders concept"
        assert parsed_concept.base_model == "stg_orders"
        assert parsed_concept.primary_key == "order_id"
        assert len(parsed_concept.columns) == 2
        assert len(parsed_concept.joins) == 1

        # Verify the join
        join = parsed_concept.joins[0]
        assert join.name == "stg_customers"
        assert join.base_key == "customer_id"
        assert join.foreign_key == "id"
        assert join.alias == "customer"
        assert len(join.columns) == 2

    def test_parse_concept_empty_base_model(self, concept_parser):
        """Test that parsing works with empty base_model."""
        concept_data = {
            "name": "invalid_concept",
            "base_model": "",  # Empty base model
            "columns": [{"name": "id"}],
        }

        unparsed = UnparsedConcept(
            name=concept_data["name"],
            base_model=concept_data["base_model"],
            columns=[UnparsedConceptColumn(name="id")],
        )

        # This should parse successfully but with empty base_model
        concept_parser.parse_concept(unparsed=unparsed)

        # Verify it was added to manifest
        concept_parser.manifest.add_concept.assert_called_once()

    def test_parse_concept_with_no_joins(self, concept_parser, mock_schema_parser):
        """Test parsing a concept with no joins."""
        concept_data = {
            "name": "simple_orders",
            "base_model": "stg_orders",
            "primary_key": "order_id",
            "columns": [{"name": "order_id"}, {"name": "status"}],
            "joins": [],
        }

        unparsed = UnparsedConcept(
            name=concept_data["name"],
            base_model=concept_data["base_model"],
            primary_key=concept_data["primary_key"],
            columns=[UnparsedConceptColumn(name=col["name"]) for col in concept_data["columns"]],
            joins=[],
        )

        concept_parser.parse_concept(unparsed=unparsed)

        mock_schema_parser.manifest.add_concept.assert_called_once()

        # Get the parsed concept from the call arguments
        call_args = mock_schema_parser.manifest.add_concept.call_args[0]
        parsed_concept = call_args[1]  # Second argument is the concept

        assert parsed_concept.name == "simple_orders"
        assert len(parsed_concept.joins) == 0
        assert len(parsed_concept.columns) == 2

    def test_parse_multiple_concepts(self, concept_parser, mock_schema_parser):
        """Test parsing multiple concepts in one file."""
        concepts_data = [
            {
                "name": "orders",
                "base_model": "stg_orders",
                "primary_key": "order_id",
                "columns": [{"name": "order_id"}],
                "joins": [],
            },
            {
                "name": "customers",
                "base_model": "stg_customers",
                "primary_key": "customer_id",
                "columns": [{"name": "customer_id"}],
                "joins": [],
            },
        ]

        unparsed_concepts = [
            UnparsedConcept(
                name=concept["name"],
                base_model=concept["base_model"],
                primary_key=concept["primary_key"],
                columns=[UnparsedConceptColumn(name="order_id")],
                joins=[],
            )
            for concept in concepts_data
        ]

        # Parse all concepts
        for unparsed in unparsed_concepts:
            concept_parser.parse_concept(unparsed=unparsed)

        # Should have called add_concept twice
        assert mock_schema_parser.manifest.add_concept.call_count == 2
