from unittest.mock import Mock

import pytest

from dbt.artifacts.resources.v1.concept import Concept, ConceptColumn, ConceptJoin
from dbt.context.providers import ParseConceptResolver, RuntimeConceptResolver
from dbt.contracts.graph.nodes import ParsedConcept
from dbt.contracts.graph.unparsed import (
    UnparsedConcept,
    UnparsedConceptColumn,
    UnparsedConceptJoin,
)


class TestConceptImplementation:
    def test_concept_column_creation(self):
        """Test that ConceptColumn can be created with basic attributes."""
        column = ConceptColumn(name="test_column", description="A test column")
        assert column.name == "test_column"
        assert column.description == "A test column"

    def test_concept_join_creation(self):
        """Test that ConceptJoin can be created with join attributes."""
        join = ConceptJoin(
            name="test_join",
            base_key="id",
            foreign_key="test_id",
            alias="test_alias",
            columns=[ConceptColumn(name="col1")],
        )
        assert join.name == "test_join"
        assert join.base_key == "id"
        assert join.foreign_key == "test_id"
        assert join.alias == "test_alias"
        assert len(join.columns) == 1

    def test_unparsed_concept_creation(self):
        """Test that UnparsedConcept can be created."""
        unparsed = UnparsedConcept(
            name="test_concept", base_model="base_table", primary_key="id", columns=[], joins=[]
        )
        assert unparsed.name == "test_concept"
        assert unparsed.base_model == "base_table"
        assert unparsed.primary_key == "id"

    def test_concept_resolver_initialization(self):
        """Test that concept resolvers can be initialized."""
        # Mock dependencies
        mock_db_wrapper = Mock()
        mock_model = Mock()
        mock_config = Mock()
        mock_manifest = Mock()

        # Add required attributes
        mock_config.project_name = "test_project"
        mock_db_wrapper.Relation = Mock()

        parse_resolver = ParseConceptResolver(
            db_wrapper=mock_db_wrapper,
            model=mock_model,
            config=mock_config,
            manifest=mock_manifest,
        )

        runtime_resolver = RuntimeConceptResolver(
            db_wrapper=mock_db_wrapper,
            model=mock_model,
            config=mock_config,
            manifest=mock_manifest,
        )

        assert parse_resolver.current_project == "test_project"
        assert runtime_resolver.current_project == "test_project"

    def test_concept_available_columns_mapping(self):
        """Test that RuntimeConceptResolver can map available columns."""
        # Mock dependencies
        mock_db_wrapper = Mock()
        mock_model = Mock()
        mock_config = Mock()
        mock_manifest = Mock()

        # Add required attributes
        mock_config.project_name = "test_project"
        mock_db_wrapper.Relation = Mock()

        resolver = RuntimeConceptResolver(
            db_wrapper=mock_db_wrapper,
            model=mock_model,
            config=mock_config,
            manifest=mock_manifest,
        )

        # Create a mock concept
        concept = Mock()
        concept.columns = [ConceptColumn(name="base_col1"), ConceptColumn(name="base_col2")]
        concept.joins = [
            ConceptJoin(
                name="join1",
                base_key="id",
                foreign_key="join_id",
                alias="j1",
                columns=[ConceptColumn(name="join_col1")],
            )
        ]

        available_columns = resolver._get_available_columns(concept)

        # Should include base columns and join columns
        assert "base_col1" in available_columns
        assert "base_col2" in available_columns
        assert "join_col1" in available_columns

        # Check column source mapping
        assert available_columns["base_col1"]["source"] == "base"
        assert available_columns["join_col1"]["source"] == "join"

    def test_determine_required_joins(self):
        """Test that RuntimeConceptResolver can determine required joins."""
        # Mock dependencies
        mock_db_wrapper = Mock()
        mock_model = Mock()
        mock_config = Mock()
        mock_manifest = Mock()

        # Add required attributes
        mock_config.project_name = "test_project"
        mock_db_wrapper.Relation = Mock()

        resolver = RuntimeConceptResolver(
            db_wrapper=mock_db_wrapper,
            model=mock_model,
            config=mock_config,
            manifest=mock_manifest,
        )

        # Create a mock concept for testing
        concept = Mock()
        concept.columns = [ConceptColumn(name="base_col")]
        concept.joins = [
            ConceptJoin(
                name="join1",
                alias="j1",
                base_key="id",
                foreign_key="join_id",
                columns=[ConceptColumn(name="join_col")],
            ),
            ConceptJoin(
                name="join2",
                alias="j2",
                base_key="id",
                foreign_key="join_id",
                columns=[ConceptColumn(name="other_join_col")],
            ),
        ]

        # Test with columns that require only one join
        requested_columns = ["base_col", "join_col"]
        required_joins = resolver._determine_required_joins(concept, requested_columns)

        # Should only include j1 join, not j2
        assert len(required_joins) == 1
        assert required_joins[0].alias == "j1"

        # Test with columns that require both joins
        requested_columns = ["base_col", "join_col", "other_join_col"]
        required_joins = resolver._determine_required_joins(concept, requested_columns)

        # Should include both joins
        assert len(required_joins) == 2
        aliases = [join.alias for join in required_joins]
        assert "j1" in aliases
        assert "j2" in aliases
