"""Unit tests for _parse_v2_column_dimensions and _parse_v2_column_entities.

These methods transform column-level dimension/entity definitions into Dimension
and Entity objects for the semantic manifest. When a dimension or entity name
differs from the column name, the resulting object must have expr set to the
column name so that MetricFlow generates SQL against the correct warehouse column.
"""

from collections import OrderedDict

from dbt.artifacts.resources import ColumnDimension, ColumnEntity, ColumnInfo
from dbt.parser.schema_yaml_readers import SemanticModelParser
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType


def _make_column(name, description="", dimension=None, entity=None, granularity=None, config=None):
    """Helper to construct a ColumnInfo with only the fields we need."""
    return ColumnInfo(
        name=name,
        description=description,
        dimension=dimension,
        entity=entity,
        granularity=granularity,
        config=config or {},
    )


def _make_columns_dict(*columns):
    """Build an OrderedDict of ColumnInfo keyed by name, as the parser expects."""
    return OrderedDict((col.name, col) for col in columns)


class TestParseV2ColumnDimensionsExpr:
    """Test that _parse_v2_column_dimensions sets expr correctly."""

    def test_dimension_name_override_sets_expr_to_column_name(self):
        """When dimension.name differs from column.name, expr must be the column name."""
        columns = _make_columns_dict(
            _make_column(
                name="afe_number",
                description="AFE identifier",
                dimension=ColumnDimension(
                    name="approval_afe_number",
                    type=DimensionType.CATEGORICAL,
                ),
            ),
        )
        dimensions = SemanticModelParser._parse_v2_column_dimensions(None, columns)
        assert len(dimensions) == 1
        dim = dimensions[0]
        assert dim.name == "approval_afe_number"
        assert dim.expr == "afe_number"

    def test_dimension_name_matches_column_name_expr_is_none(self):
        """When dimension.name matches column.name, expr should be None."""
        columns = _make_columns_dict(
            _make_column(
                name="status",
                dimension=ColumnDimension(
                    name="status",
                    type=DimensionType.CATEGORICAL,
                ),
            ),
        )
        dimensions = SemanticModelParser._parse_v2_column_dimensions(None, columns)
        assert len(dimensions) == 1
        assert dimensions[0].name == "status"
        assert dimensions[0].expr is None

    def test_dimension_empty_name_defaults_to_column_name(self):
        """When dimension.name is empty string, name defaults to column.name and expr is None."""
        columns = _make_columns_dict(
            _make_column(
                name="category",
                dimension=ColumnDimension(
                    name="",
                    type=DimensionType.CATEGORICAL,
                ),
            ),
        )
        dimensions = SemanticModelParser._parse_v2_column_dimensions(None, columns)
        assert len(dimensions) == 1
        assert dimensions[0].name == "category"
        assert dimensions[0].expr is None

    def test_dimension_shorthand_type_expr_is_none(self):
        """When dimension is just a DimensionType (shorthand), expr should be None."""
        columns = _make_columns_dict(
            _make_column(
                name="color",
                dimension=DimensionType.CATEGORICAL,
            ),
        )
        dimensions = SemanticModelParser._parse_v2_column_dimensions(None, columns)
        assert len(dimensions) == 1
        assert dimensions[0].name == "color"
        assert dimensions[0].expr is None


class TestParseV2ColumnEntitiesExpr:
    """Test that _parse_v2_column_entities sets expr correctly."""

    def test_entity_name_override_sets_expr_to_column_name(self):
        """When entity.name differs from column.name, expr must be the column name."""
        columns = _make_columns_dict(
            _make_column(
                name="id",
                description="Primary key",
                entity=ColumnEntity(
                    name="id_entity",
                    type=EntityType.PRIMARY,
                ),
            ),
        )
        entities = SemanticModelParser._parse_v2_column_entities(None, columns)
        assert len(entities) == 1
        ent = entities[0]
        assert ent.name == "id_entity"
        assert ent.expr == "id"

    def test_entity_name_matches_column_name_expr_is_none(self):
        """When entity.name matches column.name, expr should be None."""
        columns = _make_columns_dict(
            _make_column(
                name="user_id",
                entity=ColumnEntity(
                    name="user_id",
                    type=EntityType.FOREIGN,
                ),
            ),
        )
        entities = SemanticModelParser._parse_v2_column_entities(None, columns)
        assert len(entities) == 1
        assert entities[0].name == "user_id"
        assert entities[0].expr is None

    def test_entity_shorthand_type_expr_is_none(self):
        """When entity is just an EntityType (shorthand), name defaults to column.name and expr is None."""
        columns = _make_columns_dict(
            _make_column(
                name="foreign_id_col",
                entity=EntityType.FOREIGN,
            ),
        )
        entities = SemanticModelParser._parse_v2_column_entities(None, columns)
        assert len(entities) == 1
        assert entities[0].name == "foreign_id_col"
        assert entities[0].expr is None
