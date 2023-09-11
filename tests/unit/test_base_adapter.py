from argparse import Namespace
from unittest import mock
import pytest

from dbt.adapters.base.impl import BaseAdapter, ConstraintSupport
import dbt.flags as flags


class TestBaseAdapterConstraintRendering:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        flags.set_from_args(Namespace(), None)

    @pytest.fixture(scope="class")
    def connection_manager(request):
        mock_connection_manager = mock.Mock()
        mock_connection_manager.TYPE = "base"
        return mock_connection_manager

    column_constraints = [
        ([{"type": "check"}], ["column_name integer"]),
        ([{"type": "check", "name": "test_name"}], ["column_name integer"]),
        (
            [{"type": "check", "expression": "test expression"}],
            ["column_name integer check (test expression)"],
        ),
        ([{"type": "not_null"}], ["column_name integer not null"]),
        (
            [{"type": "not_null", "expression": "test expression"}],
            ["column_name integer not null test expression"],
        ),
        ([{"type": "unique"}], ["column_name integer unique"]),
        (
            [{"type": "unique", "expression": "test expression"}],
            ["column_name integer unique test expression"],
        ),
        ([{"type": "primary_key"}], ["column_name integer primary key"]),
        (
            [{"type": "primary_key", "expression": "test expression"}],
            ["column_name integer primary key test expression"],
        ),
        ([{"type": "foreign_key"}], ["column_name integer"]),
        (
            [{"type": "foreign_key", "expression": "other_table (c1)"}],
            ["column_name integer references other_table (c1)"],
        ),
        ([{"type": "check"}, {"type": "unique"}], ["column_name integer unique"]),
    ]

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", column_constraints)
    def test_render_raw_columns_constraints(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.ENFORCED for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_columns_constraints(
            {
                "column_name": {
                    "name": "column_name",
                    "data_type": "integer",
                    "constraints": constraints,
                }
            }
        )
        assert rendered_constraints == expected_rendered_constraints

    column_constraints_unsupported = [
        ([{"type": "check"}], ["column_name integer"]),
        ([{"type": "check", "expression": "test expression"}], ["column_name integer"]),
        ([{"type": "not_null"}], ["column_name integer"]),
        ([{"type": "not_null", "expression": "test expression"}], ["column_name integer"]),
        ([{"type": "unique"}], ["column_name integer"]),
        ([{"type": "unique", "expression": "test expression"}], ["column_name integer"]),
        ([{"type": "primary_key"}], ["column_name integer"]),
        ([{"type": "primary_key", "expression": "test expression"}], ["column_name integer"]),
        ([{"type": "foreign_key"}], ["column_name integer"]),
        ([{"type": "check"}, {"type": "unique"}], ["column_name integer"]),
    ]

    @pytest.mark.parametrize(
        "constraints,expected_rendered_constraints", column_constraints_unsupported
    )
    def test_render_raw_columns_constraints_unsupported(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.NOT_SUPPORTED
            for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_columns_constraints(
            {
                "column_name": {
                    "name": "column_name",
                    "data_type": "integer",
                    "constraints": constraints,
                }
            }
        )
        assert rendered_constraints == expected_rendered_constraints

    model_constraints = [
        ([{"type": "check"}], []),
        ([{"type": "check", "expression": "test expression"}], ["check (test expression)"]),
        (
            [{"type": "check", "expression": "test expression", "name": "test_name"}],
            ["constraint test_name check (test expression)"],
        ),
        ([{"type": "not_null"}], []),
        ([{"type": "not_null", "expression": "test expression"}], []),
        ([{"type": "unique", "columns": ["c1", "c2"]}], ["unique (c1, c2)"]),
        ([{"type": "unique", "columns": ["c1", "c2"]}], ["unique (c1, c2)"]),
        (
            [
                {
                    "type": "unique",
                    "columns": ["c1", "c2"],
                    "expression": "test expression",
                    "name": "test_name",
                }
            ],
            ["constraint test_name unique test expression (c1, c2)"],
        ),
        ([{"type": "primary_key", "columns": ["c1", "c2"]}], ["primary key (c1, c2)"]),
        (
            [{"type": "primary_key", "columns": ["c1", "c2"], "expression": "test expression"}],
            ["primary key test expression (c1, c2)"],
        ),
        (
            [
                {
                    "type": "primary_key",
                    "columns": ["c1", "c2"],
                    "expression": "test expression",
                    "name": "test_name",
                }
            ],
            ["constraint test_name primary key test expression (c1, c2)"],
        ),
        (
            [{"type": "foreign_key", "columns": ["c1", "c2"], "expression": "other_table (c1)"}],
            ["foreign key (c1, c2) references other_table (c1)"],
        ),
        (
            [
                {
                    "type": "foreign_key",
                    "columns": ["c1", "c2"],
                    "expression": "other_table (c1)",
                    "name": "test_name",
                }
            ],
            ["constraint test_name foreign key (c1, c2) references other_table (c1)"],
        ),
    ]

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", model_constraints)
    def test_render_raw_model_constraints(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.ENFORCED for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_model_constraints(constraints)
        assert rendered_constraints == expected_rendered_constraints

    @pytest.mark.parametrize("constraints,expected_rendered_constraints", model_constraints)
    def test_render_raw_model_constraints_unsupported(
        self, constraints, expected_rendered_constraints, request
    ):
        BaseAdapter.ConnectionManager = request.getfixturevalue("connection_manager")
        BaseAdapter.CONSTRAINT_SUPPORT = {
            constraint: ConstraintSupport.NOT_SUPPORTED
            for constraint in BaseAdapter.CONSTRAINT_SUPPORT
        }

        rendered_constraints = BaseAdapter.render_raw_model_constraints(constraints)
        assert rendered_constraints == []
