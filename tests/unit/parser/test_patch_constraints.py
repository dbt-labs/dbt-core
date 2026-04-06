"""Unit tests for ModelPatchParser.patch_constraints behavior under require_valid_unenforced_constraints."""
import unittest
from argparse import Namespace
from unittest.mock import MagicMock, patch

from dbt.flags import set_from_args
from dbt.tests.util import safe_set_invocation_context
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.events.types import Note

set_from_args(Namespace(warn_error=False), None)


def _make_parser():
    """Return a ModelPatchParser with infrastructure mocked out, with the real patch_constraints."""
    from dbt.parser.schemas import ModelPatchParser

    parser = MagicMock(spec=ModelPatchParser)
    # Bind the real implementation so it actually runs
    parser.patch_constraints = lambda node, constraints: ModelPatchParser.patch_constraints(
        parser, node, constraints
    )
    # Stub out helpers called inside patch_constraints
    parser._validate_constraint_prerequisites = MagicMock()
    parser._validate_pk_constraints = MagicMock()
    parser._process_constraints_refs_and_sources = MagicMock()
    return parser


def _make_node(enforced: bool):
    """Return a minimal ModelNode mock with the given contract enforcement state."""
    contract = MagicMock()
    contract.enforced = enforced

    config = MagicMock()
    config.get.return_value = contract

    node = MagicMock()
    node.name = "my_model"
    node.config = config
    node.columns = {}
    node.constraints = []
    node.all_constraints = []
    return node


_INVALID_CONSTRAINT = {"columns": ["id"]}  # missing "type"
_VALID_CONSTRAINT = {"type": "not_null", "columns": ["id"]}


class TestPatchConstraintsUnenforced(unittest.TestCase):
    def setUp(self):
        safe_set_invocation_context()

    # ------------------------------------------------------------------
    # Flag = False (default): invalid constraint filtered, Note emitted
    # ------------------------------------------------------------------

    def test_invalid_constraint_filtered_when_flag_false(self):
        parser = _make_parser()
        node = _make_node(enforced=False)

        with patch(
            "dbt.parser.schemas.get_flags",
            return_value=MagicMock(require_valid_unenforced_constraints=False),
        ):
            parser.patch_constraints(node, [_INVALID_CONSTRAINT])

        assert node.constraints == []

    def test_valid_constraints_preserved_when_flag_false(self):
        parser = _make_parser()
        node = _make_node(enforced=False)

        with patch(
            "dbt.parser.schemas.get_flags",
            return_value=MagicMock(require_valid_unenforced_constraints=False),
        ):
            parser.patch_constraints(node, [_INVALID_CONSTRAINT, _VALID_CONSTRAINT])

        # Only the valid constraint survives
        assert len(node.constraints) == 1
        assert node.constraints[0].type == ConstraintType.not_null

    def test_note_fired_for_invalid_constraint(self):
        parser = _make_parser()
        node = _make_node(enforced=False)

        with patch(
            "dbt.parser.schemas.get_flags",
            return_value=MagicMock(require_valid_unenforced_constraints=False),
        ), patch("dbt.parser.schemas.fire_event") as mock_fire:
            parser.patch_constraints(node, [_INVALID_CONSTRAINT])

        fired_notes = [
            call.args[0] for call in mock_fire.call_args_list if isinstance(call.args[0], Note)
        ]
        assert fired_notes, "Expected at least one Note event to be fired"
        assert any("my_model" in n.msg for n in fired_notes), (
            "Expected a Note event mentioning the model name"
        )

    # ------------------------------------------------------------------
    # Flag = True: invalid constraint raises ParsingError
    # ------------------------------------------------------------------

    def test_invalid_constraint_raises_when_flag_true(self):
        from dbt.exceptions import ParsingError

        parser = _make_parser()
        node = _make_node(enforced=False)

        with patch(
            "dbt.parser.schemas.get_flags",
            return_value=MagicMock(require_valid_unenforced_constraints=True),
        ):
            with self.assertRaises(ParsingError) as ctx:
                parser.patch_constraints(node, [_INVALID_CONSTRAINT])

        assert "Invalid constraint type" in str(ctx.exception)
        assert "my_model" in str(ctx.exception)

    # ------------------------------------------------------------------
    # Enforced contract: strict validation unchanged regardless of flag
    # ------------------------------------------------------------------

    def test_enforced_contract_raises_regardless_of_flag(self):
        from dbt.exceptions import ParsingError

        for flag_value in (True, False):
            with self.subTest(flag_value=flag_value):
                parser = _make_parser()
                node = _make_node(enforced=True)

                with patch(
                    "dbt.parser.schemas.get_flags",
                    return_value=MagicMock(require_valid_unenforced_constraints=flag_value),
                ):
                    with self.assertRaises(ParsingError):
                        parser.patch_constraints(node, [_INVALID_CONSTRAINT])

    def test_enforced_contract_valid_constraints_pass(self):
        parser = _make_parser()
        node = _make_node(enforced=True)

        with patch(
            "dbt.parser.schemas.get_flags",
            return_value=MagicMock(require_valid_unenforced_constraints=False),
        ):
            parser.patch_constraints(node, [_VALID_CONSTRAINT])

        assert len(node.constraints) == 1
