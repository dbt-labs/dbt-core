import pytest
from unittest.mock import patch

from dbt.graph.selector_spec import SelectionCriteria, IndirectSelection


@pytest.mark.parametrize(
    "indirect_selection_value,expected_value",
    [(v, v) for v in IndirectSelection],
)
def test_selection_criteria_default_indirect_value(indirect_selection_value, expected_value):
    # Check selection criteria with indirect selection value would follow the resolved value in flags
    # if indirect selection is not specified in the selection criteria.
    with patch("dbt.graph.selector_spec.get_flags") as patched_get_flags:
        patched_get_flags.return_value.INDIRECT_SELECTION = indirect_selection_value
        patched_get_flags.INDIRECT_SELECTION = indirect_selection_value
        selection_dict_without_indirect_selection_specified = {
            "method": "path",
            "value": "models/marts/orders.sql",
            "children": False,
            "parents": False,
        }
        selection_criteria_without_indirect_selection_specified = (
            SelectionCriteria.selection_criteria_from_dict(
                selection_dict_without_indirect_selection_specified,
                selection_dict_without_indirect_selection_specified,
            )
        )
        assert (
            selection_criteria_without_indirect_selection_specified.indirect_selection
            == expected_value
        )
        selection_dict_without_indirect_selection_specified = {
            "method": "path",
            "value": "models/marts/orders.sql",
            "children": False,
            "parents": False,
            "indirect_selection": "buildable",
        }
        selection_criteria_with_indirect_selection_specified = (
            SelectionCriteria.selection_criteria_from_dict(
                selection_dict_without_indirect_selection_specified,
                selection_dict_without_indirect_selection_specified,
            )
        )
        assert (
            selection_criteria_with_indirect_selection_specified.indirect_selection == "buildable"
        )
