import unittest
from unittest import mock

from dbt.contracts.graph.parsed import (
    ParsedMacro
)
from dbt.context.macro_resolver import MacroResolver


def mock_macro(name, package_name):
    macro = mock.MagicMock(
        __class__=ParsedMacro,
        package_name=package_name,
        resource_type='macro',
        unique_id=f'macro.{package_name}.{name}',
    )
    # Mock(name=...) does not set the `name` attribute, this does.
    macro.name = name
    return macro


class TestMacroResolver(unittest.TestCase):

    def test_resolver(self):
        data = [
            {'package_name': 'my_test', 'name': 'unique'},
            {'package_name': 'my_test', 'name': 'macro_xx'},
            {'package_name': 'one', 'name': 'unique'},
            {'package_name': 'one', 'name': 'not_null'},
            {'package_name': 'two', 'name': 'macro_a'},
            {'package_name': 'two', 'name': 'macro_b'},
        ]
        macros = {}
        for mdata in data:
            macro = mock_macro(mdata['name'], mdata['package_name'])
            macros[macro.unique_id] = macro
        resolver = MacroResolver(macros, 'my_test', ['one'])
        assert(resolver)
        self.assertEqual(resolver.get_macro_id('one', 'not_null'), 'macro.one.not_null')
