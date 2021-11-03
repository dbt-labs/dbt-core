import dbt.exceptions
import textwrap
import yaml
import unittest
from dbt.config.selectors import SelectorDict


def get_selector_dict(txt: str) -> dict:
    txt = textwrap.dedent(txt)
    dct = yaml.safe_load(txt)
    return dct


class SelectorUnitTest(unittest.TestCase):

    def test_compare_cli_non_cli(self):
        dct = get_selector_dict('''\
            selectors:
              - name: nightly_diet_snowplow
                description: "This uses more CLI-style syntax"
                definition:
                  union:
                    - intersection:
                        - '@source:snowplow'
                        - 'tag:nightly'
                    - 'models/export'
                    - exclude:
                        - intersection:
                            - 'package:snowplow'
                            - 'config.materialized:incremental'
                        - export_performance_timing
              - name: nightly_diet_snowplow_full
                description: "This is a fuller YAML specification"
                definition:
                  union:
                    - intersection:
                        - method: source
                          value: snowplow
                          childrens_parents: true
                        - method: tag
                          value: nightly
                    - method: path
                      value: models/export
                    - exclude:
                        - intersection:
                            - method: package
                              value: snowplow
                            - method: config.materialized
                              value: incremental
                        - method: fqn
                          value: export_performance_timing
            ''')

        sel_dict = SelectorDict.parse_from_selectors_list(dct['selectors'])
        assert(sel_dict)
        with_strings = sel_dict['nightly_diet_snowplow']['definition']
        no_strings = sel_dict['nightly_diet_snowplow_full']['definition']
        self.assertEqual(with_strings, no_strings)

    def test_single_string_definition(self):
        dct = get_selector_dict('''\
            selectors:
              - name: nightly_selector
                definition:
                  'tag:nightly'
            ''')

        sel_dict = SelectorDict.parse_from_selectors_list(dct['selectors'])
        assert(sel_dict)
        expected = {'method': 'tag', 'value': 'nightly'}
        definition = sel_dict['nightly_selector']['definition']
        self.assertEqual(expected, definition)


    def test_single_key_value_definition(self):
        dct = get_selector_dict('''\
            selectors:
              - name: nightly_selector
                definition:
                  tag: nightly
            ''')

        sel_dict = SelectorDict.parse_from_selectors_list(dct['selectors'])
        assert(sel_dict)
        expected = {'method': 'tag', 'value': 'nightly'}
        definition = sel_dict['nightly_selector']['definition']
        self.assertEqual(expected, definition)

    def test_parent_definition(self):
        dct = get_selector_dict('''\
            selectors:
              - name: kpi_nightly_selector
                definition:
                  '+exposure:kpi_nightly'
            ''')

        sel_dict = SelectorDict.parse_from_selectors_list(dct['selectors'])
        assert(sel_dict)
        expected = {'method': 'exposure', 'value': 'kpi_nightly', 'parents': True}
        definition = sel_dict['kpi_nightly_selector']['definition']
        self.assertEqual(expected, definition)

    def test_plus_definition(self):
        dct = get_selector_dict('''\
            selectors:
              - name: my_model_children_selector
                definition:
                  'my_model+2'
            ''')

        sel_dict = SelectorDict.parse_from_selectors_list(dct['selectors'])
        assert(sel_dict)
        expected = {'method': 'fqn', 'value': 'my_model', 'children': True, 'children_depth': '2'}
        definition = sel_dict['my_model_children_selector']['definition']
        self.assertEqual(expected, definition)
