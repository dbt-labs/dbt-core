import dbt.exceptions
import textwrap
import yaml
import unittest
from dbt.config.selectors import selector_config_from_data

from dbt.config.selectors import SelectorConfig


def get_selector_dict(txt: str) -> dict:
    txt = textwrap.dedent(txt)
    dct = yaml.safe_load(txt)
    return dct


class SelectorUnitTest(unittest.TestCase):
    def test_parse_multiple_excludes(self):
        dct = get_selector_dict(
            """\
            selectors:
                - name: mult_excl
                  definition:
                    union:
                      - method: tag
                        value: nightly
                      - exclude:
                         - method: tag
                           value: hourly
                      - exclude:
                         - method: tag
                           value: daily
            """
        )
        with self.assertRaisesRegex(
            dbt.exceptions.DbtSelectorsError, "cannot provide multiple exclude arguments"
        ):
            selector_config_from_data(dct)

    def test_parse_set_op_plus(self):
        dct = get_selector_dict(
            """\
            selectors:
                - name: union_plus
                  definition:
                    - union:
                       - method: tag
                         value: nightly
                       - exclude:
                          - method: tag
                            value: hourly
                    - method: tag
                      value: foo
            """
        )
        with self.assertRaisesRegex(
            dbt.exceptions.DbtSelectorsError, "Valid root-level selector definitions"
        ):
            selector_config_from_data(dct)

    def test_parse_multiple_methods(self):
        dct = get_selector_dict(
            """\
            selectors:
                - name: mult_methods
                  definition:
                    - tag:hourly
                    - tag:nightly
                    - fqn:start
            """
        )
        with self.assertRaisesRegex(
            dbt.exceptions.DbtSelectorsError, "Valid root-level selector definitions"
        ):
            selector_config_from_data(dct)

    def test_parse_set_with_method(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: mixed_syntaxes
                    definition:
                      key: value
                      method: tag
                      value: foo
                      union:
                        - method: tag
                          value: m1234
                        - exclude:
                          - method: tag
                            value: m5678
            """
        )
        with self.assertRaisesRegex(
            dbt.exceptions.DbtSelectorsError,
            "Only a single 'union' or 'intersection' key is allowed",
        ):
            selector_config_from_data(dct)

    def test_complex_sector(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: nightly_diet_snowplow
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
            """
        )
        selectors = selector_config_from_data(dct)
        assert isinstance(selectors, SelectorConfig)

    def test_exclude_not_list(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: summa_exclude
                    definition:
                      union:
                        - method: tag
                          value: nightly
                        - exclude:
                            method: tag
                            value: daily
            """
        )
        with self.assertRaisesRegex(dbt.exceptions.DbtSelectorsError, "Expected a list"):
            selector_config_from_data(dct)

    def test_invalid_key(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: summa_nothing
                    definition:
                      method: tag
                      key: nightly
            """
        )
        with self.assertRaisesRegex(dbt.exceptions.DbtSelectorsError, "Expected either 1 key"):
            selector_config_from_data(dct)

    def test_invalid_single_def(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: summa_nothing
                    definition:
                      fubar: tag
            """
        )
        with self.assertRaisesRegex(dbt.exceptions.DbtSelectorsError, "not a valid method name"):
            selector_config_from_data(dct)

    def test_method_no_value(self):
        dct = get_selector_dict(
            """\
                selectors:
                  - name: summa_nothing
                    definition:
                      method: tag
            """
        )
        with self.assertRaisesRegex(dbt.exceptions.DbtSelectorsError, "not a valid method name"):
            selector_config_from_data(dct)

    def test_multiple_default_true(self):
        """Test selector_config_from_data returns the correct error when multiple
        default values are set
        """
        dct = get_selector_dict(
            """\
                selectors:
                  - name: summa_nothing
                    definition:
                      method: tag
                      value: nightly
                    default: true
                  - name: summa_something
                    definition:
                      method: tag
                      value: daily
                    default: true
        """
        )
        with self.assertRaisesRegex(
            dbt.exceptions.DbtSelectorsError, "Found multiple selectors with `default: true`:"
        ):
            selector_config_from_data(dct)
