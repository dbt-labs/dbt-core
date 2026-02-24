import os

import pytest

from dbt.tests.util import run_dbt

MACROS__TAG_SELECTOR_SQL = """
{% macro selector_from_tag_env() %}
  {{ return({
    'method': 'tag',
    'value': env_var('SELECTOR_TAG', 'alpha')
  }) }}
{% endmacro %}
"""

SELECTORS_YML = """
selectors:
  - name: dynamic_tag_selector
    definition: "{{ selector_from_tag_env() }}"
"""

MODELS__MODEL_ALPHA_SQL = """
{{ config(tags=['alpha']) }}
select 1 as id
"""

MODELS__MODEL_BETA_SQL = """
{{ config(tags=['beta']) }}
select 2 as id
"""


class TestSelectorMacroRendering:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"tag_selector.sql": MACROS__TAG_SELECTOR_SQL}

    @pytest.fixture(scope="class")
    def selectors(self):
        return SELECTORS_YML

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_alpha.sql": MODELS__MODEL_ALPHA_SQL,
            "model_beta.sql": MODELS__MODEL_BETA_SQL,
        }

    def test_selector_macro_renders_with_env_var(self, project):
        os.environ["SELECTOR_TAG"] = "alpha"
        try:
            first = run_dbt(
                ["ls", "--resource-type", "model", "--selector", "dynamic_tag_selector"]
            )
            assert first == ["test.model_alpha"]

            os.environ["SELECTOR_TAG"] = "beta"
            second = run_dbt(
                ["ls", "--resource-type", "model", "--selector", "dynamic_tag_selector"]
            )
            assert second == ["test.model_beta"]
        finally:
            os.environ.pop("SELECTOR_TAG", None)
