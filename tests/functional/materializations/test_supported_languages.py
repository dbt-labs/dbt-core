import pytest

from dbt.tests.util import run_dbt

custom_mat_tmpl = """
{% materialization custom_mat{} %}
    {%- set target_relation = this.incorporate(type='table') %}
    {% call statement('main') -%}
        select 1 as column1
    {%- endcall %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

models__sql_model = """
{{ config(materialized='custom_mat') }}
select 1 as fun
"""

models__py_model = """
def model(dbt, session):
    dbt.config(materialized='custom_mat')
    return
"""


class SupportedLanguageBase:
    model_map = {
        "sql": ("sql_model.sql", models__sql_model),
        "python": ("py_model.py", models__py_model),
    }

    @pytest.fixture(scope="class")
    def macros(self):
        custom_mat = custom_mat_tmpl.replace("{}", "")

        if hasattr(self, "supported_langs"):
            custom_mat = custom_mat_tmpl.replace(
                "{}", f", supported_languages=[{self.lang_list()}]"
            )
        return {"custom_mat.sql": custom_mat}

    @pytest.fixture(scope="class")
    def models(self):
        file_name, model = self.model_map[self.use_lang]
        return {file_name: model}

    def lang_list(self):
        return ", ".join([f"'{l}'" for l in self.supported_langs])

    def test_language(self, project):
        result = run_dbt(["run"], expect_pass=self.expect_pass)
        if not self.expect_pass:
            assert "only supports languages" in result.results[0].message


class TestSupportedLanguages_SupportsDefault_UsingSql(SupportedLanguageBase):
    use_lang = "sql"
    expect_pass = True


class TestSupportedLanguages_SupportsDefault_UsingPython(SupportedLanguageBase):
    use_lang = "python"
    expect_pass = False


class TestSupportedLanguages_SupportsSql_UsingSql(SupportedLanguageBase):
    supported_langs = ["sql"]
    use_lang = "sql"
    expect_pass = True


class TestSupportedLanguages_SuppotsSql_UsingPython(SupportedLanguageBase):
    supported_langs = ["sql"]
    use_lang = "python"
    expect_pass = False


class TestSupportedLanguages_SuppotsPython_UsingSql(SupportedLanguageBase):
    supported_langs = ["python"]
    use_lang = "sql"
    expect_pass = False


class TestSupportedLanguages_SuppotsPython_UsingPython(SupportedLanguageBase):
    supported_langs = ["python"]
    use_lang = "python"
    expect_pass = True


class TestSupportedLanguages_SuppotsSqlAndPython_UsingSql(SupportedLanguageBase):
    supported_langs = ["sql", "python"]
    use_lang = "sql"
    expect_pass = True


class TestSupportedLanguages_SuppotsSqlAndPython_UsingPython(SupportedLanguageBase):
    supported_langs = ["sql", "python"]
    use_lang = "python"
    expect_pass = True
