# import pytest
# from dbt.tests.fixtures.project import write_project_files


ref_macro__macros__ref_sql = """
{% macro ref(model_name) %}

{% endmacro %}
"""


# @pytest.fixture(scope="class")
# def basic():
#     return {"schema.yml": basic__schema_yml, "model_a.sql": basic__model_a_sql}

# @pytest.fixture(scope="class")
# def source_macro():
#     return {"schema.yml": source_macro__schema_yml, "models": {"model_a.sql": source_macro__models__model_a_sql}, "macros": {"source.sql": source_macro__macros__source_sql}}

# @pytest.fixture(scope="class")
# def ref_macro():
#     return {"schema.yml": ref_macro__schema_yml, "models": {"model_a.sql": ref_macro__models__model_a_sql}, "macros": {"ref.sql": ref_macro__macros__ref_sql}}

# @pytest.fixture(scope="class")
# def config_macro():
#     return {"schema.yml": config_macro__schema_yml, "models": {"model_a.sql": config_macro__models__model_a_sql}, "macros": {"config.sql": config_macro__macros__config_sql}}

# @pytest.fixture(scope="class")
# def project_files(project_root, basic, source_macro, ref_macro, config_macro,):
#     write_project_files(project_root, "basic", basic)
#     write_project_files(project_root, "source_macro", source_macro)
#     write_project_files(project_root, "ref_macro", ref_macro)
#     write_project_files(project_root, "config_macro", config_macro)
