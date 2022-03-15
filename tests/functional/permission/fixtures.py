import pytest
from dbt.tests.fixtures.project import write_project_files


models__view_model_sql = """

select * from {{ this.schema }}.seed

"""


@pytest.fixture(scope="class")
def models():
    return {"view_model.sql": models__view_model_sql}


@pytest.fixture(scope="class")
def project_files(
    project_root,
    models,
):
    write_project_files(project_root, "models", models)
