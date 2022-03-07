import pytest
from dbt.tests.fixtures.project import write_project_files


models__one_sql = """
select 1 /failed
"""

models__two_sql = """
select 1 /failed
"""


@pytest.fixture
def models():
    return {"one.sql": models__one_sql, "two.sql": models__two_sql}


@pytest.fixture
def project_files(
    project_root,
    models,
):
    write_project_files(project_root, "models", models)
