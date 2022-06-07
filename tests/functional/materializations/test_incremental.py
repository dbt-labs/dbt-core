import pytest
from dbt.tests.util import run_dbt


my_model_sql = """
  select 1 as fun
"""


@pytest.fixture(scope="class")
def models():
    return {"my_model.sql": my_model_sql}


def test_basic(project):
    results = run_dbt(["run"])
    assert len(results) == 1

    macro_func = project.adapter.get_incremental_strategy_macro("default")
    assert macro_func
    assert type(macro_func).__name__ == "MacroGenerator"

    macro_func = project.adapter.get_incremental_strategy_macro("append")
    assert macro_func
    assert type(macro_func).__name__ == "MacroGenerator"

    macro_func = project.adapter.get_incremental_strategy_macro("delete+insert")
    assert macro_func
    assert type(macro_func).__name__ == "MacroGenerator"

    # Note: the following 2 strategies aren't actually supported on Postgres,
    # so will probably need to be removed before final version.
    # Leaving here while developing
    macro_func = project.adapter.get_incremental_strategy_macro("merge")
    assert macro_func
    assert type(macro_func).__name__ == "MacroGenerator"

    macro_func = project.adapter.get_incremental_strategy_macro("insert_overwrite")
    assert macro_func
    assert type(macro_func).__name__ == "MacroGenerator"
