import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import RuntimeException


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

    # These two incremental strategies are not valid for Postgres
    with pytest.raises(RuntimeException) as excinfo:
        macro_func = project.adapter.get_incremental_strategy_macro("merge")
    assert "merge" in str(excinfo.value)

    with pytest.raises(RuntimeException) as excinfo:
        macro_func = project.adapter.get_incremental_strategy_macro("insert_overwrite")
    assert "insert_overwrite" in str(excinfo.value)
