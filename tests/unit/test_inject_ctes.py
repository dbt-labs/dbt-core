from dbt.compilation import inject_ctes_into_sql
from dbt.contracts.graph.nodes import InjectedCTE
import re


def norm_whitespace(string):
    _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
    string = _RE_COMBINE_WHITESPACE.sub(" ", string).strip()
    return string


def test_inject_ctes_simple1():
    starting_sql = "select * from __dbt__cte__base"
    ctes = [
        InjectedCTE(
            id="model.test.base",
            sql=" __dbt__cte__base as (\n\n\nselect * from test16873767336887004702_test_ephemeral.seed\n)",
        )
    ]
    expected_sql = """with __dbt__cte__base as (
        select * from test16873767336887004702_test_ephemeral.seed
        ) select * from __dbt__cte__base"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_simple2():
    starting_sql = "select * from __dbt__cte__ephemeral_level_two"
    ctes = [
        InjectedCTE(
            id="model.test.ephemeral_level_two",
            sql=' __dbt__cte__ephemeral_level_two as (\n\nselect * from "dbt"."test16873757769710148165_test_ephemeral"."source_table"\n)',
        )
    ]
    expected_sql = """with __dbt__cte__ephemeral_level_two as (
        select * from "dbt"."test16873757769710148165_test_ephemeral"."source_table"
        ) select * from __dbt__cte__ephemeral_level_two"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_multiple_ctes():

    starting_sql = "select * from __dbt__cte__ephemeral"
    ctes = [
        InjectedCTE(
            id="model.test.ephemeral_level_two",
            sql=' __dbt__cte__ephemeral_level_two as (\n\nselect * from "dbt"."test16873735573223965828_test_ephemeral"."source_table"\n)',
        ),
        InjectedCTE(
            id="model.test.ephemeral",
            sql=" __dbt__cte__ephemeral as (\n\nselect * from __dbt__cte__ephemeral_level_two\n)",
        ),
    ]
    expected_sql = """with __dbt__cte__ephemeral_level_two as (
            select * from "dbt"."test16873735573223965828_test_ephemeral"."source_table"
        ),  __dbt__cte__ephemeral as (
            select * from __dbt__cte__ephemeral_level_two
        ) select * from __dbt__cte__ephemeral"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_multiple_ctes_more_complex():
    starting_sql = """select * from __dbt__cte__female_only
        union all
        select * from "dbt"."test16873757723266827902_test_ephemeral"."double_dependent" where gender = 'Male'"""
    ctes = [
        InjectedCTE(
            id="model.test.base",
            sql=" __dbt__cte__base as (\n\n\nselect * from test16873757723266827902_test_ephemeral.seed\n)",
        ),
        InjectedCTE(
            id="model.test.base_copy",
            sql=" __dbt__cte__base_copy as (\n\n\nselect * from __dbt__cte__base\n)",
        ),
        InjectedCTE(
            id="model.test.female_only",
            sql=" __dbt__cte__female_only as (\n\n\nselect * from __dbt__cte__base_copy where gender = 'Female'\n)",
        ),
    ]
    expected_sql = """with __dbt__cte__base as (
            select * from test16873757723266827902_test_ephemeral.seed
        ),  __dbt__cte__base_copy as (
            select * from __dbt__cte__base
        ),  __dbt__cte__female_only as (
            select * from __dbt__cte__base_copy where gender = 'Female'
        ) select * from __dbt__cte__female_only
        union all
        select * from "dbt"."test16873757723266827902_test_ephemeral"."double_dependent" where gender = 'Male'"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_starting_with1():
    starting_sql = """
       with internal_cte as (select * from sessions)
       select * from internal_cte
    """
    ctes = [
        InjectedCTE(
            id="cte_id_1",
            sql="__dbt__cte__ephemeral as (select * from table)",
        ),
        InjectedCTE(
            id="cte_id_2",
            sql="__dbt__cte__events as (select id, type from events)",
        ),
    ]
    expected_sql = """with __dbt__cte__ephemeral as (select * from table),
       __dbt__cte__events as (select id, type from events),
       internal_cte as (select * from sessions)
       select * from internal_cte"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_starting_with2():
    starting_sql = """with my_other_cool_cte as (
        select id, name from __dbt__cte__ephemeral
        where id > 1000
    )
    select name, id from my_other_cool_cte"""
    ctes = [
        InjectedCTE(
            id="model.singular_tests_ephemeral.ephemeral",
            sql=' __dbt__cte__ephemeral as (\n\n\nwith my_cool_cte as (\n  select name, id from "dbt"."test16873917221900185954_test_singular_tests_ephemeral"."base"\n)\nselect id, name from my_cool_cte where id is not null\n)',
        )
    ]
    expected_sql = """with  __dbt__cte__ephemeral as (
        with my_cool_cte as (
          select name, id from "dbt"."test16873917221900185954_test_singular_tests_ephemeral"."base"
        )
        select id, name from my_cool_cte where id is not null
        ), my_other_cool_cte as (
            select id, name from __dbt__cte__ephemeral
            where id > 1000
        )
        select name, id from my_other_cool_cte"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_comment_with():
    # Test injection with a comment containing "with"
    starting_sql = """
        --- This is sql with a comment
        select * from __dbt__cte__base
    """
    ctes = [
        InjectedCTE(
            id="model.test.base",
            sql=" __dbt__cte__base as (\n\n\nselect * from test16873767336887004702_test_ephemeral.seed\n)",
        )
    ]
    expected_sql = """with __dbt__cte__base as (
        select * from test16873767336887004702_test_ephemeral.seed
        ) --- This is sql with a comment
        select * from __dbt__cte__base"""

    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)


def test_inject_ctes_with_recursive():
    # Test injection with "recursive" keyword
    starting_sql = """
        with recursive t(n) as (
            select * from __dbt__cte__first_ephemeral_model
          union all
            select n+1 from t where n < 100
        )
        select sum(n) from t
    """
    ctes = [
        InjectedCTE(
            id="model.test.first_ephemeral_model",
            sql=" __dbt__cte__first_ephemeral_model as (\n\nselect 1 as fun\n)",
        )
    ]
    expected_sql = """with recursive  __dbt__cte__first_ephemeral_model as (
        select 1 as fun
        ), t(n) as (
            select * from __dbt__cte__first_ephemeral_model
          union all
            select n+1 from t where n < 100
        )
        select sum(n) from t
    """
    generated_sql = inject_ctes_into_sql(starting_sql, ctes)
    assert norm_whitespace(generated_sql) == norm_whitespace(expected_sql)
