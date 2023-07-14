from typing import Dict, List, Optional

from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.postgres.relation import PostgresRelation


def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
    assert isinstance(relation, PostgresRelation)
    sql = f"""
    select
        'table' as relation_type
    from pg_tables
    where schemaname = '{relation.schema}'
    and tablename = '{relation.identifier}'
    union all
    select
        'view' as relation_type
    from pg_views
    where schemaname = '{relation.schema}'
    and viewname = '{relation.identifier}'
    union all
    select
        'materialized_view' as relation_type
    from pg_matviews
    where schemaname = '{relation.schema}'
    and matviewname = '{relation.identifier}'
    """
    results = project.run_sql(sql, fetch="all")
    if len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.name} found!")
    else:
        return results[0][0]


def query_indexes(project, relation: BaseRelation) -> List[Dict[str, str]]:
    assert isinstance(relation, PostgresRelation)
    # pulled directly from `postgres__describe_indexes_template` and manually verified
    sql = f"""
        select
            i.relname                                   as name,
            m.amname                                    as method,
            ix.indisunique                              as "unique",
            array_to_string(array_agg(a.attname), ',')  as column_names
        from pg_index ix
        join pg_class i
            on i.oid = ix.indexrelid
        join pg_am m
            on m.oid=i.relam
        join pg_class t
            on t.oid = ix.indrelid
        join pg_namespace n
            on n.oid = t.relnamespace
        join pg_attribute a
            on a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
        where t.relname ilike '{ relation.identifier }'
          and n.nspname ilike '{ relation.schema }'
          and t.relkind in ('r', 'm')
        group by 1, 2, 3
        order by 1, 2, 3
    """
    raw_indexes = project.run_sql(sql, fetch="all")
    indexes = [
        {
            header: value
            for header, value in zip(["name", "method", "unique", "column_names"], index)
        }
        for index in raw_indexes
    ]
    return indexes
