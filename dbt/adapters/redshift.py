import multiprocessing

from dbt.adapters.postgres import PostgresAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


drop_lock = multiprocessing.Lock()


class RedshiftAdapter(PostgresAdapter):

    @classmethod
    def type(cls):
        return 'redshift'

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def _get_columns_in_table_sql(cls, schema_name, table_name):
        # TODO : how do we make this a macro?
        if schema_name is None:
            table_schema_filter = '1=1'
        else:
            table_schema_filter = "table_schema = '{schema_name}'".format(
                    schema_name=schema_name)

        sql = """
            with bound_views as (
                select
                    table_schema,
                    column_name,
                    data_type,
                    character_maximum_length

                from information_schema.columns
                where table_name = '{table_name}'
            ),

            unbound_views as (
                select
                    view_schema,
                    col_name,
                    col_type,
                    case
                        when col_type like 'character%'
                          then REGEXP_SUBSTR(col_type, '[0-9]+')::int
                        else null
                    end as character_maximum_length

                from pg_get_late_binding_view_cols()
                cols(view_schema name, view_name name, col_name name,
                     col_type varchar, col_num int)
                where view_name = '{table_name}'
            ),

            unioned as (
                select * from bound_views
                union all
                select * from unbound_views
            )

            select column_name, data_type, character_maximum_length
            from unioned
            where {table_schema_filter}
        """.format(table_name=table_name,
                   table_schema_filter=table_schema_filter).strip()
        return sql

    @classmethod
    def drop(cls, profile, schema, relation, relation_type, model_name=None):
        global drop_lock

        to_return = None

        try:
            drop_lock.acquire()

            connection = cls.get_connection(profile, model_name)

            if connection.get('transaction_open'):
                cls.commit(profile, connection)

            cls.begin(profile, connection.get('name'))

            to_return = super(PostgresAdapter, cls).drop(
                profile, schema, relation, relation_type, model_name)

            cls.commit(profile, connection)
            cls.begin(profile, connection.get('name'))

            return to_return

        finally:
            drop_lock.release()
