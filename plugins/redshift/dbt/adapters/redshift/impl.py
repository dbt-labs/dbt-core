from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions


class RedshiftAdapter(PostgresAdapter):
    ConnectionManager = RedshiftConnectionManager

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        # Redshift doesn't support cross-database queries, so we can ignore the
        # relation's database
        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema = '{}'".format(relation.schema)

        sql = """
            with bound_views as (
                select
                    ordinal_position,
                    table_schema,
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision || ',' || numeric_scale as numeric_size

                from information_schema.columns
                where table_name = '{table_name}'
            ),

            unbound_views as (
                select
                    ordinal_position,
                    view_schema,
                    col_name,
                    case
                        when col_type ilike 'character varying%' then
                            'character varying'
                        when col_type ilike 'numeric%' then 'numeric'
                        else col_type
                    end as col_type,
                    case
                        when col_type like 'character%'
                        then nullif(REGEXP_SUBSTR(col_type, '[0-9]+'), '')::int
                        else null
                    end as character_maximum_length,
                    case
                        when col_type like 'numeric%'
                        then nullif(REGEXP_SUBSTR(col_type, '[0-9,]+'), '')
                        else null
                    end as numeric_size

                from pg_get_late_binding_view_cols()
                cols(view_schema name, view_name name, col_name name,
                     col_type varchar, ordinal_position int)
                where view_name = '{table_name}'
            ),

            unioned as (
                select * from bound_views
                union all
                select * from unbound_views
            )

            select
                column_name,
                data_type,
                character_maximum_length,
                numeric_size

            from unioned
            where {schema_filter}
            order by ordinal_position
        """.format(table_name=relation.identifier,
                   schema_filter=schema_filter).strip()
        return sql

    def drop_relation(self, relation, model_name=None):
        """
        In Redshift, DROP TABLE ... CASCADE should not be used
        inside a transaction. Redshift doesn't prevent the CASCADE
        part from conflicting with concurrent transactions. If we do
        attempt to drop two tables with CASCADE at once, we'll often
        get the dreaded:

            table was dropped by a concurrent transaction

        So, we need to lock around calls to the underlying
        drop_relation() function.

        https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html
        """
        with self.connections.fresh_transaction(model_name):
            parent = super(RedshiftAdapter, self)
            return parent.drop_relation(relation, model_name)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "varchar(24)"
