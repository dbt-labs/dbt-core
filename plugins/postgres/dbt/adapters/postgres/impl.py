import psycopg2

import time

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


GET_RELATIONS_MACRO_NAME = 'get_relations'


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager

    @classmethod
    def date_function(cls):
        return 'datenow()'

    def _link_cached_database_relations(self, manifest, database, schemas):
        table = self.execute_macro(manifest, GET_RELATIONS_MACRO_NAME)

        for (refed_schema, refed_name, dep_schema, dep_name) in table:
            referenced = self.Relation.create(
                database=database,
                schema=refed_schema,
                identifier=refed_name
            )
            dependent = self.Relation.create(
                database=database,
                schema=dep_schema,
                identifier=dep_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(dependent, referenced)

    def _link_cached_relations(self, manifest):
        schemas = manifest.get_used_schemas()
        # make a map of {db: [schemas]}
        schema_map = {}
        for db, schema in schemas:
            schema_map.setdefault(db, []).append(schema.lower())

        try:
            for db, schemas in schema_map.items():
                self._link_cached_database_relations(manifest, db, schemas)
        finally:
            self.release_connection(GET_RELATIONS_MACRO_NAME)

    def _relations_cache_for_schemas(self, manifest):
        super(PostgresAdapter, self)._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)

    def list_relations_without_caching(self, database, schema,
                                       model_name=None):
        assert database is not None
        assert schema is not None
        sql = """
        select
          table_catalog as database,
          table_name as name,
          table_schema as schema,
          case when table_type = 'BASE TABLE' then 'table'
               when table_type = 'VIEW' then 'view'
               else table_type
          end as table_type
        from information_schema.tables
        where table_schema ilike '{schema}'
          and table_catalog ilike '{database}'
        """.format(database=database, schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        return [self.Relation.create(
            database=_database,
            schema=_schema,
            identifier=name,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=_type)
                for (_database, name, _schema, _type) in results]

    def list_schemas(self, database, model_name=None):
        sql = """
        select distinct schema_name
        from information_schema.schemata
        where catalog_name='{database}'
        """.format(database=database).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, database, schema, model_name=None):
        sql = """
        select count(*)
        from information_schema.schemata
        where catalog_name='{database}'
          and schema_name='{schema}'
        """.format(database=database, schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name,
                                            auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        db_filter = '1=1'
        if relation.database:
            db_filter = "table_catalog ilike '{}'".format(relation.database)

        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema = '{}'".format(relation.schema)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from information_schema.columns
        where table_name = '{table_name}'
          and {schema_filter}
          and {db_filter}
        order by ordinal_position
        """.format(table_name=relation.identifier,
                   schema_filter=schema_filter,
                   db_filter=db_filter).strip()

        return sql

    def _create_schema_sql(self, database, schema):
        if self.config.credentials.database != database:
            raise dbt.exceptions.NotImplementedException(
                'Can only create schemas on the active database in {}'
                .format(self.type())
            )

        schema = self.quote_as_configured(schema, 'schema')

        return 'create schema if not exists {schema}'.format(schema=schema)

    def _drop_schema_sql(self, database, schema):
        if self.config.credentials.database != database:
            raise dbt.exceptions.NotImplementedException(
                'Can only drop schemas on the active database in {}'
                .format(self.type())
            )

        schema = self.quote_as_configured(schema, 'schema')

        return 'drop schema if exists {schema} cascade'.format(schema=schema)
