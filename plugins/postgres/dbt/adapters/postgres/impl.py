import psycopg2

import time

from dbt.adapters.base.meta import available_raw
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = 'postgres_get_relations'


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager

    @classmethod
    def date_function(cls):
        return 'now()'

    @available_raw
    def verify_database(self, database):
        database = database.strip('"')
        expected = self.config.credentials.database
        if database != expected:
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    def _link_cached_database_relations(self, schemas):
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

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

    def _get_cache_schemas(self, manifest):
        # postgres/redshift only allow one database (the main one)
        schemas = super(PostgresAdapter, self)._get_cache_schemas(manifest)
        return schemas.flatten()

    def _link_cached_relations(self, manifest):
        schemas = set()
        for db, schema in manifest.get_used_schemas():
            self.verify_database(db)
            schemas.add(schema)

        try:
            self._link_cached_database_relations(schemas)
        finally:
            self.release_connection(GET_RELATIONS_MACRO_NAME)

    def _relations_cache_for_schemas(self, manifest):
        super(PostgresAdapter, self)._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)
