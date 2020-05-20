from dataclasses import dataclass
from typing import Optional, Set
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres import PostgresColumn
import dbt.exceptions


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = 'postgres_get_relations'


@dataclass
class PostgresConfig(AdapterConfig):
    unlogged: Optional[bool] = None


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager
    Column = PostgresColumn

    AdapterSpecificConfigs = PostgresConfig

    @classmethod
    def date_function(cls):
        return 'now()'

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

        for (dep_schema, dep_name, refed_schema, refed_name) in table:
            dependent = self.Relation.create(
                database=database,
                schema=dep_schema,
                identifier=dep_name
            )
            referenced = self.Relation.create(
                database=database,
                schema=refed_schema,
                identifier=refed_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(referenced, dependent)

    def _get_catalog_schemas(self, manifest):
        # postgres/redshift only allow one database (the main one)
        schemas = super()._get_catalog_schemas(manifest)
        try:
            return schemas.flatten()
        except dbt.exceptions.RuntimeException as exc:
            dbt.exceptions.raise_compiler_error(
                'Cross-db references not allowed in adapter {}: Got {}'.format(
                    self.type(), exc.msg
                )
            )

    def _link_cached_relations(self, manifest):
        schemas: Set[str] = set()
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest):
        super()._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)
