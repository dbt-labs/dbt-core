from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres import PostgresColumn
import dbt.exceptions


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = 'postgres_get_relations'


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager
    Column = PostgresColumn

    @classmethod
    def date_function(cls):
        return 'now()'

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        else:
            database = database.lower()
        expected = self.config.credentials.database
        if database != expected:
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    def _link_cached_database_relations(self, schemas):
        """

        :param Set[str] schemas: The set of schemas that should have links
            added.
        """
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

    def _get_cache_schemas(self, manifest, exec_only=False):
        # postgres/redshift only allow one database (the main one)
        schemas = super()._get_cache_schemas(manifest, exec_only=exec_only)
        try:
            return schemas.flatten()
        except dbt.exceptions.RuntimeException as exc:
            dbt.exceptions.raise_compiler_error(
                'Cross-db references not allowed in adapter {}: Got {}'.format(
                    self.type(), exc.msg
                )
            )

    def _link_cached_relations(self, manifest):
        schemas = set()
        # only link executable nodes
        info_schema_name_map = self._get_cache_schemas(manifest,
                                                       exec_only=True)
        for db, schema in info_schema_name_map.search():
            self.verify_database(db.database)
            schemas.add(schema)

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest):
        super()._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)
