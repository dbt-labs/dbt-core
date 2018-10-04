import psycopg2

from contextlib import contextmanager
import time

from dbt.adapters.sql import SQLAdapter
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


GET_RELATIONS_OPERATION_NAME = 'get_relations_data'


class PostgresAdapter(SQLAdapter):

    DEFAULT_TCP_KEEPALIVE = 0  # 0 means to use the default value

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug('Postgres error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release_connection(connection_name)
            except psycopg2.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(
                dbt.compat.to_string(e).strip())

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release_connection(connection_name)
            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def type(cls):
        return 'postgres'

    @classmethod
    def date_function(cls):
        return 'datenow()'

    @classmethod
    def get_status(cls, cursor):
        return cursor.statusmessage

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def open_connection(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        base_credentials = connection.credentials
        credentials = cls.get_credentials(connection.credentials.incorporate())
        kwargs = {}
        keepalives_idle = credentials.get('keepalives_idle',
                                          cls.DEFAULT_TCP_KEEPALIVE)
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if keepalives_idle:
            kwargs['keepalives_idle'] = keepalives_idle

        try:
            handle = psycopg2.connect(
                dbname=credentials.dbname,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=10,
                **kwargs)

            connection.handle = handle
            connection.state = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel_connection(self, connection):
        connection_name = connection.name
        pid = connection.handle.get_backend_pid()

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql, 'master')
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    def _link_cached_relations(self, manifest):
        schemas = manifest.get_used_schemas()
        try:
            table = self.run_operation(manifest, GET_RELATIONS_OPERATION_NAME)
        finally:
            self.release_connection(GET_RELATIONS_OPERATION_NAME)
        table = self._relations_filter_table(table, schemas)

        for (refed_schema, refed_name, dep_schema, dep_name) in table:
            referenced = self.Relation.create(schema=refed_schema,
                                              identifier=refed_name)
            dependent = self.Relation.create(schema=dep_schema,
                                             identifier=dep_name)
            self.cache.add_link(dependent, referenced)

    def _relations_cache_for_schemas(self, manifest):
        super(PostgresAdapter, self)._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)

    def list_relations_without_caching(self, schema, model_name=None):
        sql = """
        select tablename as name, schemaname as schema, 'table' as type from pg_tables
        where schemaname ilike '{schema}'
        union all
        select viewname as name, schemaname as schema, 'view' as type from pg_views
        where schemaname ilike '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        return [self.Relation.create(
            database=self.config.credentials.dbname,
            schema=_schema,
            identifier=name,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=type)
                for (name, _schema, type) in results]

    def get_existing_schemas(self, model_name=None):
        sql = "select distinct nspname from pg_namespace"

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, schema, model_name=None):
        sql = """
        select count(*) from pg_namespace where nspname = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name,
                                            auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema = '{}'".format(relation.schema)

        db_prefix = ''
        if relation.database:
            db_prefix = '{}.'.format(relation.database)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {db_prefix}information_schema.columns
        where table_name = '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=relation.identifier,
                   schema_filter=schema_filter).strip()

        return sql
