import psycopg2

from contextlib import contextmanager

import dbt.adapters.default
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


GET_RELATIONS_OPERATION_NAME = 'get_relations_data'


class PostgresAdapter(dbt.adapters.default.DefaultAdapter):

    DEFAULT_TCP_KEEPALIVE = 0  # 0 means to use the default value

    @contextmanager
    def exception_handler(self, sql, model_name=None, connection_name=None):
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

    # DATABASE INSPECTION FUNCTIONS
    # These require the profile AND project, as they need to know
    # database-specific configs at the project level.
    def alter_column_type(self, schema, table, column_name,
                          new_column_type, model_name=None):
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """

        relation = self.Relation.create(
            schema=schema,
            identifier=table,
            quote_policy=self.config.quoting
        )

        opts = {
            "relation": relation,
            "old_column": column_name,
            "tmp_column": "{}__dbt_alter".format(column_name),
            "dtype": new_column_type
        }

        sql = """
        alter table {relation} add column "{tmp_column}" {dtype};
        update {relation} set "{tmp_column}" = "{old_column}";
        alter table {relation} drop column "{old_column}" cascade;
        alter table {relation} rename column "{tmp_column}" to "{old_column}";
        """.format(**opts).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name)

        return connection, cursor

    def _link_cached_relations(self, manifest, schemas):
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

    def _list_relations(self, schema, model_name=None):
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
    def convert_text_type(cls, agate_table, col_idx):
        return "text"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float8" if decimals else "integer"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp without time zone"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"
