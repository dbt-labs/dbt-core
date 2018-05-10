import multiprocessing

from dbt.adapters.postgres import PostgresAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions
import boto3
import psycopg2

drop_lock = multiprocessing.Lock()


class RedshiftAdapter(PostgresAdapter):

    @classmethod
    def type(cls):
        return 'redshift'

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def get_redshift_credentials(cls, config):
        result = config.copy()

        method = result.get('method')

        if method == 'database':
            return (result)

        elif method == 'iam':
            cluster_id = result.get('cluster_id')
            if not cluster_id:
                error = '`cluster_id` must be set in profile if IAM authentication method selected'
                raise dbt.exceptions.FailedToConnectException(error)

            client = boto3.client('redshift')

            # replace username and password with temporary redshift credentials
            try:
                cluster_creds = client.get_cluster_credentials(DbUser=result.get('user'),
                                                               DbName=result.get('dbname'),
                                                               ClusterIdentifier=result.get('cluster_id'),
                                                               AutoCreate=False)
                result['user_tmp'] = cluster_creds.get('DbUser')
                result['pass_tmp'] = cluster_creds.get('DbPassword')
            except client.exceptions.ClientError as e:
                error = ('Unable to get temporary Redshift cluster credentials: "{}"'.format(str(e)))
                raise dbt.exceptions.FailedToConnectException(error)

            return result

        else:
            error = ('Invalid `method` in profile: "{}"'.format(method))
            raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = cls.get_redshift_credentials(connection.get('credentials', {}))
            user = credentials.get('user_tmp') if credentials.get('user_tmp') else credentials.get('user')
            password = credentials.get('pass_tmp') if credentials.get('pass_tmp') else credentials.get('pass')

            handle = psycopg2.connect(
                dbname=credentials.get('dbname'),
                user=user,
                host=credentials.get('host'),
                password=password,
                port=credentials.get('port'),
                connect_timeout=10)

            result['handle'] = handle
            result['state'] = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def _get_columns_in_table_sql(cls, schema_name, table_name, database):
        # Redshift doesn't support cross-database queries,
        # so we can ignore the `database` argument

        # TODO : how do we make this a macro?
        if schema_name is None:
            table_schema_filter = '1=1'
        else:
            table_schema_filter = "table_schema = '{schema_name}'".format(
                schema_name=schema_name)

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
            where {table_schema_filter}
            order by ordinal_position
        """.format(table_name=table_name,
                   table_schema_filter=table_schema_filter).strip()
        return sql

    @classmethod
    def drop_relation(cls, profile, project, relation, model_name=None):
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
        global drop_lock

        to_return = None

        try:
            drop_lock.acquire()

            connection = cls.get_connection(profile, model_name)

            if connection.get('transaction_open'):
                cls.commit(profile, connection)

            cls.begin(profile, connection.get('name'))

            to_return = super(PostgresAdapter, cls).drop_relation(
                profile, project, relation, model_name)

            cls.commit(profile, connection)
            cls.begin(profile, connection.get('name'))

            return to_return

        finally:
            drop_lock.release()

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "varchar(24)"
