import multiprocessing

from dbt.adapters.postgres import PostgresAdapter
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions
import boto3

drop_lock = multiprocessing.Lock()


class RedshiftAdapter(PostgresAdapter):

    DEFAULT_TCP_KEEPALIVE = 240

    @classmethod
    def type(cls):
        return 'redshift'

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def fetch_cluster_credentials(cls, db_user, db_name, cluster_id,
                                  duration_s):
        """Fetches temporary login credentials from AWS. The specified user
        must already exist in the database, or else an error will occur"""
        boto_client = boto3.client('redshift')

        try:
            return boto_client.get_cluster_credentials(
                DbUser=db_user,
                DbName=db_name,
                ClusterIdentifier=cluster_id,
                DurationSeconds=duration_s,
                AutoCreate=False)

        except boto_client.exceptions.ClientError as e:
            raise dbt.exceptions.FailedToConnectException(
                    "Unable to get temporary Redshift cluster credentials: "
                    "{}".format(e))

    @classmethod
    def get_tmp_iam_cluster_credentials(cls, credentials):
        cluster_id = credentials.get('cluster_id')

        # default via:
        # boto3.readthedocs.io/en/latest/reference/services/redshift.html
        iam_duration_s = credentials.get('iam_duration_seconds', 900)

        if not cluster_id:
            raise dbt.exceptions.FailedToConnectException(
                    "'cluster_id' must be provided in profile if IAM "
                    "authentication method selected")

        cluster_creds = cls.fetch_cluster_credentials(
            credentials.get('user'),
            credentials.get('dbname'),
            credentials.get('cluster_id'),
            iam_duration_s,
        )

        # replace username and password with temporary redshift credentials
        return dbt.utils.merge(credentials, {
            'user': cluster_creds.get('DbUser'),
            'pass': cluster_creds.get('DbPassword')
        })

    @classmethod
    def get_credentials(cls, credentials):
        method = credentials.get('method')

        # Support missing 'method' for backwards compatibility
        if method == 'database' or method is None:
            logger.debug("Connecting to Redshift using 'database' credentials")
            return credentials

        elif method == 'iam':
            logger.debug("Connecting to Redshift using 'IAM' credentials")
            return cls.get_tmp_iam_cluster_credentials(credentials)

        else:
            raise dbt.exceptions.FailedToConnectException(
                    "Invalid 'method' in profile: '{}'".format(method))

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
