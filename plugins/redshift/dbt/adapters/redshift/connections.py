from contextlib import contextmanager
import multiprocessing

from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres import PostgresCredentials
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions

import boto3

drop_lock = multiprocessing.Lock()

REDSHIFT_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'method': {
            'enum': ['database', 'iam'],
            'description': (
                'database: use user/pass creds; iam: use temporary creds'
            ),
        },
        'database': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'schema': {
            'type': 'string',
        },
        'cluster_id': {
            'type': 'string',
            'description': (
                'If using IAM auth, the name of the cluster'
            )
        },
        'iam_duration_seconds': {
            'type': 'integer',
            'minimum': 900,
            'maximum': 3600,
            'description': (
                'If using IAM auth, the ttl for the temporary credentials'
            )
        },
        'search_path': {
            'type': 'string',
        },
        'keepalives_idle': {
            'type': 'integer',
        },
        'required': ['database', 'host', 'user', 'port', 'schema']
    }
}


class RedshiftCredentials(PostgresCredentials):
    SCHEMA = REDSHIFT_CREDENTIALS_CONTRACT

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('method', 'database')
        super().__init__(*args, **kwargs)

    @property
    def type(self):
        return 'redshift'

    def _connection_keys(self):
        return (
            'host', 'port', 'user', 'database', 'schema', 'method',
            'search_path')


class RedshiftConnectionManager(PostgresConnectionManager):
    DEFAULT_TCP_KEEPALIVE = 240
    TYPE = 'redshift'

    @contextmanager
    def fresh_transaction(self, name=None):
        """On entrance to this context manager, hold an exclusive lock and
        create a fresh transaction for redshift, then commit and begin a new
        one before releasing the lock on exit.

        See drop_relation in RedshiftAdapter for more information.

        :param Optional[str] name: The name of the connection to use, or None
            to use the default.
        """
        with drop_lock:
            connection = self.get_thread_connection()

            if connection.transaction_open:
                self.commit()

            self.begin()
            yield

            self.commit()
            self.begin()

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
                "Unable to get temporary Redshift cluster credentials: {}"
                .format(e))

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
            credentials.user,
            credentials.database,
            credentials.cluster_id,
            iam_duration_s,
        )

        # replace username and password with temporary redshift credentials
        return credentials.incorporate(
            user=cluster_creds.get('DbUser'),
            password=cluster_creds.get('DbPassword')
        )

    @classmethod
    def get_credentials(cls, credentials):
        method = credentials.method

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
