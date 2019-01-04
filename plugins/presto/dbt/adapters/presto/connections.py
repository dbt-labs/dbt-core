from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager


PRESTO_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
    },
    'required': ['database', 'schema'],
}


class PrestoCredentials(Credentials):
    SCHEMA = PRESTO_CREDENTIALS_CONTRACT

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        raise NotImplementedError


class PrestoConnectionManager(SQLConnectionManager):
    TYPE = 'presto'
