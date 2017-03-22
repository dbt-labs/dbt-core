from voluptuous import Schema, Required, All, Any, Range, Optional

from dbt.compat import basestring
from dbt.contracts.common import validate_with
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


connection_contract = Schema({
    Required('type'): Any('postgres', 'redshift', 'snowflake'),
    Required('name'): Any(None, basestring),
    Required('state'): Any('init', 'open', 'closed', 'fail'),
    Required('transaction_open'): bool,
    Required('handle'): Any(None, object),
    Required('credentials'): object,
})

postgres_credentials_contract = Schema({
    Required('dbname'): basestring,
    Required('host'): basestring,
    Required('user'): basestring,
    Required('pass'): basestring,
    Required('port'): All(int, Range(min=0, max=65535)),
    Required('schema'): basestring,
})

snowflake_credentials_contract = Schema({
    Required('account'): basestring,
    Required('user'): basestring,
    Required('password'): basestring,
    Required('database'): basestring,
    Required('schema'): basestring,
    Required('warehouse'): basestring,
    Optional('role'): basestring,
})

credentials_mapping = {
    'postgres': postgres_credentials_contract,
    'redshift': postgres_credentials_contract,
    'snowflake': snowflake_credentials_contract,
}


def validate_connection(connection):
    validate_with(connection_contract, connection)

    credentials_contract = credentials_mapping.get(connection.get('type'))
    validate_with(credentials_contract, connection.get('credentials'))
