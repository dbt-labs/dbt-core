from voluptuous import Schema, Required, All, Any, Extra, Range, Optional

from dbt.contracts.common import validate_with
from dbt.logger import GLOBAL_LOGGER as logger


connection_contract = Schema({
    Required('type'): Any('postgres', 'redshift', 'snowflake'),
    Required('state'): Any('init', 'open', 'closed', 'fail'),
    Required('handle'): Any(None, object),
    Required('credentials'): object,
})

postgres_credentials_contract = Schema({
    Required('dbname'): str,
    Required('host'): str,
    Required('user'): str,
    Required('pass'): str,
    Required('port'): All(int, Range(min=0, max=65535)),
    Required('schema'): str,
})

snowflake_credentials_contract = Schema({
    Required('account'): str,
    Required('user'): str,
    Required('password'): str,
    Required('database'): str,
    Required('schema'): str,
    Required('warehouse'): str,
    Optional('role'): str,
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
