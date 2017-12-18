from voluptuous import Schema, Required, All, Any, Range, Optional

from dbt.compat import basestring
from dbt.contracts.common import validate_with
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


adapter_types = ['postgres', 'redshift', 'snowflake', 'bigquery']
connection_contract = Schema({
    Required('type'): Any(*adapter_types),
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
    Required('port'): Any(All(int, Range(min=0, max=65535)), basestring),
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

bigquery_auth_methods = ['oauth', 'service-account', 'service-account-json']
bigquery_credentials_contract = Schema({
    Required('method'): Any(*bigquery_auth_methods),
    Required('project'): basestring,
    Required('schema'): basestring,
    Optional('keyfile'): basestring,
    Optional('keyfile_json'): object,
    Optional('timeout_seconds'): int,
})

credentials_mapping = {
    'postgres': postgres_credentials_contract,
    'redshift': postgres_credentials_contract,
    'snowflake': snowflake_credentials_contract,
    'bigquery': bigquery_credentials_contract,
}


def validate_connection(connection):
    validate_with(connection_contract, connection)

    credentials_contract = credentials_mapping.get(connection.get('type'))
    validate_with(credentials_contract, connection.get('credentials'))
