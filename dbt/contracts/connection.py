from voluptuous import Schema, Required, All, Any
from voluptuous.error import MultipleInvalid

from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger


connection_contract = Schema({
    Required('type'): Any('postgres', 'redshift'),
    Required('state'): Any('init', 'open', 'closed', 'fail'),
    Required('handle'): Any(None),
})


def validate_connection(connection):
    try:
        connection_contract(connection)
    except MultipleInvalid as e:
        logger.info(e)
        raise ValidationException(str(e))
