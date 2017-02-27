from voluptuous.error import Invalid, MultipleInvalid

from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger


def validate_with(schema, data):
    try:
        schema(data)

    except MultipleInvalid as e:
        logger.error(str(e))
        raise ValidationException(str(e))

    except Invalid as e:
        logger.error(str(e))
        raise ValidationException(str(e))
