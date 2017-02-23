from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length, ALLOW_EXTRA
from voluptuous.error import Invalid, MultipleInvalid

from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

project_contract = Schema({
    Required('name'): str
}, extra=ALLOW_EXTRA)

projects_list_contract = Schema({str: project_contract})

def validate(project):
    try:
        project_contract(project)

    except Invalid as e:
        logger.info(e)
        raise ValidationException(str(e))

def validate_list(projects):
    try:
        projects_list_contract(projects)

    except Invalid as e:
        logger.info(e)
        raise ValidationException(str(e))
