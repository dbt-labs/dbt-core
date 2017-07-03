from voluptuous import Schema, Required, ALLOW_EXTRA

from dbt.contracts.common import validate_with
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

project_contract = Schema({
    Required('name'): str
}, extra=ALLOW_EXTRA)

projects_list_contract = Schema({str: project_contract})


def validate(project):
    validate_with(project_contract, project)


def validate_list(projects):
    validate_with(projects_list_contract, projects)
