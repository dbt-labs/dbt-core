from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

PROJECT_CONTRACT = {
    'type': 'object',
    'additionalProperties': True,
    # TODO: Come back and wire the rest of the project config stuff into this.
    'description': 'The project configuration. This is incomplete.',
    'properties': {
        'name': {
            'type': 'string',
        }
    },
    'required': ['name'],
}

PROJECTS_LIST_PROJECT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': PROJECT_CONTRACT,
    },
}


class ProjectList(APIObject):
    SCHEMA = PROJECTS_LIST_PROJECT
