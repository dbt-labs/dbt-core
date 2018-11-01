from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.utils import deep_merge
from dbt.contracts.connection import POSTGRES_CREDENTIALS_CONTRACT, \
    REDSHIFT_CREDENTIALS_CONTRACT, SNOWFLAKE_CREDENTIALS_CONTRACT, \
    BIGQUERY_CREDENTIALS_CONTRACT

# TODO: add description fields.
ARCHIVE_TABLE_CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'source_table': {'type': 'string'},
        'target_table': {'type': 'string'},
        'updated_at': {'type': 'string'},
        'unique_key': {'type': 'string'},
    },
    'required': ['source_table', 'target_table', 'updated_at', 'unique_key'],
}


ARCHIVE_CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'source_schema': {'type': 'string'},
        'target_schema': {'type': 'string'},
        'tables': {
            'type': 'array',
            'item': ARCHIVE_TABLE_CONFIG_CONTRACT,
        }
    },
    'required': ['source_schema', 'target_schema', 'tables'],
}


PROJECT_CONTRACT = {
    'type': 'object',
    'description': 'The project configuration.',
    'additionalProperties': False,
    'properties': {
        'name': {
            'type': 'string',
            'pattern': r'^[^\d\W]\w*\Z',
        },
        'version': {
            'anyOf': [
                {
                    'type': 'string',
                    'pattern': (
                        # this does not support the full semver (does not
                        # allow a trailing -fooXYZ) and is not restrictive
                        # enough for full semver, (allows '1.0'). But it's like
                        # 'semver lite'.
                        r'^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(\.(?:0|[1-9]\d*))?$'
                    ),
                },
                {
                    # the internal global_project/dbt_project.yml is actually
                    # 1.0. Heaven only knows how many users have done the same
                    'type': 'number',
                },
            ],
        },
        'project-root': {
            'type': 'string',
        },
        'source-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'macro-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'data-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'test-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'analysis-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'docs-paths': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'target-path': {
            'type': 'string',
        },
        'clean-targets': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'profile': {
            'type': ['null', 'string'],
        },
        'log-path': {
            'type': 'string',
        },
        'modules-path': {
            'type': 'string',
        },
        'quoting': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'identifier': {
                    'type': 'boolean',
                },
                'schema': {
                    'type': 'boolean',
                },
                'database': {
                    'type': 'boolean',
                },
                'project': {
                    'type': 'boolean',
                }
            },
        },
        'models': {
            'type': 'object',
            'additionalProperties': True,
        },
        'on-run-start': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'on-run-end': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'archive': {
            'type': 'array',
            'items': ARCHIVE_CONFIG_CONTRACT,
        },
        'seeds': {
            'type': 'object',
            'additionalProperties': True,
        },
    },
    'required': ['name', 'version'],
}


class Project(APIObject):
    SCHEMA = PROJECT_CONTRACT


LOCAL_PACKAGE_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'local': {
            'type': 'string',
            'description': 'The absolute path to the local package.',
        },
        'required': ['local'],
    },
}


GIT_PACKAGE_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'git': {
            'type': 'string',
            'description': (
                'The URL to the git repository that stores the pacakge'
            ),
        },
        'revision': {
            'type': ['string', 'array'],
            'item': 'string',
            'description': 'The git revision to use, if it is not tip',
        },
    },
    'required': ['git'],
}


VERSION_SPECIFICATION_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'major': {
            'type': ['string', 'null'],
        },
        'minor': {
            'type': ['string', 'null'],
        },
        'patch': {
            'type': ['string', 'null'],
        },
        'prerelease': {
            'type': ['string', 'null'],
        },
        'build': {
            'type': ['string', 'null'],
        },
        'matcher': {
            'type': 'string',
            'enum': ['=', '>=', '<=', '>', '<'],
        },
    },
    'required': ['major', 'minor', 'patch', 'prerelease', 'build', 'matcher'],
}


REGISTRY_PACKAGE_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'package': {
            'type': 'string',
            'description': 'The name of the package',
        },
        'version': {
            'type': ['string', 'array'],
            'item': {
                'anyOf': [
                    VERSION_SPECIFICATION_CONTRACT,
                    'string'
                ],
            },
            'description': 'The version of the package',
        },
    },
    'required': ['package'],
}


PACKAGE_FILE_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'packages': {
            'type': 'array',
            'items': {
                'anyOf': [
                    LOCAL_PACKAGE_CONTRACT,
                    GIT_PACKAGE_CONTRACT,
                    REGISTRY_PACKAGE_CONTRACT,
                ],
            },
        },
    },
    'required': ['packages'],
}


# the metadata from the registry has extra things that we don't care about.
REGISTRY_PACKAGE_METADATA_CONTRACT = deep_merge(
    PACKAGE_FILE_CONTRACT,
    {
        'additionalProperties': True,
        'properties': {
            'name': {
                'type': 'string',
            },
            'downloads': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'tarball': {
                        'type': 'string',
                    },
                },
                'required': ['tarball']
            },
        },
        'required': PACKAGE_FILE_CONTRACT['required'][:] + ['downloads']
    }
)


class PackageConfig(APIObject):
    SCHEMA = PACKAGE_FILE_CONTRACT


PROFILE_INFO_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'profile_name': {
            'type': 'string',
        },
        'target_name': {
            'type': 'string',
        },
        'send_anonymous_usage_stats': {
            'type': 'boolean',
        },
        'use_colors': {
            'type': 'boolean',
        },
        'threads': {
            'type': 'number',
        },
        'credentials': {
            'anyOf': [
                POSTGRES_CREDENTIALS_CONTRACT,
                REDSHIFT_CREDENTIALS_CONTRACT,
                SNOWFLAKE_CREDENTIALS_CONTRACT,
                BIGQUERY_CREDENTIALS_CONTRACT,
            ],
        },
    },
    'required': [
        'profile_name', 'target_name', 'send_anonymous_usage_stats',
        'use_colors', 'threads', 'credentials'
    ],
}


class ProfileConfig(APIObject):
    SCHEMA = PROFILE_INFO_CONTRACT


def _merge_requirements(base, *args):
    required = base[:]
    for arg in args:
        required.extend(arg['required'])
    return required


CONFIG_CONTRACT = deep_merge(
    PROJECT_CONTRACT,
    PACKAGE_FILE_CONTRACT,
    PROFILE_INFO_CONTRACT,
    {
        'properties': {
            'cli_vars': {
                'type': 'object',
                'additionalProperties': True,
            },
            # override quoting: both 'identifier' and 'schema' must be
            # populated
            'quoting': {
                'required': ['identifier', 'schema'],
            },
        },
        'required': _merge_requirements(
            ['cli_vars'],
            PROJECT_CONTRACT,
            PACKAGE_FILE_CONTRACT,
            PROFILE_INFO_CONTRACT
        ),
    },
)


class Configuration(APIObject):
    SCHEMA = CONFIG_CONTRACT


PROJECTS_LIST_PROJECT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': CONFIG_CONTRACT,
    },
}


class ProjectList(APIObject):
    SCHEMA = PROJECTS_LIST_PROJECT

    def serialize(self):
        return {k: v.serialize() for k, v in self._contents.items()}
