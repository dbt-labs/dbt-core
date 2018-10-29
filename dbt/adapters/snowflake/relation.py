from dbt.adapters.default.relation import DefaultRelation
import dbt.utils


class SnowflakeRelation(DefaultRelation):
    DEFAULTS = {
        'metadata': {
            'type': 'SnowflakeRelation'
        },
        'quote_character': '"',
        'quote_policy': {
            'database': True,
            'schema': False,
            'identifier': False,
        },
        'include_policy': {
            'database': False,
            'schema': True,
            'identifier': True,
        }
    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                'type': 'object',
                'properties': {
                    'type': {
                        'type': 'string',
                        'const': 'SnowflakeRelation',
                    },
                },
            },
            'type': {
                'enum': DefaultRelation.RelationTypes + [None],
            },
            'path': DefaultRelation.PATH_SCHEMA,
            'include_policy': DefaultRelation.POLICY_SCHEMA,
            'quote_policy': DefaultRelation.POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    @classmethod
    def _create_from_node(cls, config, node, **kwargs):
        return cls.create(
            database=config.credentials.database,
            schema=node.get('schema'),
            identifier=node.get('alias'),
            **kwargs)
