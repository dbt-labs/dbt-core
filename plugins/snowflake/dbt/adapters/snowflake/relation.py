from dbt.adapters.base.relation import BaseRelation
import dbt.utils


class SnowflakeRelation(BaseRelation):
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
            'database': True,
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
                'enum': BaseRelation.RelationTypes + [None],
            },
            'path': BaseRelation.PATH_SCHEMA,
            'include_policy': BaseRelation.POLICY_SCHEMA,
            'quote_policy': BaseRelation.POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    @classmethod
    def _create_from_node(cls, config, node, **kwargs):
        return cls.create(
            database=node.get('database'),
            schema=node.get('schema'),
            identifier=node.get('alias'),
            **kwargs)
