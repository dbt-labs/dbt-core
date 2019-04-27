from dbt.adapters.base.relation import BaseRelation
from dbt.utils import filter_null_values


class BigQueryRelation(BaseRelation):
    External = "external"

    DEFAULTS = {
        'metadata': {
            'type': 'BigQueryRelation'
        },
        'quote_character': '`',
        'quote_policy': {
            'database': True,
            'schema': True,
            'identifier': True
        },
        'include_policy': {
            'database': True,
            'schema': True,
            'identifier': True
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
                        'const': 'BigQueryRelation',
                    },
                },
            },
            'type': {
                'enum': BaseRelation.RelationTypes + [External, None],
            },
            'path': BaseRelation.PATH_SCHEMA,
            'include_policy': BaseRelation.POLICY_SCHEMA,
            'quote_policy': BaseRelation.POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    def matches(self, database=None, schema=None, identifier=None):
        search = filter_null_values({
            'database': database,
            'schema': schema,
            'identifier': identifier
        })

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if self.get_path_part(k) != v:
                return False

        return True

    @classmethod
    def create(cls, database=None, schema=None,
               identifier=None, table_name=None,
               type=None, **kwargs):
        if table_name is None:
            table_name = identifier

        return cls(type=type,
                   path={
                       'database': database,
                       'schema': schema,
                       'identifier': identifier
                   },
                   table_name=table_name,
                   **kwargs)

    def quote(self, database=None, schema=None, identifier=None):
        policy = filter_null_values({
            'database': database,
            'schema': schema,
            'identifier': identifier
        })

        return self.incorporate(quote_policy=policy)

    @property
    def database(self):
        return self.path.get('database')

    @property
    def project(self):
        return self.path.get('database')

    @property
    def schema(self):
        return self.path.get('schema')

    @property
    def dataset(self):
        return self.path.get('schema')

    @property
    def identifier(self):
        return self.path.get('identifier')
