from dbt.adapters.default.relation import DefaultRelation
from dbt.utils import filter_null_values


class BigQueryRelation(DefaultRelation):
    External = "external"

    DEFAULTS = {
        'metadata': {
            'type': 'BigQueryRelation'
        },
        'quote_character': '`',
        'quote_policy': {
            'project': True,
            'schema': True,
            'identifier': True
        },
        'include_policy': {
            'project': True,
            'schema': True,
            'identifier': True
        }
    }

    PATH_SCHEMA = {
        'type': 'object',
        'properties': {
            'project': {'type': ['string', 'null']},
            'schema': {'type': ['string', 'null']},
            'identifier': {'type': 'string'},
        },
        'required': ['project', 'schema', 'identifier'],
    }

    POLICY_SCHEMA = {
        'type': 'object',
        'properties': {
            'project': {'type': 'boolean'},
            'schema': {'type': 'boolean'},
            'identifier': {'type': 'boolean'},
        },
        'required': ['project', 'schema', 'identifier'],
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
                'enum': DefaultRelation.RelationTypes + [External, None],
            },
            'path': PATH_SCHEMA,
            'include_policy': POLICY_SCHEMA,
            'quote_policy': POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    PATH_ELEMENTS = ['project', 'schema', 'identifier']

    def matches(self, project=None, schema=None, identifier=None):
        search = filter_null_values({
            'project': project,
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
    def create_from_node(cls, profile, node, **kwargs):
        return cls.create(
            project=profile.get('project'),
            schema=node.get('schema'),
            identifier=node.get('alias'),
            **kwargs)

    @classmethod
    def create(cls, project=None, schema=None,
               identifier=None, table_name=None,
               type=None, **kwargs):
        if table_name is None:
            table_name = identifier

        return cls(type=type,
                   path={
                       'project': project,
                       'schema': schema,
                       'identifier': identifier
                   },
                   table_name=table_name,
                   **kwargs)

    def quote(self, project=None, schema=None, identifier=None):
        policy = filter_null_values({
            'project': project,
            'schema': schema,
            'identifier': identifier
        })

        return self.incorporate(quote_policy=policy)

    def include(self, project=None, schema=None, identifier=None):
        policy = filter_null_values({
            'project': project,
            'schema': schema,
            'identifier': identifier
        })

        return self.incorporate(include_policy=policy)

    @property
    def project(self):
        return self.path.get('project')

    @property
    def schema(self):
        return self.path.get('schema')

    @property
    def dataset(self):
        return self.path.get('schema')

    @property
    def identifier(self):
        return self.path.get('identifier')
