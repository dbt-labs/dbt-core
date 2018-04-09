from dbt.api import APIObject
from dbt.utils import filter_null_values


class DefaultRelation(APIObject):

    Table = "table"
    View = "view"
    CTE = "cte"

    RelationTypes = [
        Table,
        View,
        CTE
    ]

    DEFAULTS = {
        'metadata': {
            '_type': 'DefaultRelation'
        },
        'quote_character': '"',
        'quote_policy': {
            'database': True,
            'schema': True,
            'identifier': True
        },
        'include_policy': {
            'database': False,
            'schema': True,
            'identifier': True
        }
    }

    PATH_SCHEMA = {
        'type': 'object',
        'properties': {
            'database': {'type': ['string', 'null']},
            'schema': {'type': ['string', 'null']},
            'identifier': {'type': 'string'},
        },
        'required': ['database', 'schema', 'identifier'],
    }

    POLICY_SCHEMA = {
        'type': 'object',
        'properties': {
            'database': {'type': 'boolean'},
            'schema': {'type': 'boolean'},
            'identifier': {'type': 'boolean'},
        },
        'required': ['database', 'schema', 'identifier'],
    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                '_type': {
                    'type': 'string',
                    'const': 'DefaultRelation',
                },
            },
            'type': {
                'enum': RelationTypes + [None],
            },
            'path': PATH_SCHEMA,
            'include_policy': POLICY_SCHEMA,
            'quote_policy': POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }

    PATH_ELEMENTS = ['database', 'schema', 'identifier']

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

    def get_path_part(self, part):
        return self.path.get(part)

    def should_quote(self, part):
        return self.quote_policy.get(part)

    def should_include(self, part):
        return self.include_policy.get(part)

    def quote(self, database=None, schema=None, identifier=None):
        policy = filter_null_values({
            'database': database,
            'schema': schema,
            'identifier': identifier
        })

        return self.incorporate(quote_policy=policy)

    def include(self, database=None, schema=None, identifier=None):
        policy = filter_null_values({
            'database': database,
            'schema': schema,
            'identifier': identifier
        })

        return self.incorporate(include_policy=policy)

    def render(self):
        parts = []

        for k in self.PATH_ELEMENTS:
            if self.should_include(k):
                path_part = self.get_path_part(k)

                if path_part is None:
                    continue
                elif k == 'identifier':
                    path_part = self.table

                parts.append(
                    self.quote_if(
                        path_part,
                        self.should_quote(k)))

        if len(parts) == 0:
            # TODO
            raise RuntimeError(
                "No path parts are included! Nothing to render.")

        return '.'.join(parts)

    def quote_if(self, identifier, should_quote):
        if should_quote:
            return self.quoted(identifier)

        return identifier

    def quoted(self, identifier):
        return '{quote_char}{identifier}{quote_char}'.format(
            quote_char=self.quote_character,
            identifier=identifier)

    @classmethod
    def create_from_node(cls, profile, node, table_name=None, **kwargs):
        return cls.create(
            database=profile.get('dbname'),
            schema=node.get('schema'),
            identifier=node.get('name'),
            table_name=table_name,
            **kwargs)

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

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.render())

    def __str__(self):
        return self.render()

    @property
    def path(self):
        return self.get('path', {})

    @property
    def database(self):
        return self.path.get('database')

    @property
    def schema(self):
        return self.path.get('schema')

    @property
    def identifier(self):
        return self.path.get('identifier')

    # Here for compatibility with old Relation interface
    @property
    def name(self):
        return self.identifier

    # Here for compatibility with old Relation interface
    @property
    def table(self):
        return self.table_name

    @property
    def is_table(self):
        return self.type == self.Table

    @property
    def is_cte(self):
        return self.type == self.CTE

    @property
    def is_view(self):
        return self.type == self.View
