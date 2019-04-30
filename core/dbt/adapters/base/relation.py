from dbt.api import APIObject
from dbt.utils import filter_null_values
from dbt.node_types import NodeType

import dbt.exceptions


class BaseRelation(APIObject):

    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materializedview"

    RelationTypes = [
        Table,
        View,
        CTE,
        MaterializedView
    ]

    DEFAULTS = {
        'metadata': {
            'type': 'BaseRelation'
        },
        'quote_character': '"',
        'quote_policy': {
            'database': True,
            'schema': True,
            'identifier': True
        },
        'include_policy': {
            'database': True,
            'schema': True,
            'identifier': True
        },
    }

    PATH_SCHEMA = {
        'type': 'object',
        'properties': {
            'database': {'type': ['string', 'null']},
            'schema': {'type': ['string', 'null']},
            'identifier': {'type': ['string', 'null']},
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
                'type': 'object',
                'properties': {
                    'type': {
                        'type': 'string',
                        'const': 'BaseRelation',
                    },
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
            raise dbt.exceptions.RuntimeException(
                "Tried to match relation, but no search path was passed!")

        exact_match = True
        approximate_match = True

        for k, v in search.items():
            if self.get_path_part(k) != v:
                exact_match = False

            if self.get_path_part(k).lower() != v.lower():
                approximate_match = False

        if approximate_match and not exact_match:
            target = self.create(
                database=database, schema=schema, identifier=identifier)
            dbt.exceptions.approximate_relation_match(target, self)

        return exact_match

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

    def information_schema(self, identifier=None):
        include_db = self.database is not None
        include_policy = filter_null_values({
            'database': include_db,
            'schema': True,
            'identifier': identifier is not None
        })
        quote_policy = filter_null_values({
            'database': self.quote_policy['database'],
            'schema': False,
            'identifier': False,
        })

        path_update = {
            'schema': 'information_schema',
            'identifier': identifier
        }

        return self.incorporate(
            quote_policy=quote_policy,
            include_policy=include_policy,
            path=path_update,
            table_name=identifier)

    def information_schema_only(self):
        return self.information_schema()

    def information_schema_table(self, identifier):
        return self.information_schema(identifier)

    def render(self, use_table_name=True):
        parts = []

        for k in self.PATH_ELEMENTS:
            if self.should_include(k):
                path_part = self.get_path_part(k)

                if path_part is None:
                    continue
                elif k == 'identifier':
                    if use_table_name:
                        path_part = self.table
                    else:
                        path_part = self.identifier

                parts.append(
                    self.quote_if(
                        path_part,
                        self.should_quote(k)))

        if len(parts) == 0:
            raise dbt.exceptions.RuntimeException(
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
    def create_from_source(cls, source, **kwargs):
        quote_policy = dbt.utils.deep_merge(
            cls.DEFAULTS['quote_policy'],
            source.quoting,
            kwargs.get('quote_policy', {})
        )
        return cls.create(
            database=source.database,
            schema=source.schema,
            identifier=source.identifier,
            quote_policy=quote_policy,
            **kwargs
        )

    @classmethod
    def create_from_node(cls, config, node, table_name=None, quote_policy=None,
                         **kwargs):
        if quote_policy is None:
            quote_policy = {}

        quote_policy = dbt.utils.merge(config.quoting, quote_policy)

        return cls.create(
            database=node.get('database'),
            schema=node.get('schema'),
            identifier=node.get('alias'),
            table_name=table_name,
            quote_policy=quote_policy,
            **kwargs)

    @classmethod
    def create_from(cls, config, node, **kwargs):
        if node.resource_type == NodeType.Source:
            return cls.create_from_source(node, **kwargs)
        else:
            return cls.create_from_node(config, node, **kwargs)

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

    def __hash__(self):
        return hash(self.render())

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
