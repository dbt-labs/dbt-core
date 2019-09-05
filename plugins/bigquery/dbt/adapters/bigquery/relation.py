from dbt.adapters.base.relation import BaseRelation, Column
from dbt.utils import filter_null_values

import google.cloud.bigquery


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
            'identifier': True,
        },
        'include_policy': {
            'database': True,
            'schema': True,
            'identifier': True,
        },
        'dbt_created': False,
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
            'dbt_created': {'type': 'boolean'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character', 'dbt_created']
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
            if not self._is_exactish_match(k, v):
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


class BigQueryColumn(Column):
    TYPE_LABELS = {
        'STRING': 'STRING',
        'TIMESTAMP': 'TIMESTAMP',
        'FLOAT': 'FLOAT64',
        'INTEGER': 'INT64',
        'RECORD': 'RECORD',
    }

    def __init__(self, column, dtype, fields=None, mode='NULLABLE'):
        super(BigQueryColumn, self).__init__(column, dtype)

        if fields is None:
            fields = []

        self.fields = self.wrap_subfields(fields)
        self.mode = mode

    @classmethod
    def wrap_subfields(cls, fields):
        return [BigQueryColumn.create_from_field(field) for field in fields]

    @classmethod
    def create_from_field(cls, field):
        return BigQueryColumn(field.name, cls.translate_type(field.field_type),
                              field.fields, field.mode)

    @classmethod
    def _flatten_recursive(cls, col, prefix=None):
        if prefix is None:
            prefix = []

        if len(col.fields) == 0:
            prefixed_name = ".".join(prefix + [col.column])
            new_col = BigQueryColumn(prefixed_name, col.dtype, col.fields,
                                     col.mode)
            return [new_col]

        new_fields = []
        for field in col.fields:
            new_prefix = prefix + [col.column]
            new_fields.extend(cls._flatten_recursive(field, new_prefix))

        return new_fields

    def flatten(self):
        return self._flatten_recursive(self)

    @property
    def quoted(self):
        return '`{}`'.format(self.column)

    def literal(self, value):
        return "cast({} as {})".format(value, self.dtype)

    @property
    def data_type(self):
        if self.dtype.upper() == 'RECORD':
            subcols = [
                "{} {}".format(col.name, col.data_type) for col in self.fields
            ]
            field_type = 'STRUCT<{}>'.format(", ".join(subcols))

        else:
            field_type = self.dtype

        if self.mode.upper() == 'REPEATED':
            return 'ARRAY<{}>'.format(field_type)

        else:
            return field_type

    def is_string(self):
        return self.dtype.lower() == 'string'

    def is_numeric(self):
        return False

    def can_expand_to(self, other_column):
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def __repr__(self):
        return "<BigQueryColumn {} ({}, {})>".format(self.name, self.data_type,
                                                     self.mode)

    def column_to_bq_schema(self):
        """Convert a column to a bigquery schema object.
        """
        kwargs = {}
        if len(self.fields) > 0:
            fields = [field.column_to_bq_schema() for field in self.fields]
            kwargs = {"fields": fields}

        return google.cloud.bigquery.SchemaField(self.name, self.dtype,
                                                 self.mode, **kwargs)
