from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions

import google.cloud.bigquery


class Column(object):
    TYPE_LABELS = {
        'STRING': 'TEXT',
        'TIMESTAMP': 'TIMESTAMP',
        'FLOAT': 'FLOAT',
        'INTEGER': 'INT'
    }

    def __init__(self, column, dtype, char_size=None, numeric_size=None):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size
        self.numeric_size = numeric_size

    @classmethod
    def translate_type(cls, dtype):
        return cls.TYPE_LABELS.get(dtype.upper(), dtype)

    @classmethod
    def create(cls, name, label_or_dtype):
        column_type = cls.translate_type(label_or_dtype)
        return cls(name, column_type)

    @property
    def name(self):
        return self.column

    @property
    def quoted(self):
        return '"{}"'.format(self.column)

    @property
    def data_type(self):
        if self.is_string():
            return Column.string_type(self.string_size())
        elif self.is_numeric():
            return Column.numeric_type(self.dtype, self.numeric_size)
        else:
            return self.dtype

    def is_string(self):
        return self.dtype.lower() in ['text', 'character varying']

    def is_numeric(self):
        return self.dtype.lower() in ['numeric', 'number']

    def string_size(self):
        if not self.is_string():
            raise RuntimeError("Called string_size() on non-string field!")

        if self.dtype == 'text' or self.char_size is None:
            # char_size should never be None. Handle it reasonably just in case
            return 255
        else:
            return int(self.char_size)

    def can_expand_to(self, other_column):
        """returns True if this column can be expanded to the size of the
        other column"""
        if not self.is_string() or not other_column.is_string():
            return False

        return other_column.string_size() > self.string_size()

    def literal(self, value):
        return "{}::{}".format(value, self.data_type)

    @classmethod
    def string_type(cls, size):
        return "character varying({})".format(size)

    @classmethod
    def numeric_type(cls, dtype, size):
        # This could be decimal(...), numeric(...), number(...)
        # Just use whatever was fed in here -- don't try to get too clever
        if size is None:
            return dtype
        else:
            return "{}({})".format(dtype, size)

    def __repr__(self):
        return "<Column {} ({})>".format(self.name, self.data_type)


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

    def to_bq_schema_object(self):
        kwargs = {}
        if len(self.fields) > 0:
            fields = [field.to_bq_schema_object() for field in self.fields]
            kwargs = {"fields": fields}

        return google.cloud.bigquery.SchemaField(self.name, self.dtype,
                                                 self.mode, **kwargs)

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
