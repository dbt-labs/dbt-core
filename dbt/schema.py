from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class Column(object):
    def __init__(self, column, dtype, char_size=None, numeric_size=None):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size
        self.numeric_size = numeric_size

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

    @classmethod
    def string_type(cls, size):
        return "character varying({})".format(size)

    @classmethod
    def numeric_type(cls, dtype, size):
        # This could be decimal(...), numeric(...), number(...)
        # Just use whatever was fed in here -- don't try to get too clever
        return "{}({})".format(dtype, size)

    def __repr__(self):
        return "<Column {} ({})>".format(self.name, self.data_type)


class BigQueryColumn(Column):
    def __init__(self, column, dtype, fields, mode):
        super(BigQueryColumn, self).__init__(column, dtype)

        self.mode = mode
        self.fields = self.wrap_subfields(fields)

    @classmethod
    def wrap_subfields(cls, fields):
        return [BigQueryColumn.create(field) for field in fields]

    @classmethod
    def create(cls, field):
        return BigQueryColumn(field.name, field.field_type, field.fields,
                              field.mode)

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

    @property
    def data_type(self):
        return self.dtype

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
