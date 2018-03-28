from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class Column(object):
    def __init__(self, column, dtype, char_size, numeric_size):
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
        print("ASKING FOR DATA TYPE. IT IS '{}'".format(self.dtype))
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
