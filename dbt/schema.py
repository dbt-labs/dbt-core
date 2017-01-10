
from dbt.logger import GLOBAL_LOGGER as logger

import psycopg2
import logging
import time
import re

SCHEMA_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the schema
'{schema}'. Either create the schema manually, or adjust the permissions of
the '{user}' user."""

RELATION_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the model
'{model}' in the schema '{schema}'. Please adjust the permissions of the
'{user}' user on the '{schema}' schema. With a superuser account, execute the
following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select, insert, delete on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """
The user '{user}' does not have sufficient permissions to drop the model
'{model}' in the schema '{schema}'. This is likely because the relation was
created by a different user. Either delete the model "{schema}"."{model}"
manually, or adjust the permissions of the '{user}' user in the '{schema}'
schema."""

READ_PERMISSION_DENIED_ERROR = """
Encountered an error while executing model '{model}'.
> {error}
Check that the user '{user}' has sufficient permissions to read from all
necessary source tables"""


class Column(object):
    def __init__(self, column, dtype, char_size):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size

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
        else:
            return self.dtype

    def is_string(self):
        return self.dtype.lower() in ['text', 'character varying']

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

    def __repr__(self):
        return "<Column {} ({})>".format(self.name, self.data_type)
