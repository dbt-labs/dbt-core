from dataclasses import dataclass
from typing import Optional, List, TypeVar, Iterable, Type

from dbt.adapters.base.column import Column

from google.cloud.bigquery import SchemaField

Self = TypeVar('Self', bound='BigQueryColumn')


@dataclass(init=False)
class BigQueryColumn(Column):
    TYPE_LABELS = {
        'STRING': 'STRING',
        'TIMESTAMP': 'TIMESTAMP',
        'FLOAT': 'FLOAT64',
        'INTEGER': 'INT64',
        'RECORD': 'RECORD',
    }
    fields: List[Self]
    mode: str

    def __init__(
        self,
        column: str,
        dtype: str,
        fields: Optional[Iterable[SchemaField]] = None,
        mode: str = 'NULLABLE',
    ) -> None:
        super().__init__(column, dtype)

        if fields is None:
            fields = []

        self.fields = self.wrap_subfields(fields)
        self.mode = mode

    @classmethod
    def wrap_subfields(
        cls: Type[Self], fields: Iterable[SchemaField]
    ) -> List[Self]:
        return [cls.create_from_field(field) for field in fields]

    @classmethod
    def create_from_field(cls: Type[Self], field: SchemaField) -> Self:
        return cls(
            field.name,
            cls.translate_type(field.field_type),
            field.fields,
            field.mode,
        )

    @classmethod
    def _flatten_recursive(
        cls: Type[Self], col: Self, prefix: Optional[str] = None
    ) -> List[Self]:
        if prefix is None:
            prefix = []

        if len(col.fields) == 0:
            prefixed_name = ".".join(prefix + [col.column])
            new_col = cls(prefixed_name, col.dtype, col.fields, col.mode)
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
    def data_type(self) -> str:
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

    def is_string(self) -> bool:
        return self.dtype.lower() == 'string'

    def is_integer(self) -> bool:
        return self.dtype.lower() == 'int64'

    def is_numeric(self) -> bool:
        return self.dtype.lower() == 'numeric'

    def is_float(self):
        return self.dtype.lower() == 'float64'

    def can_expand_to(self: Self, other_column: Self) -> bool:
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def __repr__(self) -> str:
        return "<BigQueryColumn {} ({}, {})>".format(self.name, self.data_type,
                                                     self.mode)

    def column_to_bq_schema(self) -> SchemaField:
        """Convert a column to a bigquery schema object.
        """
        kwargs = {}
        if len(self.fields) > 0:
            fields = [field.column_to_bq_schema() for field in self.fields]
            kwargs = {"fields": fields}

        return SchemaField(self.name, self.dtype, self.mode, **kwargs)
