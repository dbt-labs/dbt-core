from dataclasses import dataclass
import re
from typing import Dict, ClassVar, Any, Optional

from dbt.exceptions import RuntimeException


@dataclass
class Column:
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "TEXT",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT",
        "INTEGER": "INT",
    }
    column: str
    dtype: str
    char_size: Optional[int] = None
    numeric_precision: Optional[Any] = None
    numeric_scale: Optional[Any] = None

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return cls.TYPE_LABELS.get(dtype.upper(), dtype)

    @classmethod
    def create(cls, name, label_or_dtype: str) -> "Column":
        column_type = cls.translate_type(label_or_dtype)
        return cls(name, column_type)

    @property
    def name(self) -> str:
        return self.column

    @property
    def quoted(self) -> str:
        return '"{}"'.format(self.column)

    @property
    def data_type(self) -> str:
        if self.is_string():
            return self.string_type(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["text", "character varying", "character", "varchar"]

    def is_number(self):
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

    def is_float(self):
        return self.dtype.lower() in [
            # floats
            "real",
            "float4",
            "float",
            "double precision",
            "float8",
        ]

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            # real types
            "smallint",
            "integer",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            # aliases
            "int2",
            "int4",
            "int8",
            "serial2",
            "serial4",
            "serial8",
        ]

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ["numeric", "decimal"]

    def string_size(self) -> int:
        if not self.is_string():
            raise RuntimeException("Called string_size() on non-string field!")

        if self.dtype == "text" or self.char_size is None:
            # char_size should never be None. Handle it reasonably just in case
            return 256
        else:
            return int(self.char_size)

    def can_expand_to(self, other_column: "Column") -> bool:
        """returns True if this column can be expanded to the size of the
        other column"""
        if not self.is_string() or not other_column.is_string():
            return False

        return other_column.string_size() > self.string_size()

    def literal(self, value: Any) -> str:
        return "{}::{}".format(value, self.data_type)

    @classmethod
    def string_type(cls, size: int) -> str:
        return "character varying({})".format(size)

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        # This could be decimal(...), numeric(...), number(...)
        # Just use whatever was fed in here -- don't try to get too clever
        if precision is None or scale is None:
            return dtype
        else:
            return "{}({},{})".format(dtype, precision, scale)

    def __repr__(self) -> str:
        return "<Column {} ({})>".format(self.name, self.data_type)

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "Column":
        match = re.match(r"([^(]+)(\([^)]+\))?", raw_data_type)
        if match is None:
            raise RuntimeException(f'Could not interpret data type "{raw_data_type}"')
        data_type, size_info = match.groups()
        char_size = None
        numeric_precision = None
        numeric_scale = None
        if size_info is not None:
            # strip out the parentheses
            size_info = size_info[1:-1]
            parts = size_info.split(",")
            if len(parts) == 1:
                try:
                    char_size = int(parts[0])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
            elif len(parts) == 2:
                try:
                    numeric_precision = int(parts[0])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
                try:
                    numeric_scale = int(parts[1])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[1]}" to an integer'
                    )

        return cls(name, data_type, char_size, numeric_precision, numeric_scale)
