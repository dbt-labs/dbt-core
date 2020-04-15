from dataclasses import dataclass

from dbt.adapters.base.column import Column
from dbt.exceptions import RuntimeException


@dataclass
class SnowflakeColumn(Column):
    def is_integer(self) -> bool:
        # everything that smells like an int is actually a NUMBER(38, 0)
        return False

    def is_numeric(self) -> bool:
        return self.dtype.lower() in [
            'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint',
            'numeric', 'decimal', 'number'
        ]

    def is_float(self):
        return self.dtype.lower() in [
            'float', 'float4', 'float8', 'double', 'double precision', 'real',
        ]

    def string_size(self) -> int:
        if not self.is_string():
            raise RuntimeException("Called string_size() on non-string field!")

        if self.dtype == 'text' or self.char_size is None:
            return 16777216
        else:
            return int(self.char_size)
