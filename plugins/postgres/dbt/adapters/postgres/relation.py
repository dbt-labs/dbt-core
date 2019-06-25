from dbt.adapters.base import Column


class PostgresColumn(Column):
    @property
    def data_type(self):
        # on postgres, do not convert 'text' to 'varchar()'
        if self.dtype.lower() == 'text':
            return self.dtype
        return super().data_type
