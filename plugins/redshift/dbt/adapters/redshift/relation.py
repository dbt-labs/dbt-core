from dbt.adapters.base import Column


class RedshiftColumn(Column):
    pass  # redshift does not inherit from postgres here
