
from dbt.parser.base_sql import BaseSqlParser


class ModelParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return relative_path
