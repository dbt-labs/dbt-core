
from dbt.parser.base_sql import BaseSqlParser
import dbt.utils


class DataTestParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return dbt.utils.get_pseudo_test_path(name, relative_path, 'data_test')
