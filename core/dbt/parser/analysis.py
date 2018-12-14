
from dbt.parser.base_sql import BaseSqlParser
import os


class AnalysisParser(BaseSqlParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return os.path.join('analysis', relative_path)
