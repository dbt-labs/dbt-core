import re
from unittest import TestCase, mock

from dbt.adapters.base.query_headers import MacroQueryStringSetter

from tests.unit.utils import config_from_parts_or_dicts
from dbt.flags import set_from_args
from argparse import Namespace

set_from_args(Namespace(WARN_ERROR=False), None)


class TestQueryHeaders(TestCase):
    def setUp(self):
        self.profile_cfg = {
            "outputs": {
                "test": {
                    "type": "postgres",
                    "dbname": "postgres",
                    "user": "test",
                    "host": "test",
                    "pass": "test",
                    "port": 5432,
                    "schema": "test",
                },
            },
            "target": "test",
        }
        self.project_cfg = {
            "name": "query_headers",
            "version": "0.1",
            "profile": "test",
            "config-version": 2,
        }
        self.query = "SELECT 1;"

    def test_comment_should_prepend_query_by_default(self):
        config = config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)
        query_header = MacroQueryStringSetter(config, mock.MagicMock(macros={}))
        sql = query_header.add(self.query)
        self.assertTrue(re.match(f"^\/\*.*\*\/\n{self.query}$", sql))  # noqa: [W605]

    def test_append_comment(self):
        self.project_cfg.update({"query-comment": {"comment": "executed by dbt", "append": True}})
        config = config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)
        query_header = MacroQueryStringSetter(config, mock.MagicMock(macros={}))
        sql = query_header.add(self.query)
        self.assertEqual(sql, f"{self.query[:-1]}\n/* executed by dbt */;")

    def test_disable_query_comment(self):
        self.project_cfg.update({"query-comment": ""})
        config = config_from_parts_or_dicts(self.project_cfg, self.profile_cfg)
        query_header = MacroQueryStringSetter(config, mock.MagicMock(macros={}))
        self.assertEqual(query_header.add(self.query), self.query)
