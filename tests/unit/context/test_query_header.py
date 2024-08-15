import re
from unittest import mock

import pytest

from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.context.query_header import generate_query_header_context
from tests.unit.utils import config_from_parts_or_dicts


class TestQueryHeaderContext:
    @pytest.fixture
    def profile_cfg(self):
        return {
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

    @pytest.fixture
    def project_cfg(self):
        return {
            "name": "query_headers",
            "version": "0.1",
            "profile": "test",
            "config-version": 2,
        }

    @pytest.fixture
    def query(self):
        return "SELECT 1;"

    def test_comment_should_prepend_query_by_default(self, profile_cfg, project_cfg, query):
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        query_header_context = generate_query_header_context(config, mock.MagicMock(macros={}))
        query_header = MacroQueryStringSetter(config, query_header_context)
        sql = query_header.add(query)
        assert re.match(f"^\/\*.*\*\/\n{query}$", sql)  # noqa: [W605]

    def test_append_comment(self, profile_cfg, project_cfg, query):
        project_cfg.update({"query-comment": {"comment": "executed by dbt", "append": True}})
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        query_header_context = generate_query_header_context(config, mock.MagicMock(macros={}))
        query_header = MacroQueryStringSetter(config, query_header_context)
        sql = query_header.add(query)

        assert sql == f"{query[:-1]}\n/* executed by dbt */;"

    def test_disable_query_comment(self, profile_cfg, project_cfg, query):
        project_cfg.update({"query-comment": ""})
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        query_header = MacroQueryStringSetter(config, mock.MagicMock(macros={}))
        assert query_header.add(query) == query
