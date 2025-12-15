import pytest

from dbt.tests.util import run_dbt, write_file

models_get__any_model_sql = """
-- models/any_model.sql
select {{ config.get('made_up_nonexistent_key', 'default_value') }} as col_value

"""

meta_model_get_sql = """
-- models/meta_model.sql
select {{ config.get('meta_key', 'meta_default_value') }} as col_value
"""

meta_model_meta_get_sql = """
-- models/meta_model.sql
select {{ config.meta_get('meta_key', 'meta_default_value') }} as col_value
"""

schema_yml = """
models:
 - name: meta_model
   config:
     meta:
       meta_key: my_meta_value
"""

meta_model_require_sql = """
-- models/meta_model.sql
select {{ config.require('meta_key') }} as col_value
"""

meta_model_meta_require_sql = """
-- models/meta_model.sql
select {{ config.require('meta_key') }} as col_value
"""


class TestConfigGetDefault:
    @pytest.fixture(scope="class")
    def models(self):
        return {"any_model.sql": models_get__any_model_sql}

    def test_config_with_get_default(
        self,
        project,
    ):
        # This test runs a model with a config.get(key, default)
        # The default value is 'default_value' and causes an error
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert str(results[0].status) == "error"
        assert 'column "default_value" does not exist' in results[0].message


class TestConfigGetMeta:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "meta_model.sql": meta_model_get_sql,
            "schema.yml": schema_yml,
        }

    def test_config_with_meta_key(
        self,
        project,
    ):
        # This test runs a model with a config.get(key, default) -> default value returned
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert str(results[0].status) == "error"
        assert 'column "meta_default_value" does not exist' in results[0].message

        write_file(meta_model_meta_get_sql, "models", "meta_model.sql")
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert str(results[0].status) == "error"
        assert 'column "my_meta_value" does not exist' in results[0].message


class TestConfigGetMetaRequire:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "meta_model.sql": meta_model_require_sql,
            "schema.yml": schema_yml,
        }

    def test_config_with_meta_require(
        self,
        project,
    ):
        # This test runs a model with a config.require(key)
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert str(results[0].status) == "error"
        assert 'column "my_meta_value" does not exist' in results[0].message

        write_file(meta_model_meta_require_sql, "models", "meta_model.sql")
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert str(results[0].status) == "error"
        assert 'column "my_meta_value" does not exist' in results[0].message
