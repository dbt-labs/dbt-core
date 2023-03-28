import os

from dbt.context.base import BaseContext
from jinja2.runtime import Undefined


class TestBaseContext:
    def test_log_jinja_undefined(self):
        # regression test for CT-2259
        try:
            os.environ["DBT_ENV_SECRET_LOG_TEST"] = "cats_are_cool"
            BaseContext.log(msg=Undefined(), info=True)
        except Exception as e:
            assert False, f"Logging an jinja2.Undefined object raises an exception: {e}"

    def test_log_with_dbt_env_secret(self):
        # regression test for CT-1783
        try:
            os.environ["DBT_ENV_SECRET_LOG_TEST"] = "cats_are_cool"
            BaseContext.log({"fact1": "I like cats"}, info=True)
        except Exception as e:
            assert False, f"Logging while a `DBT_ENV_SECRET` was set raised an exception: {e}"
