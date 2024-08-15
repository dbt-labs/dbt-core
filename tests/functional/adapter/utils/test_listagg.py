import pytest

from tests.functional.adapter.utils.base_utils import BaseUtils
from tests.functional.adapter.utils.fixture_listagg import (
    models__test_listagg_sql,
    models__test_listagg_yml,
    seeds__data_listagg_csv,
    seeds__data_listagg_output_csv,
)


class BaseListagg(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_sql, "listagg"
            ),
        }


class TestListagg(BaseListagg):
    pass
