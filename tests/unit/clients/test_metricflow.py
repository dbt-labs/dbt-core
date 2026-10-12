import pytest

from dbt.clients.metricflow import _ENGINES_BY_ADAPTER_TYPE, RenderOnlySqlClient
from dbt.exceptions import DbtRuntimeError


@pytest.mark.parametrize("adapter_type", sorted(_ENGINES_BY_ADAPTER_TYPE))
def test_supported_adapter_types_have_an_engine_and_renderer(adapter_type):
    client = RenderOnlySqlClient(adapter_type)
    engine, renderer_class = _ENGINES_BY_ADAPTER_TYPE[adapter_type]
    assert client.sql_engine_type == engine
    assert isinstance(client.sql_plan_renderer, renderer_class)


def test_unsupported_adapter_type_names_the_supported_ones():
    with pytest.raises(DbtRuntimeError, match="does not support the 'spark' adapter") as exc_info:
        RenderOnlySqlClient("spark")
    assert "postgres" in str(exc_info.value)


@pytest.mark.parametrize("method", ["query", "execute", "dry_run"])
def test_the_client_never_runs_sql(method):
    client = RenderOnlySqlClient("postgres")
    with pytest.raises(DbtRuntimeError, match="never run by MetricFlow"):
        getattr(client, method)("select 1")


def test_bind_parameter_keys_are_rendered():
    assert RenderOnlySqlClient("postgres").render_bind_parameter_key("a") == ":a"
