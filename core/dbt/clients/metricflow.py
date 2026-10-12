from typing import Dict, NoReturn, Tuple, Type

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlPlanRenderer
from metricflow.sql.render.big_query import BigQuerySqlPlanRenderer
from metricflow.sql.render.databricks import DatabricksSqlPlanRenderer
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer
from metricflow.sql.render.postgres import PostgresSQLSqlPlanRenderer
from metricflow.sql.render.redshift import RedshiftSqlPlanRenderer
from metricflow.sql.render.snowflake import SnowflakeSqlPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer
from metricflow.sql.render.trino import TrinoSqlPlanRenderer
from metricflow.sql.render.vertica import VerticaSqlPlanRenderer
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantics.model.dbt_manifest_parser import (
    transform_dbt_generated_manifest,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

from dbt.exceptions import DbtRuntimeError

# The adapters MetricFlow can render SQL for, keyed by the adapter type name. This mirrors
# the mapping `dbt-metricflow` uses for `dbt sl query`.
_ENGINES_BY_ADAPTER_TYPE: Dict[str, Tuple[SqlEngine, Type[SqlPlanRenderer]]] = {
    "athena": (SqlEngine.ATHENA, AthenaSqlPlanRenderer),
    "bigquery": (SqlEngine.BIGQUERY, BigQuerySqlPlanRenderer),
    "databricks": (SqlEngine.DATABRICKS, DatabricksSqlPlanRenderer),
    "duckdb": (SqlEngine.DUCKDB, DuckDbSqlPlanRenderer),
    "postgres": (SqlEngine.POSTGRES, PostgresSQLSqlPlanRenderer),
    "redshift": (SqlEngine.REDSHIFT, RedshiftSqlPlanRenderer),
    "snowflake": (SqlEngine.SNOWFLAKE, SnowflakeSqlPlanRenderer),
    "trino": (SqlEngine.TRINO, TrinoSqlPlanRenderer),
    "vertica": (SqlEngine.VERTICA, VerticaSqlPlanRenderer),
}


class RenderOnlySqlClient:
    """A MetricFlow `SqlClient` that can plan and render queries, but never runs them.

    MetricFlow needs a SQL client to know which dialect to render. dbt-core only ever asks
    MetricFlow what a query would be, and runs the resulting SQL itself, so every method that
    would talk to a warehouse is an error.
    """

    def __init__(self, adapter_type: str) -> None:
        try:
            self._sql_engine_type, renderer_class = _ENGINES_BY_ADAPTER_TYPE[adapter_type]
        except KeyError:
            raise DbtRuntimeError(
                f"The semantic layer does not support the '{adapter_type}' adapter. Supported "
                f"adapters are: {', '.join(sorted(_ENGINES_BY_ADAPTER_TYPE))}"
            ) from None
        self._sql_plan_renderer = renderer_class()

    @property
    def sql_engine_type(self) -> SqlEngine:
        return self._sql_engine_type

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        return self._sql_plan_renderer

    def _unsupported(self) -> NoReturn:
        raise DbtRuntimeError("MetricFlow queries are rendered by dbt, never run by MetricFlow")

    def query(
        self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    ) -> MetricFlowDataTable:
        self._unsupported()

    def execute(
        self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    ) -> None:
        self._unsupported()

    def dry_run(
        self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()
    ) -> None:
        self._unsupported()

    def close(self) -> None:
        pass

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f":{bind_parameter_key}"


def build_metricflow_engine(
    semantic_manifest: PydanticSemanticManifest, adapter_type: str
) -> MetricFlowEngine:
    """Build a MetricFlow engine from the semantic manifest dbt-core builds while parsing.

    The manifest is transformed first, as MetricFlow expects, because dbt-core's own does not
    have every transformation applied (this is what `dbt-metricflow` does when it reads
    `semantic_manifest.json`). This is not cheap, so callers should build one engine and reuse it.
    """
    return MetricFlowEngine(
        semantic_manifest_lookup=SemanticManifestLookup(
            transform_dbt_generated_manifest(semantic_manifest)
        ),
        sql_client=RenderOnlySqlClient(adapter_type),
    )
