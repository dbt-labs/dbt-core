import enum
from typing import List, Optional, Union, no_type_check

from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import (
    datasource_dataset,
    documentation_dataset,
    schema_dataset,
)

from dbt.adapters.contracts.connection import Credentials
from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import (
    GenericTestNode,
    ManifestNode,
    SeedNode,
    SourceDefinition,
)


class Adapter(enum.Enum):
    # supported adapters
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"
    POSTGRES = "postgres"
    DATABRICKS = "databricks"
    SQLSERVER = "sqlserver"
    DREMIO = "dremio"
    ATHENA = "athena"
    DUCKDB = "duckdb"
    TRINO = "trino"

    @staticmethod
    def adapters() -> str:
        # String representation of all supported adapter names
        return ",".join([f"`{x.value}`" for x in list(Adapter)])


class SparkConnectionMethod(enum.Enum):
    THRIFT = "thrift"
    ODBC = "odbc"
    HTTP = "http"

    @staticmethod
    def methods():
        return [x.value for x in SparkConnectionMethod]


def extract_schema_dataset_facet(
    node: Union[ManifestNode, SourceDefinition],
) -> List[schema_dataset.SchemaDatasetFacetFields]:
    if node.resource_type == NodeType.Seed:
        return _extract_schema_dataset_from_seed(node)
    else:
        return _extract_schema_dataset_facet_from_manifest_sql_node(node)


def _extract_schema_dataset_facet_from_manifest_sql_node(
    manifest_sql_node: Union[ManifestNode, SourceDefinition],
) -> List[schema_dataset.SchemaDatasetFacetFields]:
    schema_fields = []
    for column_info in manifest_sql_node.columns.values():
        description = column_info.description
        name = column_info.name
        data_type = column_info.data_type or ""
        schema_fields.append(
            schema_dataset.SchemaDatasetFacetFields(
                name=name, description=description, type=data_type
            )
        )
    return schema_fields


def _extract_schema_dataset_from_seed(
    seed: SeedNode,
) -> List[schema_dataset.SchemaDatasetFacetFields]:
    schema_fields = []
    for col_name in seed.config.column_types:
        col_type = seed.config.column_types[col_name]
        schema_fields.append(schema_dataset.SchemaDatasetFacetFields(name=col_name, type=col_type))
    return schema_fields


def get_model_inputs(
    node_unique_id: str, manifest: Manifest
) -> List[ManifestNode | SourceDefinition]:
    upstreams: List[ManifestNode | SourceDefinition] = []
    input_node_ids = manifest.parent_map.get(node_unique_id, [])
    for input_node_id in input_node_ids:
        if input_node_id.startswith("source."):
            upstreams.append(manifest.sources[input_node_id])
        else:
            upstreams.append(manifest.nodes[input_node_id])
    return upstreams


def node_to_dataset(
    node: Union[ManifestNode, SourceDefinition], dataset_namespace: str
) -> Dataset:
    facets = {
        "dataSource": datasource_dataset.DatasourceDatasetFacet(
            name=dataset_namespace, uri=dataset_namespace
        ),
        "schema": schema_dataset.SchemaDatasetFacet(fields=extract_schema_dataset_facet(node)),
        "documentation": documentation_dataset.DocumentationDatasetFacet(
            description=node.description
        ),
    }
    node_fqn = ".".join(node.fqn)
    return Dataset(namespace=dataset_namespace, name=node_fqn, facets=facets)


def get_test_column(test_node: GenericTestNode) -> Optional[str]:
    return test_node.test_metadata.kwargs.get("column_name")


@no_type_check
def extract_namespace(adapter: Credentials) -> str:
    # Extract namespace from profile's type
    if adapter.type == Adapter.SNOWFLAKE.value:
        return f"snowflake://{_fix_account_name(adapter.account)}"
    elif adapter.type == Adapter.BIGQUERY.value:
        return "bigquery"
    elif adapter.type == Adapter.REDSHIFT.value:
        return f"redshift://{adapter.host}:{adapter.port}"
    elif adapter.type == Adapter.POSTGRES.value:
        return f"postgres://{adapter.host}:{adapter.port}"
    elif adapter.type == Adapter.TRINO.value:
        return f"trino://{adapter.host}:{adapter.port}"
    elif adapter.type == Adapter.DATABRICKS.value:
        return f"databricks://{adapter.host}"
    elif adapter.type == Adapter.SQLSERVER.value:
        return f"mssql://{adapter.server}:{adapter.port}"
    elif adapter.type == Adapter.DREMIO.value:
        return f"dremio://{adapter.software_host}:{adapter.port}"
    elif adapter.type == Adapter.ATHENA.value:
        return f"awsathena://athena.{adapter.region_name}.amazonaws.com"
    elif adapter.type == Adapter.DUCKDB.value:
        return f"duckdb://{adapter.path}"
    elif adapter.type == Adapter.SPARK.value:
        port = ""
        if hasattr(adapter, "port"):
            port = f":{adapter.port}"
        elif adapter.method in [
            SparkConnectionMethod.HTTP.value,
            SparkConnectionMethod.ODBC.value,
        ]:
            port = "443"
        elif adapter.method == SparkConnectionMethod.THRIFT.value:
            port = "10001"

        if adapter.method in SparkConnectionMethod.methods():
            return f"spark://{adapter.host}{port}"
        else:
            raise NotImplementedError(
                f"Connection method `{adapter.method}` is not " f"supported for spark adapter."
            )
    else:
        raise NotImplementedError(
            f"Only {Adapter.adapters()} adapters are supported right now. "
            f"Passed {adapter.type}"
        )


def _fix_account_name(name: str) -> str:
    if not any(word in name for word in ["-", "_"]):
        # If there is neither '-' nor '_' in the name, we append `.us-west-1.aws`
        return f"{name}.us-west-1.aws"

    if "." in name:
        # Logic for account locator with dots remains unchanged
        spl = name.split(".")
        if len(spl) == 1:
            account = spl[0]
            region, cloud = "us-west-1", "aws"
        elif len(spl) == 2:
            account, region = spl
            cloud = "aws"
        else:
            account, region, cloud = spl
        return f"{account}.{region}.{cloud}"

    # Check for existing accounts with cloud names
    if cloud := next((c for c in ["aws", "gcp", "azure"] if c in name), ""):
        parts = name.split(cloud)
        account = parts[0].strip("-_.")

        if not (region := parts[1].strip("-_.").replace("_", "-")):
            return name
        return f"{account}.{region}.{cloud}"

    # Default case, return the original name
    return name
