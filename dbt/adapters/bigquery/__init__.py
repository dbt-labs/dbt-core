from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.connections import BigQueryCredentials
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.factory import register_adapter_type

register_adapter_type(BigQueryAdapter, BigQueryCredentials)
