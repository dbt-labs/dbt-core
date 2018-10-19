from dbt.adapters.redshift.connections import RedshiftConnectionManager
from dbt.adapters.redshift.connections import RedshiftCredentials
from dbt.adapters.redshift.impl import RedshiftAdapter
from dbt.adapters.factory import register_adapter_type

register_adapter_type(RedshiftAdapter, RedshiftCredentials)
