from dbt.adapters.redshift.connections import RedshiftConnectionManager  # noqa
from dbt.adapters.redshift.connections import RedshiftCredentials
from dbt.adapters.redshift.relation import RedshiftColumn  # noqa
from dbt.adapters.redshift.relation import RedshiftRelation  # noqa: F401
from dbt.adapters.redshift.impl import RedshiftAdapter


from dbt.adapters.base import AdapterPlugin
from dbt.include import redshift

Plugin = AdapterPlugin(
    adapter=RedshiftAdapter,
    credentials=RedshiftCredentials,
    include_path=redshift.PACKAGE_PATH,
    dependencies=['postgres'])
