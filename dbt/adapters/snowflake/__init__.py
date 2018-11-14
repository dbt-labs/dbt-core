from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.adapters.snowflake.connections import SnowflakeCredentials
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.impl import SnowflakeAdapter


Adapter = SnowflakeAdapter
Credentials = SnowflakeCredentials
