
import re 
from typing import Mapping, Any, Optional

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.utils import filter_null_values
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import RuntimeException


GET_COLUMNS_IN_RELATION_MACRO_NAME = 'snowflake__get_columns_in_relation'

class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    ConnectionManager = SnowflakeConnectionManager

    AdapterSpecificConfigs = frozenset(
        {"transient", "cluster_by", "automatic_clustering", "secure",
         "copy_grants", "snowflake_warehouse"}
    )

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        return super()._catalog_filter_table(lowered, manifest)

    
    def get_columns_in_relation(self, relation):
        return self.execute_macro(
            GET_COLUMNS_IN_RELATION_MACRO_NAME,
            kwargs={'relation': relation}
        )


    def _make_match_kwargs(self, database, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {"identifier": identifier, "schema": schema, "database": database}
        )
    
    def has_schema_changed(self, temp_relation, target_relation):
        """
        Look for schema changes between the target columns and reference columns
        Step through each column and return that a schema change *has* happened
        if any of the following are true:
        1. Number of columns are different (column has been added or removed)
        2. Columns have different names
        3. Columns have different data type
        4. Columns have different data type size
        """
        reference_columns = {
            c.name: c for c in
            self.get_columns_in_relation(relation = temp_relation)
        }

        target_columns = {
            c.name: c for c in
            self.get_columns_in_relation(relation = target_relation)
        }

        # 1. The schema has changed if columns have been added or removed
        if len(reference_columns) != len(target_columns):
            logger.debug("Schema difference detected: column count in reference does not match target. reference columns: {}  target columns: {} ".format(reference_columns, target_columns))
            logger.debug("target_ref = {} ".format(target_columns))
            logger.debug("temp_ref = {} ".format(reference_columns))
            return True

        for reference_column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(reference_column_name)
            # 2a. The schema has changed if a reference column is not found in the target columns
            if target_column is None:
                logger.debug("Schema difference detected: reference column {} not found in target".format(reference_column_name))
                logger.debug("target_ref = {}".format(target_columns))
                logger.debug("temp_ref = {}".format(reference_columns))
                return True

            # 3/4. If the columns do not have the same data type and size (see core/dbt/schema.py for more details) 
            reference_type = re.findall(r"^(\w*)",reference_column.data_type) # just get the name of datatype, not size/precision
            target_type = re.findall(r"^(\w*)",target_column.data_type)
            if reference_type[0] != target_type[0]:
                logger.debug("Schema difference detected: Datatype {} in reference column {} do not match target datatype {}".format(reference_type, reference_column.name, target_type))
                logger.debug("target_ref = {}".format(target_columns))
                logger.debug("temp_ref = {}".format(reference_columns))
                return True

        for i, target_column_name in enumerate(target_columns):
            reference_column = reference_columns.get(target_column_name)
            target_column = target_columns.get(target_column_name)
            # 2b. The schema has changed if a target column is not found in the reference columns
            if reference_column is None:
                logger.debug("Schema difference detected: target column {} not found in reference".format(target_column_name))
                logger.debug("target_ref = {}".format(target_columns))
                logger.debug("temp_ref = {}".format(reference_columns))
                return True

        # Nothing has detected as changed
        logger.debug("No schema difference detected")
        logger.debug("target_ref = {}".format(target_columns))
        logger.debug("temp_ref = {}".format(reference_columns))
        return False

    def _get_warehouse(self) -> str:
        _, table = self.execute(
            'select current_warehouse() as warehouse',
            fetch=True
        )
        if len(table) == 0 or len(table[0]) == 0:
            # can this happen?
            raise RuntimeException(
                'Could not get current warehouse: no results'
            )
        return str(table[0][0])

    def _use_warehouse(self, warehouse: str):
        """Use the given warehouse. Quotes are never applied."""
        self.execute('use warehouse {}'.format(warehouse))

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        default_warehouse = self.config.credentials.warehouse
        warehouse = config.get('snowflake_warehouse', default_warehouse)
        if warehouse == default_warehouse or warehouse is None:
            return None
        previous = self._get_warehouse()
        self._use_warehouse(warehouse)
        return previous

    def post_model_hook(
        self, config: Mapping[str, Any], context: Optional[str]
    ) -> None:
        if context is not None:
            self._use_warehouse(context)
