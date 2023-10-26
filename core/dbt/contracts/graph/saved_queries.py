from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.type_enums.export_destination_type import ExportDestinationType
from typing import Optional


@dataclass
class ExportConfig(dbtClassMixin):
    """Nested configuration attributes for exports."""

    export_as: ExportDestinationType
    schema_name: Optional[str] = None
    alias: Optional[str] = None


@dataclass
class Export(dbtClassMixin):
    """Configuration for writing query results to a table."""

    name: str
    config: ExportConfig
