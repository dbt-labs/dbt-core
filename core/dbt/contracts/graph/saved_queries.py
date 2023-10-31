from __future__ import annotations

from dataclasses import dataclass
from dbt.contracts.graph.semantic_layer_common import WhereFilterIntersection
from dbt.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.type_enums.export_destination_type import ExportDestinationType
from typing import List, Optional


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

    def same_name(self, old: Export) -> bool:
        return self.name == old.name

    def same_export_as(self, old: Export) -> bool:
        return self.config.export_as == old.config.export_as

    def same_schema_name(self, old: Export) -> bool:
        return self.config.schema_name == old.config.schema_name

    def same_alias(self, old: Export) -> bool:
        return self.config.alias == old.config.alias

    def same_contents(self, old: Export) -> bool:
        return (
            self.same_name(old)
            and self.same_export_as(old)
            and self.same_schema_name(old)
            and self.same_alias(old)
        )


@dataclass
class QueryParams(dbtClassMixin):
    """The query parameters for the saved query"""

    metrics: List[str]
    group_by: List[str]
    where: Optional[WhereFilterIntersection]
