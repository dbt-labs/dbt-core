from __future__ import annotations

from dataclasses import dataclass
from dbt.artifacts.resources import WhereFilterIntersection
from dbt_common.dataclass_schema import dbtClassMixin
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


@dataclass
class QueryParams(dbtClassMixin):
    """The query parameters for the saved query"""

    metrics: List[str]
    group_by: List[str]
    where: Optional[WhereFilterIntersection]
