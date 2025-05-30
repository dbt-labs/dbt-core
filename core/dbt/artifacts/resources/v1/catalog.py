from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dbt.adapters.catalogs import CatalogIntegrationConfig
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class CatalogWriteIntegrationConfig(CatalogIntegrationConfig):
    name: str
    catalog_type: str
    external_volume: Optional[str] = None
    table_format: Optional[str] = None
    catalog_name: Optional[str] = None
    file_format: Optional[str] = None
    adapter_properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Catalog(dbtClassMixin):
    name: str
    active_write_integration: Optional[str] = None
    write_integrations: List[CatalogWriteIntegrationConfig] = field(default_factory=list)
