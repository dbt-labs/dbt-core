from dataclasses import dataclass, field
from typing import List, Optional

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class CatalogIntegration(dbtClassMixin):
    name: str
    external_volume: str
    table_format: str
    catalog_type: str


@dataclass
class Catalog(dbtClassMixin):
    name: str
    active_write_integration: Optional[str] = None
    write_integrations: List[CatalogIntegration] = field(default_factory=list)


@dataclass
class Catalogs(dbtClassMixin):
    catalogs: List[Catalog]
