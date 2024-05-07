from typing import Dict, Union, Optional, NamedTuple, Any, List
from dataclasses import dataclass, field
from datetime import datetime

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.utils.formatting import lowercase
from dbt.artifacts.schemas.base import ArtifactMixin, BaseArtifactMetadata, schema_version

Primitive = Union[bool, str, float, None]
PrimitiveDict = Dict[str, Primitive]

CatalogKey = NamedTuple(
    "CatalogKey", [("database", Optional[str]), ("schema", str), ("name", str)]
)


@dataclass
class StatsItem(dbtClassMixin):
    id: str
    label: str
    value: Primitive
    include: bool
    description: Optional[str] = None


StatsDict = Dict[str, StatsItem]


@dataclass
class ColumnMetadata(dbtClassMixin):
    type: str
    index: int
    name: str
    comment: Optional[str] = None


ColumnMap = Dict[str, ColumnMetadata]


@dataclass
class TableMetadata(dbtClassMixin):
    type: str
    schema: str
    name: str
    database: Optional[str] = None
    comment: Optional[str] = None
    owner: Optional[str] = None


@dataclass
class CatalogTable(dbtClassMixin):
    metadata: TableMetadata
    columns: ColumnMap
    stats: StatsDict
    # the same table with two unique IDs will just be listed two times
    unique_id: Optional[str] = None

    def key(self) -> CatalogKey:
        return CatalogKey(
            lowercase(self.metadata.database),
            self.metadata.schema.lower(),
            self.metadata.name.lower(),
        )


@dataclass
class CatalogMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(
        default_factory=lambda: str(CatalogArtifact.dbt_schema_version)
    )


@dataclass
class CatalogResults(dbtClassMixin):
    nodes: Dict[str, CatalogTable]
    sources: Dict[str, CatalogTable]
    errors: Optional[List[str]] = None
    _compile_results: Optional[Any] = None

    def __post_serialize__(self, dct: Dict, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if "_compile_results" in dct:
            del dct["_compile_results"]
        return dct


@dataclass
@schema_version("catalog", 1)
class CatalogArtifact(CatalogResults, ArtifactMixin):
    metadata: CatalogMetadata

    @classmethod
    def from_results(
        cls,
        generated_at: datetime,
        nodes: Dict[str, CatalogTable],
        sources: Dict[str, CatalogTable],
        compile_results: Optional[Any],
        errors: Optional[List[str]],
    ) -> "CatalogArtifact":
        meta = CatalogMetadata(generated_at=generated_at)
        return cls(
            metadata=meta,
            nodes=nodes,
            sources=sources,
            errors=errors,
            _compile_results=compile_results,
        )
