from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from dbt.artifacts.schemas.base import (
    ArtifactMixin,
    BaseArtifactMetadata,
    schema_version,
)
from dbt_common.contracts.metadata import CatalogTable
from dbt_common.dataclass_schema import dbtClassMixin

Primitive = Union[bool, str, float, None]
PrimitiveDict = Dict[str, Primitive]


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
