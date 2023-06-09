from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.references import DimensionReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing import Optional


@dataclass
class FileSlice(dbtClassMixin):
    """Provides file slice level context about what something was created from.

    Implementation of the dbt-semantic-interfaces `FileSlice` protocol
    """

    filename: str
    content: str
    start_line_number: int
    end_line_number: int


@dataclass
class SourceFileMetadata(dbtClassMixin):
    """Provides file context about what something was created from.

    Implementation of the dbt-semantic-interfaces `Metadata` protocol
    """

    repo_file_path: str
    file_slice: FileSlice


@dataclass
class DimensionValidityParams(dbtClassMixin):
    is_start: bool = False
    is_end: bool = False


@dataclass
class DimensionTypeParams(dbtClassMixin):
    time_granularity: TimeGranularity
    validity_params: Optional[DimensionValidityParams] = None


@dataclass
class Dimension(dbtClassMixin):
    name: str
    type: DimensionType
    description: Optional[str] = None
    is_partition: bool = False
    type_params: Optional[DimensionTypeParams] = None
    expr: Optional[str] = None
    metadata: Optional[SourceFileMetadata] = None

    @property
    def reference(self) -> DimensionReference:
        return DimensionReference(element_name=self.name)

    @property
    def time_dimension_reference(self) -> Optional[TimeDimensionReference]:
        if self.type == DimensionType.TIME:
            return TimeDimensionReference(element_name=self.name)
        else:
            return None

    @property
    def validity_params(self) -> Optional[DimensionValidityParams]:
        if self.type_params:
            return self.type_params.validity_params
        else:
            return None
