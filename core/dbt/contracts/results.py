from dbt.contracts.graph.manifest import CompileResultNode
from dbt.contracts.graph.unparsed import (
    Time, FreshnessStatus, FreshnessThreshold
)
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from dbt.contracts.util import Writable, Replaceable
from dbt.exceptions import InternalException
from dbt.logger import (
    TimingProcessor,
    JsonOnly,
    GLOBAL_LOGGER as logger,
)
from dbt.utils import lowercase
from hologram.helpers import StrEnum
from hologram import JsonSchemaMixin

import agate

from dataclasses import dataclass, field
from datetime import datetime
from typing import Union, Dict, List, Optional, Any, NamedTuple


@dataclass
class TimingInfo(JsonSchemaMixin):
    name: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def begin(self):
        self.started_at = datetime.utcnow()

    def end(self):
        self.completed_at = datetime.utcnow()


class collect_timing_info:
    def __init__(self, name: str):
        self.timing_info = TimingInfo(name=name)

    def __enter__(self):
        self.timing_info.begin()
        return self.timing_info

    def __exit__(self, exc_type, exc_value, traceback):
        self.timing_info.end()
        with JsonOnly(), TimingProcessor(self.timing_info):
            logger.debug('finished collecting timing info')


@dataclass
class PartialResult(JsonSchemaMixin, Writable):
    node: CompileResultNode
    error: Optional[str] = None
    status: Union[None, str, int, bool] = None
    execution_time: Union[str, int] = 0
    thread_id: Optional[str] = None
    timing: List[TimingInfo] = field(default_factory=list)
    fail: Optional[bool] = None
    warn: Optional[bool] = None

    # if the result got to the point where it could be skipped/failed, we would
    # be returning a real result, not a partial.
    @property
    def skipped(self):
        return False


@dataclass
class WritableRunModelResult(PartialResult):
    skip: bool = False

    @property
    def skipped(self):
        return self.skip


@dataclass
class RunModelResult(WritableRunModelResult):
    agate_table: Optional[agate.Table] = None

    def to_dict(self, *args, **kwargs):
        dct = super().to_dict(*args, **kwargs)
        dct.pop('agate_table', None)
        return dct


@dataclass
class ExecutionResult(JsonSchemaMixin, Writable):
    results: List[Union[WritableRunModelResult, PartialResult]]
    generated_at: datetime
    elapsed_time: float

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, idx):
        return self.results[idx]


@dataclass
class RunOperationResult(ExecutionResult):
    success: bool


# due to issues with typing.Union collapsing subclasses, this can't subclass
# PartialResult
@dataclass
class SourceFreshnessResult(JsonSchemaMixin, Writable):
    node: ParsedSourceDefinition
    max_loaded_at: datetime
    snapshotted_at: datetime
    age: float
    status: FreshnessStatus
    error: Optional[str] = None
    execution_time: Union[str, int] = 0
    thread_id: Optional[str] = None
    timing: List[TimingInfo] = field(default_factory=list)
    fail: Optional[bool] = None

    def __post_init__(self):
        self.fail = self.status == 'error'

    @property
    def warned(self):
        return self.status == 'warn'

    @property
    def skipped(self):
        return False


@dataclass
class FreshnessMetadata(JsonSchemaMixin):
    generated_at: datetime
    elapsed_time: float


@dataclass
class FreshnessExecutionResult(FreshnessMetadata):
    results: List[Union[PartialResult, SourceFreshnessResult]]

    def write(self, path, omit_none=True):
        """Create a new object with the desired output schema and write it."""
        meta = FreshnessMetadata(
            generated_at=self.generated_at,
            elapsed_time=self.elapsed_time,
        )
        sources = {}
        for result in self.results:
            result_value: Union[
                SourceFreshnessRuntimeError, SourceFreshnessOutput
            ]
            unique_id = result.node.unique_id
            if result.error is not None:
                result_value = SourceFreshnessRuntimeError(
                    error=result.error,
                    state=FreshnessErrorEnum.runtime_error,
                )
            else:
                # we know that this must be a SourceFreshnessResult
                if not isinstance(result, SourceFreshnessResult):
                    raise InternalException(
                        'Got {} instead of a SourceFreshnessResult for a '
                        'non-error result in freshness execution!'
                        .format(type(result))
                    )
                # if we're here, we must have a non-None freshness threshold
                criteria = result.node.freshness
                if criteria is None:
                    raise InternalException(
                        'Somehow evaluated a freshness result for a source '
                        'that has no freshness criteria!'
                    )
                result_value = SourceFreshnessOutput(
                    max_loaded_at=result.max_loaded_at,
                    snapshotted_at=result.snapshotted_at,
                    max_loaded_at_time_ago_in_s=result.age,
                    state=result.status,
                    criteria=criteria,
                )
            sources[unique_id] = result_value
        output = FreshnessRunOutput(meta=meta, sources=sources)
        output.write(path, omit_none=omit_none)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, idx):
        return self.results[idx]


def _copykeys(src, keys, **updates):
    return {k: getattr(src, k) for k in keys}


@dataclass
class FreshnessCriteria(JsonSchemaMixin):
    warn_after: Time
    error_after: Time


class FreshnessErrorEnum(StrEnum):
    runtime_error = 'runtime error'


@dataclass
class SourceFreshnessRuntimeError(JsonSchemaMixin):
    error: str
    state: FreshnessErrorEnum


@dataclass
class SourceFreshnessOutput(JsonSchemaMixin):
    max_loaded_at: datetime
    snapshotted_at: datetime
    max_loaded_at_time_ago_in_s: float
    state: FreshnessStatus
    criteria: FreshnessThreshold


SourceFreshnessRunResult = Union[SourceFreshnessOutput,
                                 SourceFreshnessRuntimeError]


@dataclass
class FreshnessRunOutput(JsonSchemaMixin, Writable):
    meta: FreshnessMetadata
    sources: Dict[str, SourceFreshnessRunResult]


Primitive = Union[bool, str, float, None]

CatalogKey = NamedTuple(
    'CatalogKey',
    [('database', Optional[str]), ('schema', str), ('name', str)]
)


@dataclass
class StatsItem(JsonSchemaMixin):
    id: str
    label: str
    value: Primitive
    description: Optional[str]
    include: bool


StatsDict = Dict[str, StatsItem]


@dataclass
class ColumnMetadata(JsonSchemaMixin):
    type: str
    comment: Optional[str]
    index: int
    name: str


ColumnMap = Dict[str, ColumnMetadata]


@dataclass
class TableMetadata(JsonSchemaMixin):
    type: str
    database: Optional[str]
    schema: str
    name: str
    comment: Optional[str]
    owner: Optional[str]


@dataclass
class CatalogTable(JsonSchemaMixin, Replaceable):
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
class CatalogResults(JsonSchemaMixin, Writable):
    nodes: Dict[str, CatalogTable]
    sources: Dict[str, CatalogTable]
    generated_at: datetime
    errors: Optional[List[str]]
    _compile_results: Optional[Any] = None
