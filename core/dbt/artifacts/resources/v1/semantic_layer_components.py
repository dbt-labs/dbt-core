from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from dbt_common.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.call_parameter_sets import JinjaCallParameterSets
from dbt_semantic_interfaces.parsing.where_filter.jinja_object_parser import (
    JinjaObjectParser,
    QueryItemLocation,
)
from dbt_semantic_interfaces.type_enums import AggregationType


@dataclass
class WhereFilter(dbtClassMixin):
    where_sql_template: str

    def call_parameter_sets(
        self, custom_granularity_names: Sequence[str]
    ) -> JinjaCallParameterSets:
        return JinjaObjectParser.parse_call_parameter_sets(
            self.where_sql_template,
            custom_granularity_names=custom_granularity_names,
            query_item_location=QueryItemLocation.NON_ORDER_BY,
        )


@dataclass
class WhereFilterIntersection(dbtClassMixin):
    where_filters: List[WhereFilter]

    def filter_expression_parameter_sets(
        self, custom_granularity_names: Sequence[str]
    ) -> Sequence[Tuple[str, JinjaCallParameterSets]]:
        raise NotImplementedError


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
class MeasureAggregationParameters(dbtClassMixin):
    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


@dataclass
class NonAdditiveDimension(dbtClassMixin):
    name: str
    window_choice: AggregationType
    window_groupings: List[str]
