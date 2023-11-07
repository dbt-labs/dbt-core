from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from dbt_semantic_interfaces.parsing.where_filter.where_filter_parser import WhereFilterParser
from typing import List, Sequence, Tuple


@dataclass
class WhereFilter(dbtClassMixin):
    where_sql_template: str

    @property
    def call_parameter_sets(self) -> FilterCallParameterSets:
        return WhereFilterParser.parse_call_parameter_sets(self.where_sql_template)


@dataclass
class WhereFilterIntersection(dbtClassMixin):
    where_filters: List[WhereFilter]

    @property
    def filter_expression_parameter_sets(self) -> Sequence[Tuple[str, FilterCallParameterSets]]:
        raise NotImplementedError
