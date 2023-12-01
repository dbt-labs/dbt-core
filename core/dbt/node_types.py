from typing import List

from dbt.dataclass_schema import StrEnum


class AccessType(StrEnum):
    Private = "private"
    Protected = "protected"
    Public = "public"

    @classmethod
    def is_valid(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class NodeType(StrEnum):
    Model = "model"
    Analysis = "analysis"
    Test = "data_test"
    Snapshot = "snapshot"
    Operation = "operation"
    Seed = "seed"
    # TODO: rm?
    RPCCall = "rpc"
    SqlOperation = "sql_operation"
    Documentation = "doc"
    Source = "source"
    Macro = "macro"
    Exposure = "exposure"
    Metric = "metric"
    Group = "group"
    SavedQuery = "saved_query"
    SemanticModel = "semantic_model"
    Unit = "unit_test"

    @classmethod
    def executable(cls) -> List["NodeType"]:
        return [
            cls.Model,
            cls.Test,
            cls.Snapshot,
            cls.Analysis,
            cls.Operation,
            cls.Seed,
            cls.Documentation,
            cls.RPCCall,
            cls.SqlOperation,
        ]

    @classmethod
    def refable(cls) -> List["NodeType"]:
        return [
            cls.Model,
            cls.Seed,
            cls.Snapshot,
        ]

    @classmethod
    def versioned(cls) -> List["NodeType"]:
        return [
            cls.Model,
        ]

    def pluralize(self) -> str:
        if self is self.Analysis:
            return "analyses"
        elif self is self.SavedQuery:
            return "saved_queries"
        return f"{self}s"


class RunHookType(StrEnum):
    Start = "on-run-start"
    End = "on-run-end"


class ModelLanguage(StrEnum):
    python = "python"
    sql = "sql"
