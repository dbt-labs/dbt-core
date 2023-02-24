from typing import List

from dbt.dataclass_schema import StrEnum


class AccessType(StrEnum):
    Protected = "protected"
    Private = "private"
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
    Test = "test"
    Snapshot = "snapshot"
    Operation = "operation"
    Seed = "seed"
    # TODO: rm?
    RPCCall = "rpc"
    SqlOperation = "sql operation"
    Documentation = "doc"
    Source = "source"
    Macro = "macro"
    Exposure = "exposure"
    Metric = "metric"
    Group = "group"

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
    def documentable(cls) -> List["NodeType"]:
        return [
            cls.Model,
            cls.Seed,
            cls.Snapshot,
            cls.Source,
            cls.Macro,
            cls.Analysis,
            cls.Exposure,
            cls.Metric,
        ]

    def pluralize(self) -> str:
        if self is self.Analysis:
            return "analyses"
        return f"{self}s"


class RunHookType(StrEnum):
    Start = "on-run-start"
    End = "on-run-end"


class ModelLanguage(StrEnum):
    python = "python"
    sql = "sql"
