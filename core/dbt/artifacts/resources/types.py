from dbt_common.dataclass_schema import StrEnum


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
    Test = "test"  # renamed to 'data_test'; preserved as 'test' here for back-compat
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
    Fixture = "fixture"
    Function = "function"

    def pluralize(self) -> str:
        if self is self.Analysis:
            return "analyses"
        elif self is self.SavedQuery:
            return "saved_queries"
        elif self is self.Test:
            return "data_tests"
        return f"{self}s"


class RunHookType(StrEnum):
    Start = "on-run-start"
    End = "on-run-end"


class ModelLanguage(StrEnum):
    python = "python"
    sql = "sql"


class ModelHookType(StrEnum):
    PreHook = "pre-hook"
    PostHook = "post-hook"


class TimePeriod(StrEnum):
    minute = "minute"
    hour = "hour"
    day = "day"

    def plural(self) -> str:
        return str(self) + "s"


class BatchSize(StrEnum):
    hour = "hour"
    day = "day"
    month = "month"
    year = "year"

    def plural(self) -> str:
        return str(self) + "s"


class FunctionType(StrEnum):
    Scalar = "scalar"
    Aggregate = "aggregate"
    Table = "table"


class FunctionVolatility(StrEnum):
    Deterministic = "deterministic"
    Stable = "stable"
    NonDeterministic = "non-deterministic"
