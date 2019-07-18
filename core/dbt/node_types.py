from hologram.helpers import StrEnum


class NodeType(StrEnum):
    Model = 'model'
    Analysis = 'analysis'
    Test = 'test'
    Snapshot = 'snapshot'
    Operation = 'operation'
    Seed = 'seed'
    RPCCall = 'rpc'
    Documentation = 'docs'
    Source = 'source'
    Macro = 'macro'

    @classmethod
    def executable(cls):
        return [v.value for v in [
            cls.Model,
            cls.Test,
            cls.Snapshot,
            cls.Analysis,
            cls.Operation,
            cls.Seed,
            cls.Documentation,
            cls.RPCCall,
        ]]

    @classmethod
    def refable(cls):
        return [v.value for v in [
            cls.Model,
            cls.Seed,
            cls.Snapshot,
        ]]


class UnparsedNodeType(StrEnum):
    Model = str(NodeType.Model)
    Analysis = str(NodeType.Analysis)
    Test = str(NodeType.Test)
    Snapshot = str(NodeType.Snapshot)
    Operation = str(NodeType.Operation)
    Seed = str(NodeType.Seed)
    RPCCall = str(NodeType.RPCCall)


class RunHookType(StrEnum):
    Start = 'on-run-start'
    End = 'on-run-end'

# It would be nice to use hologram.StrLiteral for these, but it results in
# un-pickleable types :(


class AnalysisType(StrEnum):
    Analysis = str(NodeType.Analysis)


class DocumentationType(StrEnum):
    Documentation = str(NodeType.Documentation)


class MacroType(StrEnum):
    Macro = str(NodeType.Macro)


class ModelType(StrEnum):
    Model = str(NodeType.Model)


class OperationType(StrEnum):
    Operation = str(NodeType.Operation)


class RPCCallType(StrEnum):
    RPCCall = str(NodeType.RPCCall)


class SeedType(StrEnum):
    Seed = str(NodeType.Seed)


class SnapshotType(StrEnum):
    Snapshot = str(NodeType.Snapshot)


class SourceType(StrEnum):
    Source = str(NodeType.Source)


class TestType(StrEnum):
    Test = str(NodeType.Test)
