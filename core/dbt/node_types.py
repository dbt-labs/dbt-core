from enum import Enum


class NodeType(str, Enum):
    Base = 'base'
    Model = 'model'
    Analysis = 'analysis'
    Test = 'test'
    Snapshot = 'snapshot'
    Macro = 'macro'
    Operation = 'operation'
    Seed = 'seed'
    Documentation = 'docs'
    Source = 'source'
    RPCCall = 'rpc'

    def __str__(self):
        return self._value_

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


class RunHookType(str, Enum):
    Start = 'on-run-start'
    End = 'on-run-end'

    def __str__(self):
        return self._value_
