
class NodeType(object):
    Base = 'base'
    Model = 'model'
    Analysis = 'analysis'
    Test = 'test'
    Archive = 'archive'
    Macro = 'macro'
    Operation = 'operation'
    Seed = 'seed'
    Documentation = 'documentation'

    @classmethod
    def executable(cls):
        return [
            cls.Model,
            cls.Test,
            cls.Archive,
            cls.Analysis,
            cls.Operation,
            cls.Seed,
            cls.Documentation,
        ]

    @classmethod
    def refable(cls):
        return [
            cls.Model,
            cls.Seed,
        ]


class RunHookType:
    Start = 'on-run-start'
    End = 'on-run-end'
    Both = [Start, End]
