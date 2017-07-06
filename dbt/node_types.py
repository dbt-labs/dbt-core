
class NodeType(object):
    Base = 'base'
    Model = 'model'
    Analysis = 'analysis'
    Test = 'test'
    Archive = 'archive'
    Macro = 'macro'
    Operation = 'operation'

    @classmethod
    def executable(cls):
        return [
            cls.Model,
            cls.Test,
            cls.Archive,
            cls.Analysis,
            cls.Operation
        ]


class RunHookType:
    Start = 'on-run-start'
    End = 'on-run-end'
    Both = [Start, End]
