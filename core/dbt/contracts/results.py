from dbt.api.object import APIObject
from dbt.utils import deep_merge
from dbt.contracts.common import named_property
from dbt.contracts.graph.manifest import COMPILE_RESULT_NODE_CONTRACT
from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT
from dbt.contracts.graph.compiled import COMPILED_NODE_CONTRACT
from dbt.contracts.graph.manifest import PARSED_MANIFEST_CONTRACT

RUN_MODEL_RESULT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'The result of a single node being run',
    'properties': {
        'error': {
            'type': ['string', 'null'],
            'description': 'The error string, or None if there was no error',
        },
        'skip': {
            'type': 'boolean',
            'description': 'True if this node was skipped',
        },
        # This is assigned by dbt.ui.printer.print_test_result_line, if a test
        # has no error and a non-zero status
        'fail': {
            'type': ['boolean', 'null'],
            'description': 'On tests, true if the test failed',
        },
        'status': {
            'type': ['string', 'null', 'number', 'boolean'],
            'description': 'The status result of the node execution',
        },
        'execution_time': {
            'type': 'number',
            'description': 'The execution time, in seconds',
        },
        'node': COMPILE_RESULT_NODE_CONTRACT,
    },
    'required': ['node'],
}


class RunModelResult(APIObject):
    SCHEMA = RUN_MODEL_RESULT_CONTRACT

    def __init__(self, node, error=None, skip=False, status=None, failed=None,
                 execution_time=0):
        super(RunModelResult, self).__init__(node=node, error=error, skip=skip,
                                             status=status, fail=failed,
                                             execution_time=execution_time)

    # these all get set after the fact, generally
    error = named_property('error',
                           'If there was an error, the text of that error')
    skip = named_property('skip', 'True if the model was skipped')
    fail = named_property('fail', 'True if this was a test and it failed')
    status = named_property('status', 'The status of the model execution')
    execution_time = named_property('execution_time',
                                    'The time in seconds to execute the model')

    @property
    def errored(self):
        return self.error is not None

    @property
    def failed(self):
        return self.fail

    @property
    def skipped(self):
        return self.skip

    def serialize(self):
        result = super(RunModelResult, self).serialize()
        result['node'] = self.node.serialize()
        return result


EXECUTION_RESULT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'The result of a single dbt invocation',
    'properties': {
        'results': {
            'type': 'array',
            'items': RUN_MODEL_RESULT_CONTRACT,
            'description': 'An array of results, one per model',
        },
        'generated_at': {
            'type': 'string',
            'format': 'date-time',
            'description': (
                'The time at which the execution result was generated'
            ),
        },
        'elapsed_time': {
            'type': 'number',
            'description': (
                'The time elapsed from before_run to after_run (hooks are not '
                'included)'
            ),
        }
    },
    'required': ['results', 'generated_at', 'elapsed_time'],
}


class ExecutionResult(APIObject):
    SCHEMA = EXECUTION_RESULT_CONTRACT

    def serialize(self):
        return {
            'results': [r.serialize() for r in self.results],
            'generated_at': self.generated_at,
            'elapsed_time': self.elapsed_time,
        }
