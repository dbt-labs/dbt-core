from dbt.api.object import APIObject
from dbt.utils import deep_merge, timestring
from dbt.contracts.common import named_property
from dbt.contracts.graph.manifest import COMPILE_RESULT_NODE_CONTRACT
from dbt.contracts.graph.unparsed import TIME_CONTRACT
from dbt.contracts.graph.parsed import PARSED_SOURCE_DEFINITION_CONTRACT


TIMING_INFO_CONTRACT = {
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string',
        },
        'started_at': {
            'type': 'string',
            'format': 'date-time',
        },
        'completed_at': {
            'type': 'string',
            'format': 'date-time',
        },
    }
}


class TimingInfo(APIObject):

    SCHEMA = TIMING_INFO_CONTRACT

    @classmethod
    def create(cls, name):
        return cls(name=name)

    def begin(self):
        self.set('started_at', timestring())

    def end(self):
        self.set('completed_at', timestring())


class collect_timing_info:
    def __init__(self, name):
        self.timing_info = TimingInfo.create(name)

    def __enter__(self):
        self.timing_info.begin()
        return self.timing_info

    def __exit__(self, exc_type, exc_value, traceback):
        self.timing_info.end()


class NodeSerializable(APIObject):
    def serialize(self):
        result = super().serialize()
        result['node'] = self.node.serialize()
        return result


PARTIAL_RESULT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'The partial result of a single node being run',
    'properties': {
        'error': {
            'type': ['string', 'null'],
            'description': 'The error string, or None if there was no error',
        },
        'status': {
            'type': ['string', 'null', 'number', 'boolean'],
            'description': 'The status result of the node execution',
        },
        'execution_time': {
            'type': 'number',
            'description': 'The execution time, in seconds',
        },
        'thread_id': {
            'type': ['string', 'null'],
            'description': 'ID of the executing thread, e.g. Thread-3',
        },
        'timing': {
            'type': 'array',
            'items': TIMING_INFO_CONTRACT,
        },
        'node': COMPILE_RESULT_NODE_CONTRACT,
    },
    'required': ['node', 'status', 'error', 'execution_time', 'thread_id',
                 'timing'],
}


class PartialResult(NodeSerializable):
    """Represent a "partial" execution result, i.e. one that has not (fully)
    been executed.

    This may be an ephemeral node (they are not compiled) or any error.
    """
    SCHEMA = PARTIAL_RESULT_CONTRACT

    def __init__(self, node, error=None, status=None, execution_time=0,
                 thread_id=None, timing=None):
        if timing is None:
            timing = []
        super().__init__(
            node=node,
            error=error,
            status=status,
            execution_time=execution_time,
            thread_id=thread_id,
            timing=timing,
        )

    # if the result got to the point where it could be skipped/failed, we would
    # be returning a real result, not a partial.
    @property
    def skipped(self):
        return False

    @property
    def failed(self):
        return None


RUN_MODEL_RESULT_CONTRACT = deep_merge(PARTIAL_RESULT_CONTRACT, {
    'description': 'The result of a single node being run',
    'properties': {
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
    },
    'required': ['skip', 'fail']
})


class RunModelResult(NodeSerializable):
    SCHEMA = RUN_MODEL_RESULT_CONTRACT

    def __init__(self, node, error=None, skip=False, status=None, failed=None,
                 thread_id=None, timing=None, execution_time=0):
        if timing is None:
            timing = []
        super().__init__(
            node=node,
            error=error,
            skip=skip,
            status=status,
            fail=failed,
            execution_time=execution_time,
            thread_id=thread_id,
            timing=timing,
        )

    # these all get set after the fact, generally
    error = named_property('error',
                           'If there was an error, the text of that error')
    skip = named_property('skip', 'True if the model was skipped')
    fail = named_property('fail', 'True if this was a test and it failed')
    status = named_property('status', 'The status of the model execution')
    execution_time = named_property('execution_time',
                                    'The time in seconds to execute the model')
    thread_id = named_property(
        'thread_id',
        'ID of the executing thread, e.g. Thread-3'
    )
    timing = named_property(
        'timing',
        'List of TimingInfo objects'
    )

    @property
    def failed(self):
        return self.fail

    @property
    def skipped(self):
        return self.skip


EXECUTION_RESULT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'The result of a single dbt invocation',
    'properties': {
        'results': {
            'type': 'array',
            'items': {
                'anyOf': [
                    RUN_MODEL_RESULT_CONTRACT,
                    PARTIAL_RESULT_CONTRACT,
                ]
            },
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


SOURCE_FRESHNESS_RESULT_CONTRACT = deep_merge(PARTIAL_RESULT_CONTRACT, {
    'properties': {
        'max_loaded_at': {
            'type': 'string',
            'format': 'date-time',
        },
        'snapshotted_at': {
            'type': 'string',
            'format': 'date-time',
        },
        'age': {
            'type': 'number',
        },
        'status': {
            'enum': ['pass', 'warn', 'error']
        },
        'node': PARSED_SOURCE_DEFINITION_CONTRACT,
    },
    'required': ['max_loaded_at', 'snapshotted_at', 'age']
})


class SourceFreshnessResult(NodeSerializable):
    SCHEMA = SOURCE_FRESHNESS_RESULT_CONTRACT

    def __init__(self, node, max_loaded_at, snapshotted_at,
                 age, status, thread_id, error=None,
                 timing=None, execution_time=0):
        max_loaded_at = max_loaded_at.isoformat()
        snapshotted_at = snapshotted_at.isoformat()
        if timing is None:
            timing = []
        super().__init__(
            node=node,
            max_loaded_at=max_loaded_at,
            snapshotted_at=snapshotted_at,
            age=age,
            status=status,
            thread_id=thread_id,
            error=error,
            timing=timing,
            execution_time=execution_time
        )

    @property
    def failed(self):
        return self.status == 'error'

    @property
    def skipped(self):
        return False


FRESHNESS_METADATA_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
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
                'The time elapsed from before_run to after_run (hooks '
                'are not included)'
            ),
        },
    },
    'required': ['generated_at', 'elapsed_time']
}


FRESHNESS_RESULTS_CONTRACT = deep_merge(FRESHNESS_METADATA_CONTRACT, {
    'description': 'The result of a single dbt source freshness invocation',
    'properties': {
        'results': {
            'type': 'array',
            'items': {
                'anyOf': [
                    PARTIAL_RESULT_CONTRACT,
                    SOURCE_FRESHNESS_RESULT_CONTRACT,
                ],
            },
        },
    },
    'required': ['results'],
})


class FreshnessExecutionResult(APIObject):
    SCHEMA = FRESHNESS_RESULTS_CONTRACT

    def __init__(self, elapsed_time, generated_at, results):
        super().__init__(
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            results=results
        )

    def serialize(self):
        return {
            'generated_at': self.generated_at,
            'elapsed_time': self.elapsed_time,
            'results': [s.serialize() for s in self.results]
        }

    def write(self, path):
        """Create a new object with the desired output schema and write it."""
        meta = {
            'generated_at': self.generated_at,
            'elapsed_time': self.elapsed_time,
        }
        sources = {}
        for result in self.results:
            unique_id = result.node.unique_id
            if result.error is not None:
                result_dict = {
                    'error': result.error,
                    'state': 'runtime error'
                }
            else:
                result_dict = {
                    'max_loaded_at': result.max_loaded_at,
                    'snapshotted_at': result.snapshotted_at,
                    'max_loaded_at_time_ago_in_s': result.age,
                    'state': result.status,
                    'criteria': result.node.freshness,
                }
            sources[unique_id] = result_dict
        output = FreshnessRunOutput(meta=meta, sources=sources)
        output.write(path)


def _copykeys(src, keys, **updates):
    return {k: getattr(src, k) for k in keys}


SOURCE_FRESHNESS_OUTPUT_ERROR_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'The source freshness output for a single source table',
    ),
    'properties': {
        'error': {
            'type': 'string',
            'description': 'The error string',
        },
        'state': {
            'enum': ['runtime error'],
        },
    }
}


SOURCE_FRESHNESS_OUTPUT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'The source freshness output for a single source table',
    ),
    'properties': {
        'max_loaded_at': {
            'type': 'string',
            'format': 'date-time',
        },
        'snapshotted_at': {
            'type': 'string',
            'format': 'date-time',
        },
        'max_loaded_at_time_ago_in_s': {
            'type': 'number',
        },
        'state': {
            'enum': ['pass', 'warn', 'error']
        },
        'criteria': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'warn_after': TIME_CONTRACT,
                'error_after': TIME_CONTRACT,
            },
        },
        'required': ['state', 'criteria', 'max_loaded_at', 'snapshotted_at',
                     'max_loaded_at_time_ago_in_s']
    }
}


FRESHNESS_RUN_OUTPUT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'The output contract for dbt source freshness invocations',
    'properties': {
        'meta': FRESHNESS_METADATA_CONTRACT,
        'sources': {
            'type': 'object',
            'additionalProperties': False,
            'description': (
                'A collection of the source results, stored by their unique '
                'IDs.'
            ),
            'patternProperties': {
                '.*': {
                    'anyOf': [
                        SOURCE_FRESHNESS_OUTPUT_ERROR_CONTRACT,
                        SOURCE_FRESHNESS_OUTPUT_CONTRACT
                    ],
                },
            },
        }
    }
}


class FreshnessRunOutput(APIObject):
    SCHEMA = FRESHNESS_RUN_OUTPUT_CONTRACT

    def __init__(self, meta, sources):
        super().__init__(meta=meta, sources=sources)


REMOTE_COMPILE_RESULT_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'raw_sql': {
            'type': 'string',
        },
        'compiled_sql': {
            'type': 'string',
        },
        'timing': {
            'type': 'array',
            'items': TIMING_INFO_CONTRACT,
        },
    },
    'required': ['raw_sql', 'compiled_sql', 'timing']
}


class RemoteCompileResult(APIObject):
    SCHEMA = REMOTE_COMPILE_RESULT_CONTRACT

    def __init__(self, raw_sql, compiled_sql, node, timing=None, **kwargs):
        if timing is None:
            timing = []
        # this should not show up in the serialized output.
        self.node = node
        super().__init__(
            raw_sql=raw_sql,
            compiled_sql=compiled_sql,
            timing=timing,
            **kwargs
        )

    @property
    def error(self):
        return None


REMOTE_RUN_RESULT_CONTRACT = deep_merge(REMOTE_COMPILE_RESULT_CONTRACT, {
    'properties': {
        'table': {
            'type': 'object',
            'properties': {
                'column_names': {
                    'type': 'array',
                    'items': {'type': 'string'},
                },
                'rows': {
                    'type': 'array',
                    # any item type is ok
                },
            },
            'required': ['rows', 'column_names'],
        },
    },
    'required': ['table'],
})


class RemoteRunResult(RemoteCompileResult):
    SCHEMA = REMOTE_RUN_RESULT_CONTRACT

    def __init__(self, raw_sql, compiled_sql, node, timing=None, table=None):
        if table is None:
            table = []
        super().__init__(
            raw_sql=raw_sql,
            compiled_sql=compiled_sql,
            timing=timing,
            table=table,
            node=node
        )
