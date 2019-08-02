from typing import Union, List, Dict, Any

from dbt.node_runners import TestRunner
from dbt.node_types import NodeType
from dbt.task.run import RunTask
from dbt.task.runnable import RemoteCallable


class TestTask(RunTask):
    """
    Testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """
    def raise_on_first_error(self):
        return False

    def safe_run_hooks(self, adapter, hook_type, extra_context):
        # Don't execute on-run-* hooks for tests
        pass

    def build_query(self):
        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.Test
        }
        test_types = [self.args.data, self.args.schema]

        if all(test_types) or not any(test_types):
            tags = []
        elif self.args.data:
            tags = ['data']
        elif self.args.schema:
            tags = ['schema']
        else:
            raise RuntimeError("unexpected")

        query['tags'] = tags
        return query

    def get_runner_type(self):
        return TestRunner


class RemoteTestProjectTask(TestTask, RemoteCallable):
    METHOD_NAME = 'test_project'

    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest.deepcopy(config=config)

    def load_manifest(self):
        # we started out with a manifest!
        pass

    def handle_request(
        self,
        models: Union[None, str, List[str]] = None,
        exclude: Union[None, str, List[str]] = None,
        data: bool = False,
        schema: bool = False,
    ) -> Dict[str, List[Any]]:
        self.args.models = self._listify(models)
        self.args.exclude = self._listify(exclude)
        self.args.data = data
        self.args.schema = schema

        results = self.run()
        return {'results': [r.to_dict() for r in results]}
