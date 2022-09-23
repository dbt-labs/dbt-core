# TODO: this file is one big TODO
# from dataclasses import dataclass
from dataclasses import dataclass
import os
from dbt.contracts.results import RunningStatus, collect_timing_info
from dbt.events.functions import fire_event
from dbt.events.types import NodeCompiling, NodeExecuting
from dbt.exceptions import RuntimeException
from dbt import flags
from dbt.task.sql import SqlCompileRunner


@dataclass
class RuntimeArgs():
    project_dir: str
    profiles_dir: str
    single_threaded: bool
    profile: str
    target: str


class SqlCompileRunnerNoIntrospection(SqlCompileRunner):
    def compile_and_execute(self, manifest, ctx):
        """
        This version of this method does not connect to the data warehouse.
        As a result, introspective queries at compilation will not be supported
        and will throw an error.
        """
        allow_introspection = str(os.environ.get(
            "__DBT_ALLOW_INTROSPECTION", "1"
        )).lower() in ("true", "1", "on")

        if allow_introspection:
            return super().compile_and_execute(manifest, ctx)

        result = None
        # with self.adapter.connection_for(self.node):
        ctx.node._event_status["node_status"] = RunningStatus.Compiling
        fire_event(
            NodeCompiling(
                node_info=ctx.node.node_info,
                unique_id=ctx.node.unique_id,
            )
        )
        with collect_timing_info("compile") as timing_info:
            # if we fail here, we still have a compiled node to return
            # this has the benefit of showing a build path for the errant
            # model
            ctx.node = self.compile(manifest)
        ctx.timing.append(timing_info)

        # for ephemeral nodes, we only want to compile, not run
        if not ctx.node.is_ephemeral_model:
            ctx.node._event_status["node_status"] = RunningStatus.Executing
            fire_event(
                NodeExecuting(
                    node_info=ctx.node.node_info,
                    unique_id=ctx.node.unique_id,
                )
            )
            with collect_timing_info("execute") as timing_info:
                result = self.run(ctx.node, manifest)
                ctx.node = result.node

            ctx.timing.append(timing_info)

        return result


def get_dbt_config(project_dir, args=None, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig
    import dbt.adapters.factory
    import dbt.events.functions

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    profile = args.profile if hasattr(args, "profile") else None
    target = args.target if hasattr(args, "target") else None

    # Construct a phony config
    config = RuntimeConfig.from_args(
        RuntimeArgs(project_dir, profiles_dir, single_threaded, profile, target)
    )
    # Clear previously registered adapters--
    # this fixes cacheing behavior on the dbt-server
    flags.set_from_args(args, config)
    dbt.adapters.factory.reset_adapters()
    # Load the relevant adapter
    dbt.adapters.factory.register_adapter(config)
    # Set invocation id
    dbt.events.functions.set_invocation_id()

    return config


def get_task_by_type(type):
    # TODO: we need to tell dbt-server what tasks are available
    from dbt.task.run import RunTask
    from dbt.task.list import ListTask
    from dbt.task.seed import SeedTask
    from dbt.task.test import TestTask
    from dbt.task.build import BuildTask
    from dbt.task.snapshot import SnapshotTask
    from dbt.task.run_operation import RunOperationTask

    if type == "run":
        return RunTask
    elif type == "test":
        return TestTask
    elif type == "list":
        return ListTask
    elif type == "seed":
        return SeedTask
    elif type == "build":
        return BuildTask
    elif type == "snapshot":
        return SnapshotTask
    elif type == "run_operation":
        return RunOperationTask

    raise RuntimeException("not a valid task")


def create_task(type, args, manifest, config):
    task = get_task_by_type(type)

    def no_op(*args, **kwargs):
        pass

    task = task(args, config)
    task.load_manifest = no_op
    task.manifest = manifest
    return task


def _get_operation_node(manifest, project_path, sql):
    from dbt.parser.manifest import process_node
    from dbt.parser.sql import SqlBlockParser
    import dbt.adapters.factory

    config = get_dbt_config(project_path)
    block_parser = SqlBlockParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    adapter = dbt.adapters.factory.get_adapter(config)
    # TODO : This needs a real name?
    sql_node = block_parser.parse_remote(sql, "name")
    process_node(config, manifest, sql_node)
    return config, sql_node, adapter


def compile_sql(manifest, project_path, sql):
    # from dbt.task.sql import SqlCompileRunner

    config, node, adapter = _get_operation_node(manifest, project_path, sql)
    runner = SqlCompileRunnerNoIntrospection(config, adapter, node, 1, 1)
    return runner.safe_run(manifest)


def execute_sql(manifest, project_path, sql):
    from dbt.task.sql import SqlExecuteRunner

    config, node, adapter = _get_operation_node(manifest, project_path, sql)
    runner = SqlExecuteRunner(config, adapter, node, 1, 1)
    # TODO: use same interface for runner
    return runner.safe_run(manifest)


def parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def deserialize_manifest(manifest_msgpack):
    from dbt.contracts.graph.manifest import Manifest

    return Manifest.from_msgpack(manifest_msgpack)


def serialize_manifest(manifest):
    # TODO: what should this take as an arg?
    return manifest.to_msgpack()
