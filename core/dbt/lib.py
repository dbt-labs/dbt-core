import os
from dbt.config.project import Project
from dbt.contracts.results import RunningStatus, collect_timing_info
from dbt.events.functions import fire_event
from dbt.events.types import NodeCompiling, NodeExecuting
from dbt.exceptions import DbtRuntimeError
from dbt.task.sql import SqlCompileRunner
from dataclasses import dataclass
from dbt.cli.resolvers import default_profiles_dir
from dbt.config.runtime import load_profile, load_project
from dbt.flags import set_from_args


@dataclass
class RuntimeArgs:
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

        TODO: This is a temporary solution to more complex permissions requirements
        for the semantic layer, and thus largely duplicates the code in the parent class
        method. Once conditional credential usage is enabled, this should be removed.
        """
        result = None
        ctx.node.update_event_status(node_status=RunningStatus.Compiling)
        fire_event(
            NodeCompiling(
                node_info=ctx.node.node_info,
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
            ctx.node.update_event_status(node_status=RunningStatus.Executing)
            fire_event(
                NodeExecuting(
                    node_info=ctx.node.node_info,
                )
            )
            with collect_timing_info("execute") as timing_info:
                result = self.run(ctx.node, manifest)
                ctx.node = result.node

            ctx.timing.append(timing_info)

        return result


def load_profile_project(project_dir, profile_name_override=None):
    profile = load_profile(project_dir, {}, profile_name_override)
    project = load_project(project_dir, False, profile, {})
    return profile, project


def get_dbt_config(project_dir, args=None, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig
    import dbt.adapters.factory
    import dbt.events.functions

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = default_profiles_dir()

    profile_name = getattr(args, "profile", None)

    runtime_args = RuntimeArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        single_threaded=single_threaded,
        profile=profile_name,
        target=getattr(args, "target", None),
    )

    # set global flags from arguments
    set_from_args(runtime_args, None)
    profile, project = load_profile_project(project_dir, profile_name)
    assert type(project) is Project

    config = RuntimeConfig.from_parts(project, profile, runtime_args)

    # the only thing this set_from_args does differently than
    # the one above is that it pass runtime config over, I don't think
    # we need that. but leaving this for now for future reference

    # flags.set_from_args(runtime_args, config)

    # This is idempotent, so we can call it repeatedly
    dbt.adapters.factory.register_adapter(config)

    # Make sure we have a valid invocation_id
    dbt.events.functions.set_invocation_id()
    dbt.events.functions.reset_metadata_vars()

    return config


def get_task_by_type(type):
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

    raise DbtRuntimeError("not a valid task")


def create_task(type, args, manifest, config):
    task = get_task_by_type(type)

    def no_op(*args, **kwargs):
        pass

    task = task(args, config)
    task.load_manifest = no_op
    task.manifest = manifest
    return task


def _get_operation_node(manifest, project_path, sql, node_name):
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
    sql_node = block_parser.parse_remote(sql, node_name)
    process_node(config, manifest, sql_node)
    return config, sql_node, adapter


def compile_sql(manifest, project_path, sql, node_name="query"):
    config, node, adapter = _get_operation_node(manifest, project_path, sql, node_name)
    allow_introspection = str(os.environ.get("__DBT_ALLOW_INTROSPECTION", "1")).lower() in (
        "true",
        "1",
        "on",
    )

    if allow_introspection:
        runner = SqlCompileRunner(config, adapter, node, 1, 1)
    else:
        runner = SqlCompileRunnerNoIntrospection(config, adapter, node, 1, 1)
    return runner.safe_run(manifest)


def execute_sql(manifest, project_path, sql, node_name="query"):
    from dbt.task.sql import SqlExecuteRunner

    config, node, adapter = _get_operation_node(manifest, project_path, sql, node_name)

    runner = SqlExecuteRunner(config, adapter, node, 1, 1)

    return runner.safe_run(manifest)


def parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def deserialize_manifest(manifest_msgpack):
    from dbt.contracts.graph.manifest import Manifest

    return Manifest.from_msgpack(manifest_msgpack)


def serialize_manifest(manifest):
    return manifest.to_msgpack()
