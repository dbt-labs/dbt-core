# TODO: this file is one big TODO
import os
from dbt.exceptions import RuntimeException
from dbt import flags
from collections import namedtuple

RuntimeArgs = namedtuple("RuntimeArgs", "project_dir profiles_dir single_threaded profile target")


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

    # TODO: yuck, let's rethink tasks a little
    task = task(args, config)

    # Wow! We can monkeypatch taskCls.load_manifest to return _our_ manifest
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
    from dbt.task.sql import SqlCompileRunner

    config, node, adapter = _get_operation_node(manifest, project_path, sql)
    runner = SqlCompileRunner(config, adapter, node, 1, 1)
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
