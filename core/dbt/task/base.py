import os
import threading
import time
import traceback
from abc import ABCMeta, abstractmethod
from typing import Type, Union, Dict, Any, Optional
from datetime import datetime

from dbt import tracking
from dbt import flags
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import (
    NodeStatus,
    RunResult,
    collect_timing_info,
    RunStatus,
    RunningStatus,
)
from dbt.exceptions import (
    NotImplementedError,
    CompilationError,
    DbtRuntimeError,
    DbtInternalError,
)
from dbt.logger import log_manager
from dbt.events.functions import fire_event
from dbt.events.types import (
    LogDbtProjectError,
    LogDbtProfileError,
    CatchableExceptionOnRun,
    InternalErrorOnRun,
    GenericExceptionOnRun,
    NodeConnectionReleaseError,
    LogDebugStackTrace,
    SkippingDetails,
    LogSkipBecauseError,
    NodeCompiling,
    NodeExecuting,
)
from dbt.events.contextvars import get_node_info
from .printer import print_run_result_error

from dbt.adapters.factory import register_adapter
from dbt.config import RuntimeConfig, Project
from dbt.config.profile import read_profile
import dbt.exceptions


class NoneConfig:
    @classmethod
    def from_args(cls, args):
        return None


def read_profiles(profiles_dir=None):
    """This is only used for some error handling"""
    if profiles_dir is None:
        profiles_dir = flags.PROFILES_DIR

    raw_profiles = read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != "config"}

    return profiles


class BaseTask(metaclass=ABCMeta):
    ConfigType: Union[Type[NoneConfig], Type[Project]] = NoneConfig

    def __init__(self, args, config):
        self.args = args
        self.args.single_threaded = False
        self.config = config

    @classmethod
    def pre_init_hook(cls, args):
        """A hook called before the task is initialized."""
        if args.log_format == "json":
            log_manager.format_json()
        else:
            log_manager.format_text()

    @classmethod
    def set_log_format(cls):
        if flags.LOG_FORMAT == "json":
            log_manager.format_json()
        else:
            log_manager.format_text()

    @classmethod
    def from_args(cls, args):
        try:
            # This is usually RuntimeConfig but will be UnsetProfileConfig
            # for the clean or deps tasks
            config = cls.ConfigType.from_args(args)
        except dbt.exceptions.DbtProjectError as exc:
            fire_event(LogDbtProjectError(exc=str(exc)))

            tracking.track_invalid_invocation(args=args, result_type=exc.result_type)
            raise dbt.exceptions.DbtRuntimeError("Could not run dbt") from exc
        except dbt.exceptions.DbtProfileError as exc:
            all_profile_names = list(read_profiles(flags.PROFILES_DIR).keys())
            fire_event(LogDbtProfileError(exc=str(exc), profiles=all_profile_names))
            tracking.track_invalid_invocation(args=args, result_type=exc.result_type)
            raise dbt.exceptions.DbtRuntimeError("Could not run dbt") from exc
        return cls(args, config)

    @abstractmethod
    def run(self):
        raise dbt.exceptions.NotImplementedError("Not Implemented")

    def interpret_results(self, results):
        return True


def get_nearest_project_dir(args):
    # If the user provides an explicit project directory, use that
    # but don't look at parent directories.
    if args.project_dir:
        project_file = os.path.join(args.project_dir, "dbt_project.yml")
        if os.path.exists(project_file):
            return args.project_dir
        else:
            raise dbt.exceptions.DbtRuntimeError(
                "fatal: Invalid --project-dir flag. Not a dbt project. "
                "Missing dbt_project.yml file"
            )

    root_path = os.path.abspath(os.sep)
    cwd = os.getcwd()

    while cwd != root_path:
        project_file = os.path.join(cwd, "dbt_project.yml")
        if os.path.exists(project_file):
            return cwd
        cwd = os.path.dirname(cwd)

    raise dbt.exceptions.DbtRuntimeError(
        "fatal: Not a dbt project (or any of the parent directories). "
        "Missing dbt_project.yml file"
    )


def move_to_nearest_project_dir(args):
    nearest_project_dir = get_nearest_project_dir(args)
    os.chdir(nearest_project_dir)
    return nearest_project_dir


class ConfiguredTask(BaseTask):
    ConfigType = RuntimeConfig

    def __init__(self, args, config):
        super().__init__(args, config)
        register_adapter(self.config)

    @classmethod
    def from_args(cls, args):
        move_to_nearest_project_dir(args)
        return super().from_args(args)


class ExecutionContext:
    """During execution and error handling, dbt makes use of mutable state:
    timing information and the newest (compiled vs executed) form of the node.
    """

    def __init__(self, node):
        self.timing = []
        self.node = node


class BaseRunner(metaclass=ABCMeta):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        self.config = config
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False
        self.skip_cause: Optional[RunResult] = None

    @abstractmethod
    def compile(self, manifest: Manifest) -> Any:
        pass

    def get_result_status(self, result) -> Dict[str, str]:
        if result.status == NodeStatus.Error:
            return {"node_status": "error", "node_error": str(result.message)}
        elif result.status == NodeStatus.Skipped:
            return {"node_status": "skipped"}
        elif result.status == NodeStatus.Fail:
            return {"node_status": "failed"}
        elif result.status == NodeStatus.Warn:
            return {"node_status": "warn"}
        else:
            return {"node_status": "passed"}

    def run_with_hooks(self, manifest):
        if self.skip:
            return self.on_skip()

        # no before/after printing for ephemeral mdoels
        if not self.node.is_ephemeral_model:
            self.before_execute()

        result = self.safe_run(manifest)
        self.node.update_event_status(
            node_status=result.status, finished_at=datetime.utcnow().isoformat()
        )

        if not self.node.is_ephemeral_model:
            self.after_execute(result)

        return result

    def _build_run_result(
        self,
        node,
        start_time,
        status,
        timing_info,
        message,
        agate_table=None,
        adapter_response=None,
        failures=None,
    ):
        execution_time = time.time() - start_time
        thread_id = threading.current_thread().name
        if adapter_response is None:
            adapter_response = {}
        return RunResult(
            status=status,
            thread_id=thread_id,
            execution_time=execution_time,
            timing=timing_info,
            message=message,
            node=node,
            agate_table=agate_table,
            adapter_response=adapter_response,
            failures=failures,
        )

    def error_result(self, node, message, start_time, timing_info):
        return self._build_run_result(
            node=node,
            start_time=start_time,
            status=RunStatus.Error,
            timing_info=timing_info,
            message=message,
        )

    def ephemeral_result(self, node, start_time, timing_info):
        return self._build_run_result(
            node=node,
            start_time=start_time,
            status=RunStatus.Success,
            timing_info=timing_info,
            message=None,
        )

    def from_run_result(self, result, start_time, timing_info):
        return self._build_run_result(
            node=result.node,
            start_time=start_time,
            status=result.status,
            timing_info=timing_info,
            message=result.message,
            agate_table=result.agate_table,
            adapter_response=result.adapter_response,
            failures=result.failures,
        )

    def skip_result(self, node, message):
        thread_id = threading.current_thread().name
        return RunResult(
            status=RunStatus.Skipped,
            thread_id=thread_id,
            execution_time=0,
            timing=[],
            message=message,
            node=node,
            adapter_response={},
            failures=None,
        )

    def compile_and_execute(self, manifest, ctx):
        result = None
        with self.adapter.connection_for(self.node):
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

    def _handle_catchable_exception(self, e, ctx):
        if e.node is None:
            e.add_node(ctx.node)

        fire_event(
            CatchableExceptionOnRun(
                exc=str(e), exc_info=traceback.format_exc(), node_info=get_node_info()
            )
        )
        return str(e)

    def _handle_internal_exception(self, e, ctx):
        fire_event(InternalErrorOnRun(build_path=self.node.build_path, exc=str(e)))
        return str(e)

    def _handle_generic_exception(self, e, ctx):
        fire_event(
            GenericExceptionOnRun(
                build_path=self.node.build_path,
                unique_id=self.node.unique_id,
                exc=str(e),
            )
        )
        fire_event(LogDebugStackTrace(exc_info=traceback.format_exc()))

        return str(e)

    def handle_exception(self, e, ctx):
        catchable_errors = (CompilationError, DbtRuntimeError)
        if isinstance(e, catchable_errors):
            error = self._handle_catchable_exception(e, ctx)
        elif isinstance(e, DbtInternalError):
            error = self._handle_internal_exception(e, ctx)
        else:
            error = self._handle_generic_exception(e, ctx)
        return error

    def safe_run(self, manifest):
        started = time.time()
        ctx = ExecutionContext(self.node)
        error = None
        result = None

        try:
            result = self.compile_and_execute(manifest, ctx)
        except Exception as e:
            error = self.handle_exception(e, ctx)
        finally:
            exc_str = self._safe_release_connection()

            # if releasing failed and the result doesn't have an error yet, set
            # an error
            if (
                exc_str is not None
                and result is not None
                and result.status != NodeStatus.Error
                and error is None
            ):
                error = exc_str

        if error is not None:
            # we could include compile time for runtime errors here
            result = self.error_result(ctx.node, error, started, [])
        elif result is not None:
            result = self.from_run_result(result, started, ctx.timing)
        else:
            result = self.ephemeral_result(ctx.node, started, ctx.timing)
        return result

    def _safe_release_connection(self):
        """Try to release a connection. If an exception is hit, log and return
        the error string.
        """
        try:
            self.adapter.release_connection()
        except Exception as exc:
            fire_event(
                NodeConnectionReleaseError(
                    node_name=self.node.name, exc=str(exc), exc_info=traceback.format_exc()
                )
            )
            return str(exc)

        return None

    def before_execute(self):
        raise NotImplementedError()

    def execute(self, compiled_node, manifest):
        raise NotImplementedError()

    def run(self, compiled_node, manifest):
        return self.execute(compiled_node, manifest)

    def after_execute(self, result):
        raise NotImplementedError()

    def _skip_caused_by_ephemeral_failure(self):
        if self.skip_cause is None or self.skip_cause.node is None:
            return False
        return self.skip_cause.node.is_ephemeral_model

    def on_skip(self):
        schema_name = self.node.schema
        node_name = self.node.name

        error_message = None
        if not self.node.is_ephemeral_model:
            # if this model was skipped due to an upstream ephemeral model
            # failure, print a special 'error skip' message.
            if self._skip_caused_by_ephemeral_failure():
                fire_event(
                    LogSkipBecauseError(
                        schema=schema_name,
                        relation=node_name,
                        index=self.node_index,
                        total=self.num_nodes,
                    )
                )
                print_run_result_error(result=self.skip_cause, newline=False)
                if self.skip_cause is None:  # mypy appeasement
                    raise DbtInternalError(
                        "Skip cause not set but skip was somehow caused by an ephemeral failure"
                    )
                # set an error so dbt will exit with an error code
                error_message = (
                    "Compilation Error in {}, caused by compilation error "
                    "in referenced ephemeral model {}".format(
                        self.node.unique_id, self.skip_cause.node.unique_id
                    )
                )
            else:
                # 'skipped' nodes should not have a value for 'node_finished_at'
                # they do have 'node_started_at', which is set in GraphRunnableTask.call_runner
                self.node.update_event_status(node_status=RunStatus.Skipped)
                fire_event(
                    SkippingDetails(
                        resource_type=self.node.resource_type,
                        schema=schema_name,
                        node_name=node_name,
                        index=self.node_index,
                        total=self.num_nodes,
                        node_info=self.node.node_info,
                    )
                )

        node_result = self.skip_result(self.node, error_message)
        return node_result

    def do_skip(self, cause=None):
        self.skip = True
        self.skip_cause = cause
