import json

from dbt.common.events.base_types import (
    DynamicLevel,
    DebugLevel,
    InfoLevel,
    WarnLevel,
    ErrorLevel,
    EventLevel,
)
from dbt.common.events.format import (
    format_fancy_output_line,
    pluralize,
    timestamp_to_datetime_string,
)

# from dbt.node_types import NodeType
from dbt.common.ui import red, green, yellow


# The classes in this file represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# Event codes have prefixes which follow this table
#
# | Code |     Description     |
# |:----:|:-------------------:|
# | A    | Pre-project loading |
# | D    | Deprecations        |
# | E    | DB adapter          |
# | I    | Project parsing     |
# | M    | Deps generation     |
# | P    | Artifacts           |
# | Q    | Node execution      |
# | W    | Node testing        |
# | Z    | Misc                |
# | T    | Test only           |
#
# The basic idea is that event codes roughly translate to the natural order of running a dbt task

# =======================================================
# M - Deps generation
# =======================================================


class RetryExternalCall(DebugLevel):
    def code(self) -> str:
        return "M020"

    def message(self) -> str:
        return f"Retrying external call. Attempt: {self.attempt} Max attempts: {self.max}"


class RecordRetryException(DebugLevel):
    def code(self) -> str:
        return "M021"

    def message(self) -> str:
        return f"External call exception: {self.exc}"


# =======================================================
# Q - Node execution
# =======================================================


class RunningOperationCaughtError(ErrorLevel):
    def code(self) -> str:
        return "Q001"

    def message(self) -> str:
        return f"Encountered an error while running operation: {self.exc}"


class CompileComplete(InfoLevel):
    def code(self) -> str:
        return "Q002"

    def message(self) -> str:
        return "Done."


class FreshnessCheckComplete(InfoLevel):
    def code(self) -> str:
        return "Q003"

    def message(self) -> str:
        return "Done."


class SeedHeader(InfoLevel):
    def code(self) -> str:
        return "Q004"

    def message(self) -> str:
        return self.header


class SQLRunnerException(DebugLevel):
    def code(self) -> str:
        return "Q006"

    def message(self) -> str:
        return f"Got an exception: {self.exc}"


class LogTestResult(DynamicLevel):
    def code(self) -> str:
        return "Q007"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR"
            status = red(
                info,
            )
        elif self.status == "pass":
            info = "PASS"
            status = green(info)
        elif self.status == "warn":
            info = f"WARN {self.num_failures}"
            status = yellow(info)
        else:  # self.status == "fail":
            info = f"FAIL {self.num_failures}"
            status = red(info)
        msg = f"{info} {self.name}"

        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.num_models,
            execution_time=self.execution_time,
        )

    @classmethod
    def status_to_level(cls, status):
        # The statuses come from TestStatus
        level_lookup = {
            "fail": EventLevel.ERROR,
            "pass": EventLevel.INFO,
            "warn": EventLevel.WARN,
            "error": EventLevel.ERROR,
        }
        if status in level_lookup:
            return level_lookup[status]
        else:
            return EventLevel.INFO


# Skipped Q008, Q009, Q010


class LogStartLine(InfoLevel):
    def code(self) -> str:
        return "Q011"

    def message(self) -> str:
        msg = f"START {self.description}"
        return format_fancy_output_line(msg=msg, status="RUN", index=self.index, total=self.total)


class LogModelResult(DynamicLevel):
    def code(self) -> str:
        return "Q012"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR creating"
            status = red(self.status.upper())
        else:
            info = "OK created"
            status = green(self.status)

        msg = f"{info} {self.description}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


# Skipped Q013, Q014


class LogSnapshotResult(DynamicLevel):
    def code(self) -> str:
        return "Q015"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR snapshotting"
            status = red(self.status.upper())
        else:
            info = "OK snapshotted"
            status = green(self.result_message)

        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


class LogSeedResult(DynamicLevel):
    def code(self) -> str:
        return "Q016"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR loading"
            status = red(self.status.upper())
        else:
            info = "OK loaded"
            status = green(self.result_message)
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


# Skipped Q017


class LogFreshnessResult(DynamicLevel):
    def code(self) -> str:
        return "Q018"

    def message(self) -> str:
        if self.status == "runtime error":
            info = "ERROR"
            status = red(info)
        elif self.status == "error":
            info = "ERROR STALE"
            status = red(info)
        elif self.status == "warn":
            info = "WARN"
            status = yellow(info)
        else:
            info = "PASS"
            status = green(info)
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )

    @classmethod
    def status_to_level(cls, status):
        # The statuses come from FreshnessStatus
        # TODO should this return EventLevel enum instead?
        level_lookup = {
            "runtime error": EventLevel.ERROR,
            "pass": EventLevel.INFO,
            "warn": EventLevel.WARN,
            "error": EventLevel.ERROR,
        }
        if status in level_lookup:
            return level_lookup[status]
        else:
            return EventLevel.INFO


# Skipped Q019, Q020, Q021


class LogCancelLine(ErrorLevel):
    def code(self) -> str:
        return "Q022"

    def message(self) -> str:
        msg = f"CANCEL query {self.conn_name}"
        return format_fancy_output_line(msg=msg, status=red("CANCEL"), index=None, total=None)


class DefaultSelector(InfoLevel):
    def code(self) -> str:
        return "Q023"

    def message(self) -> str:
        return f"Using default selector {self.name}"


class NodeStart(DebugLevel):
    def code(self) -> str:
        return "Q024"

    def message(self) -> str:
        return f"Began running node {self.node_info.unique_id}"


class NodeFinished(DebugLevel):
    def code(self) -> str:
        return "Q025"

    def message(self) -> str:
        return f"Finished running node {self.node_info.unique_id}"


class QueryCancelationUnsupported(InfoLevel):
    def code(self) -> str:
        return "Q026"

    def message(self) -> str:
        msg = (
            f"The {self.type} adapter does not support query "
            "cancellation. Some queries may still be "
            "running!"
        )
        return yellow(msg)


class ConcurrencyLine(InfoLevel):
    def code(self) -> str:
        return "Q027"

    def message(self) -> str:
        return f"Concurrency: {self.num_threads} threads (target='{self.target_name}')"


class WritingInjectedSQLForNode(DebugLevel):
    def code(self) -> str:
        return "Q029"

    def message(self) -> str:
        return f'Writing injected SQL for node "{self.node_info.unique_id}"'


class NodeCompiling(DebugLevel):
    def code(self) -> str:
        return "Q030"

    def message(self) -> str:
        return f"Began compiling node {self.node_info.unique_id}"


class NodeExecuting(DebugLevel):
    def code(self) -> str:
        return "Q031"

    def message(self) -> str:
        return f"Began executing node {self.node_info.unique_id}"


class LogHookStartLine(InfoLevel):
    def code(self) -> str:
        return "Q032"

    def message(self) -> str:
        msg = f"START hook: {self.statement}"
        return format_fancy_output_line(
            msg=msg, status="RUN", index=self.index, total=self.total, truncate=True
        )


class LogHookEndLine(InfoLevel):
    def code(self) -> str:
        return "Q033"

    def message(self) -> str:
        msg = f"OK hook: {self.statement}"
        return format_fancy_output_line(
            msg=msg,
            status=green(self.status),
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
            truncate=True,
        )


class SkippingDetails(InfoLevel):
    def code(self) -> str:
        return "Q034"

    def message(self) -> str:
        # ToDo: move to core or figure out NodeType
        if self.resource_type in ["model", "seed", "snapshot"]:
            msg = f"SKIP relation {self.schema}.{self.node_name}"
        else:
            msg = f"SKIP {self.resource_type} {self.node_name}"
        return format_fancy_output_line(
            msg=msg, status=yellow("SKIP"), index=self.index, total=self.total
        )


class NothingToDo(WarnLevel):
    def code(self) -> str:
        return "Q035"

    def message(self) -> str:
        return "Nothing to do. Try checking your model configs and model specification args"


class RunningOperationUncaughtError(ErrorLevel):
    def code(self) -> str:
        return "Q036"

    def message(self) -> str:
        return f"Encountered an error while running operation: {self.exc}"


class EndRunResult(DebugLevel):
    def code(self) -> str:
        return "Q037"

    def message(self) -> str:
        return "Command end result"


class NoNodesSelected(WarnLevel):
    def code(self) -> str:
        return "Q038"

    def message(self) -> str:
        return "No nodes selected!"


class CommandCompleted(DebugLevel):
    def code(self) -> str:
        return "Q039"

    def message(self) -> str:
        status = "succeeded" if self.success else "failed"
        completed_at = timestamp_to_datetime_string(self.completed_at)
        return f"Command `{self.command}` {status} at {completed_at} after {self.elapsed:0.2f} seconds"


class ShowNode(InfoLevel):
    def code(self) -> str:
        return "Q041"

    def message(self) -> str:
        if self.output_format == "json":
            if self.is_inline:
                return json.dumps({"show": json.loads(self.preview)}, indent=2)
            else:
                return json.dumps(
                    {"node": self.node_name, "show": json.loads(self.preview)}, indent=2
                )
        else:
            if self.is_inline:
                return f"Previewing inline node:\n{self.preview}"
            else:
                return f"Previewing node '{self.node_name}':\n{self.preview}"


class CompiledNode(InfoLevel):
    def code(self) -> str:
        return "Q042"

    def message(self) -> str:
        if self.output_format == "json":
            if self.is_inline:
                return json.dumps({"compiled": self.compiled}, indent=2)
            else:
                return json.dumps({"node": self.node_name, "compiled": self.compiled}, indent=2)
        else:
            if self.is_inline:
                return f"Compiled inline node is:\n{self.compiled}"
            else:
                return f"Compiled node '{self.node_name}' is:\n{self.compiled}"


# =======================================================
# W - Node testing
# =======================================================

# Skipped W001


class CatchableExceptionOnRun(DebugLevel):
    def code(self) -> str:
        return "W002"

    def message(self) -> str:
        return str(self.exc)


class InternalErrorOnRun(DebugLevel):
    def code(self) -> str:
        return "W003"

    def message(self) -> str:
        prefix = f"Internal error executing {self.build_path}"

        internal_error_string = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/dbt-labs/dbt-core
""".strip()

        return f"{red(prefix)}\n" f"{str(self.exc).strip()}\n\n" f"{internal_error_string}"


class GenericExceptionOnRun(ErrorLevel):
    def code(self) -> str:
        return "W004"

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = f"Unhandled error while executing {node_description}"
        return f"{red(prefix)}\n{str(self.exc).strip()}"


class NodeConnectionReleaseError(DebugLevel):
    def code(self) -> str:
        return "W005"

    def message(self) -> str:
        return f"Error releasing connection for node {self.node_name}: {str(self.exc)}"


class FoundStats(InfoLevel):
    def code(self) -> str:
        return "W006"

    def message(self) -> str:
        return f"Found {self.stat_line}"


# =======================================================
# Z - Misc
# =======================================================


class MainKeyboardInterrupt(InfoLevel):
    def code(self) -> str:
        return "Z001"

    def message(self) -> str:
        return "ctrl-c"


class MainEncounteredError(ErrorLevel):
    def code(self) -> str:
        return "Z002"

    def message(self) -> str:
        return f"Encountered an error:\n{self.exc}"


class MainStackTrace(ErrorLevel):
    def code(self) -> str:
        return "Z003"

    def message(self) -> str:
        return self.stack_trace


# Skipped Z004


class SystemCouldNotWrite(DebugLevel):
    def code(self) -> str:
        return "Z005"

    def message(self) -> str:
        return (
            f"Could not write to path {self.path}({len(self.path)} characters): "
            f"{self.reason}\nexception: {self.exc}"
        )


class SystemExecutingCmd(DebugLevel):
    def code(self) -> str:
        return "Z006"

    def message(self) -> str:
        return f'Executing "{" ".join(self.cmd)}"'


class SystemStdOut(DebugLevel):
    def code(self) -> str:
        return "Z007"

    def message(self) -> str:
        return f'STDOUT: "{str(self.bmsg)}"'


class SystemStdErr(DebugLevel):
    def code(self) -> str:
        return "Z008"

    def message(self) -> str:
        return f'STDERR: "{str(self.bmsg)}"'


class SystemReportReturnCode(DebugLevel):
    def code(self) -> str:
        return "Z009"

    def message(self) -> str:
        return f"command return code={self.returncode}"


class TimingInfoCollected(DebugLevel):
    def code(self) -> str:
        return "Z010"

    def message(self) -> str:
        started_at = timestamp_to_datetime_string(self.timing_info.started_at)
        completed_at = timestamp_to_datetime_string(self.timing_info.completed_at)
        return f"Timing info for {self.node_info.unique_id} ({self.timing_info.name}): {started_at} => {completed_at}"


# This prints the stack trace at the debug level while allowing just the nice exception message
# at the error level - or whatever other level chosen.  Used in multiple places.


class LogDebugStackTrace(DebugLevel):
    def code(self) -> str:
        return "Z011"

    def message(self) -> str:
        return f"{self.exc_info}"


# We don't write "clean" events to the log, because the clean command
# may have removed the log directory.


class CheckCleanPath(InfoLevel):
    def code(self) -> str:
        return "Z012"

    def message(self) -> str:
        return f"Checking {self.path}/*"


class ConfirmCleanPath(InfoLevel):
    def code(self) -> str:
        return "Z013"

    def message(self) -> str:
        return f"Cleaned {self.path}/*"


class ProtectedCleanPath(InfoLevel):
    def code(self) -> str:
        return "Z014"

    def message(self) -> str:
        return f"ERROR: not cleaning {self.path}/* because it is protected"


class FinishedCleanPaths(InfoLevel):
    def code(self) -> str:
        return "Z015"

    def message(self) -> str:
        return "Finished cleaning all paths."


class OpenCommand(InfoLevel):
    def code(self) -> str:
        return "Z016"

    def message(self) -> str:
        msg = f"""To view your profiles.yml file, run:

{self.open_cmd} {self.profiles_dir}"""

        return msg


# We use events to create console output, but also think of them as a sequence of important and
# meaningful occurrences to be used for debugging and monitoring. The Formatting event helps eases
# the tension between these two goals by allowing empty lines, heading separators, and other
# formatting to be written to the console, while they can be ignored for other purposes. For
# general information that isn't simple formatting, the Note event should be used instead.


class Formatting(InfoLevel):
    def code(self) -> str:
        return "Z017"

    def message(self) -> str:
        return self.msg


class RunResultWarning(WarnLevel):
    def code(self) -> str:
        return "Z021"

    def message(self) -> str:
        info = "Warning"
        return yellow(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


class RunResultFailure(ErrorLevel):
    def code(self) -> str:
        return "Z022"

    def message(self) -> str:
        info = "Failure"
        return red(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


class StatsLine(InfoLevel):
    def code(self) -> str:
        return "Z023"

    def message(self) -> str:
        stats_line = "Done. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}"
        return stats_line.format(**self.stats)


class RunResultError(ErrorLevel):
    def code(self) -> str:
        return "Z024"

    def message(self) -> str:
        # This is the message on the result object, cannot be built here
        return f"  {self.msg}"


class RunResultErrorNoMessage(ErrorLevel):
    def code(self) -> str:
        return "Z025"

    def message(self) -> str:
        return f"  Status: {self.status}"


class SQLCompiledPath(InfoLevel):
    def code(self) -> str:
        return "Z026"

    def message(self) -> str:
        return f"  compiled Code at {self.path}"


class CheckNodeTestFailure(InfoLevel):
    def code(self) -> str:
        return "Z027"

    def message(self) -> str:
        msg = f"select * from {self.relation_name}"
        border = "-" * len(msg)
        return f"  See test failures:\n  {border}\n  {msg}\n  {border}"


# Skipped Z028, Z029


class EndOfRunSummary(InfoLevel):
    def code(self) -> str:
        return "Z030"

    def message(self) -> str:
        error_plural = pluralize(self.num_errors, "error")
        warn_plural = pluralize(self.num_warnings, "warning")
        if self.keyboard_interrupt:
            message = yellow("Exited because of keyboard interrupt")
        elif self.num_errors > 0:
            message = red(f"Completed with {error_plural} and {warn_plural}:")
        elif self.num_warnings > 0:
            message = yellow(f"Completed with {warn_plural}:")
        else:
            message = green("Completed successfully")
        return message


# Skipped Z031, Z032, Z033


class LogSkipBecauseError(ErrorLevel):
    def code(self) -> str:
        return "Z034"

    def message(self) -> str:
        msg = f"SKIP relation {self.schema}.{self.relation} due to ephemeral model error"
        return format_fancy_output_line(
            msg=msg, status=red("ERROR SKIP"), index=self.index, total=self.total
        )


# Skipped Z035


class EnsureGitInstalled(ErrorLevel):
    def code(self) -> str:
        return "Z036"

    def message(self) -> str:
        return (
            "Make sure git is installed on your machine. More "
            "information: "
            "https://docs.getdbt.com/docs/package-management"
        )


class DepsCreatingLocalSymlink(DebugLevel):
    def code(self) -> str:
        return "Z037"

    def message(self) -> str:
        return "Creating symlink to local dependency."


class DepsSymlinkNotAvailable(DebugLevel):
    def code(self) -> str:
        return "Z038"

    def message(self) -> str:
        return "Symlinks are not available on this OS, copying dependency."


class DisableTracking(DebugLevel):
    def code(self) -> str:
        return "Z039"

    def message(self) -> str:
        return (
            "Error sending anonymous usage statistics. Disabling tracking for this execution. "
            "If you wish to permanently disable tracking, see: "
            "https://docs.getdbt.com/reference/global-configs#send-anonymous-usage-stats."
        )


class SendingEvent(DebugLevel):
    def code(self) -> str:
        return "Z040"

    def message(self) -> str:
        return f"Sending event: {self.kwargs}"


class SendEventFailure(DebugLevel):
    def code(self) -> str:
        return "Z041"

    def message(self) -> str:
        return "An error was encountered while trying to send an event"


class FlushEvents(DebugLevel):
    def code(self) -> str:
        return "Z042"

    def message(self) -> str:
        return "Flushing usage events"


class FlushEventsFailure(DebugLevel):
    def code(self) -> str:
        return "Z043"

    def message(self) -> str:
        return "An error was encountered while trying to flush usage events"


class TrackingInitializeFailure(DebugLevel):
    def code(self) -> str:
        return "Z044"

    def message(self) -> str:
        return "Got an exception trying to initialize tracking"


# this is the message from the result object


class RunResultWarningMessage(WarnLevel):
    def code(self) -> str:
        return "Z046"

    def message(self) -> str:
        # This is the message on the result object, cannot be formatted in event
        return self.msg


class DebugCmdOut(InfoLevel):
    def code(self) -> str:
        return "Z047"

    def message(self) -> str:
        return self.msg


class DebugCmdResult(InfoLevel):
    def code(self) -> str:
        return "Z048"

    def message(self) -> str:
        return self.msg


class ListCmdOut(InfoLevel):
    def code(self) -> str:
        return "Z049"

    def message(self) -> str:
        return self.msg


class Note(InfoLevel):
    """The Note event provides a way to log messages which aren't likely to be
    useful as more structured events. For console formatting text like empty
    lines and separator bars, use the Formatting event instead."""

    def code(self) -> str:
        return "Z050"

    def message(self) -> str:
        return self.msg


class ResourceReport(DebugLevel):
    def code(self) -> str:
        return "Z051"

    def message(self) -> str:
        return f"Resource report: {self.to_json()}"
