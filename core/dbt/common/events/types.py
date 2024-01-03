from dbt.common.events.base_types import (
    DebugLevel,
    InfoLevel,
    WarnLevel,
    ErrorLevel,
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
