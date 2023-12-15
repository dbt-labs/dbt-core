import builtins
from typing import List, Any, Optional
import os

from dbt.common.constants import SECRET_ENV_PREFIX
from dbt.common.dataclass_schema import ValidationError


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = str(msg)

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


class DbtBaseException(Exception):
    CODE = -32000
    MESSAGE = "Server Error"

    def data(self):
        # if overriding, make sure the result is json-serializable.
        return {
            "type": self.__class__.__name__,
            "message": str(self),
        }


class DbtInternalError(DbtBaseException):
    def __init__(self, msg: str):
        self.stack: List = []
        self.msg = scrub_secrets(msg, env_secrets())

    @property
    def type(self):
        return "Internal"

    def process_stack(self):
        lines = []
        stack = self.stack
        first = True

        if len(stack) > 1:
            lines.append("")

            for item in stack:
                msg = "called by"

                if first:
                    msg = "in"
                    first = False

                lines.append(f"> {msg}")

        return lines

    def __str__(self):
        if hasattr(self.msg, "split"):
            split_msg = self.msg.split("\n")
        else:
            split_msg = str(self.msg).split("\n")

        lines = ["{}".format(self.type + " Error")] + split_msg

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(["  " + line for line in lines[1:]])


class DbtRuntimeError(RuntimeError, DbtBaseException):
    CODE = 10001
    MESSAGE = "Runtime error"

    def __init__(self, msg: str, node=None) -> None:
        self.stack: List = []
        self.node = node
        self.msg = scrub_secrets(msg, env_secrets())

    def add_node(self, node=None):
        if node is not None and node is not self.node:
            if self.node is not None:
                self.stack.append(self.node)
            self.node = node

    @property
    def type(self):
        return "Runtime"

    def node_to_string(self, node: Any):
        """
        Given a node-like object we attempt to create the best identifier we can
        """
        result = ""
        if hasattr(node, "resource_type"):
            result += node.resource_type
        if hasattr(node, "name"):
            result += f" {node.name}"
        if hasattr(node, "original_file_path"):
            result += f" ({node.original_file_path})"

        return result.strip() if result != "" else "<Unknown>"

    def process_stack(self):
        lines = []
        stack = self.stack + [self.node]
        first = True

        if len(stack) > 1:
            lines.append("")

            for item in stack:
                msg = "called by"

                if first:
                    msg = "in"
                    first = False

                lines.append(f"> {msg} {self.node_to_string(item)}")

        return lines

    def validator_error_message(self, exc: builtins.Exception):
        """Given a dbt.dataclass_schema.ValidationError (which is basically a
        jsonschema.ValidationError), return the relevant parts as a string
        """
        if not isinstance(exc, ValidationError):
            return str(exc)
        path = "[%s]" % "][".join(map(repr, exc.relative_path))
        return f"at path {path}: {exc.message}"

    def __str__(self, prefix: str = "! "):
        node_string = ""

        if self.node is not None:
            node_string = f" in {self.node_to_string(self.node)}"

        if hasattr(self.msg, "split"):
            split_msg = self.msg.split("\n")
        else:
            split_msg = str(self.msg).split("\n")

        lines = ["{}{}".format(self.type + " Error", node_string)] + split_msg

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(["  " + line for line in lines[1:]])

    def data(self):
        result = DbtBaseException.data(self)
        if self.node is None:
            return result

        result.update(
            {
                "raw_code": self.node.raw_code,
                # the node isn't always compiled, but if it is, include that!
                "compiled_code": getattr(self.node, "compiled_code", None),
            }
        )
        return result


class CompilationError(DbtRuntimeError):
    CODE = 10004
    MESSAGE = "Compilation Error"

    @property
    def type(self):
        return "Compilation"

    def _fix_dupe_msg(self, path_1: str, path_2: str, name: str, type_name: str) -> str:
        if path_1 == path_2:
            return (
                f"remove one of the {type_name} entries for {name} in this file:\n - {path_1!s}\n"
            )
        else:
            return (
                f"remove the {type_name} entry for {name} in one of these files:\n"
                f" - {path_1!s}\n{path_2!s}"
            )


class RecursionError(DbtRuntimeError):
    pass


class DbtConfigError(DbtRuntimeError):
    CODE = 10007
    MESSAGE = "DBT Configuration Error"

    # ToDo: Can we remove project?
    def __init__(self, msg: str, project=None, result_type="invalid_project", path=None) -> None:
        self.project = project
        super().__init__(msg)
        self.result_type = result_type
        self.path = path

    def __str__(self, prefix="! ") -> str:
        msg = super().__str__(prefix)
        if self.path is None:
            return msg
        else:
            return f"{msg}\n\nError encountered in {self.path}"


class NotImplementedError(DbtBaseException):
    def __init__(self, msg: str) -> None:
        self.msg = msg
        self.formatted_msg = f"ERROR: {self.msg}"
        super().__init__(self.formatted_msg)


class SemverError(Exception):
    def __init__(self, msg: Optional[str] = None) -> None:
        self.msg = msg
        if msg is not None:
            super().__init__(msg)
        else:
            super().__init__()


class VersionsNotCompatibleError(SemverError):
    pass


class DbtValidationError(DbtRuntimeError):
    CODE = 10005
    MESSAGE = "Validation Error"


class DbtDatabaseError(DbtRuntimeError):
    CODE = 10003
    MESSAGE = "Database Error"

    def process_stack(self):
        lines = []

        if hasattr(self.node, "build_path") and self.node.build_path:
            lines.append(f"compiled Code at {self.node.build_path}")

        return lines + DbtRuntimeError.process_stack(self)

    @property
    def type(self):
        return "Database"


class UnexpectedNullError(DbtDatabaseError):
    def __init__(self, field_name: str, source):
        self.field_name = field_name
        self.source = source
        msg = (
            f"Expected a non-null value when querying field '{self.field_name}' of table "
            f" {self.source} but received value 'null' instead"
        )
        super().__init__(msg)


class CommandError(DbtRuntimeError):
    def __init__(self, cwd: str, cmd: List[str], msg: str = "Error running command") -> None:
        cmd_scrubbed = list(scrub_secrets(cmd_txt, env_secrets()) for cmd_txt in cmd)
        super().__init__(msg)
        self.cwd = cwd
        self.cmd = cmd_scrubbed
        self.args = (cwd, cmd_scrubbed, msg)

    def __str__(self):
        if len(self.cmd) == 0:
            return f"{self.msg}: No arguments given"
        return f'{self.msg}: "{self.cmd[0]}"'
