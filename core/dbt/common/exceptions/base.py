import builtins
from typing import List, Any

from dbt.common.dataclass_schema import ValidationError
from dbt.common.utils.exceptions import scrub_secrets, env_secrets


class DbtInternalError(Exception):
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


class DbtRuntimeError(RuntimeError, Exception):
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
        result = Exception.data(self)
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
