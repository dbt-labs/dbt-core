from typing import Any

from dbt.common.exceptions import CompilationError, DbtBaseException


class MacroReturn(DbtBaseException):
    """
    Hack of all hacks
    This is not actually an exception.
    It's how we return a value from a macro.
    """

    def __init__(self, value) -> None:
        self.value = value


class UndefinedMacroError(CompilationError):
    def __str__(self, prefix: str = "! ") -> str:
        msg = super().__str__(prefix)
        return (
            f"{msg}. This can happen when calling a macro that does "
            "not exist. Check for typos and/or install package dependencies "
            'with "dbt deps".'
        )


class UndefinedCompilationError(CompilationError):
    def __init__(self, name: str, node) -> None:
        self.name = name
        self.node = node
        self.msg = f"{self.name} is undefined"
        super().__init__(msg=self.msg)


class CaughtMacroError(CompilationError):
    def __init__(self, exc) -> None:
        self.exc = exc
        super().__init__(msg=str(exc))


class CaughtMacroErrorWithNodeError(CompilationError):
    def __init__(self, exc, node) -> None:
        self.exc = exc
        self.node = node
        super().__init__(msg=str(exc))


class JinjaRenderingError(CompilationError):
    pass


class MaterializationArgError(CompilationError):
    def __init__(self, name: str, argument: str) -> None:
        self.name = name
        self.argument = argument
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f"materialization '{self.name}' received unknown argument '{self.argument}'."
        return msg


class MacroNameNotStringError(CompilationError):
    def __init__(self, kwarg_value) -> None:
        self.kwarg_value = kwarg_value
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f"The macro_name parameter ({self.kwarg_value}) "
            "to adapter.dispatch was not a string"
        )
        return msg


class MacrosSourcesUnWriteableError(CompilationError):
    def __init__(self, node) -> None:
        self.node = node
        msg = 'cannot "write" macros or sources'
        super().__init__(msg=msg)


class MacroArgTypeError(CompilationError):
    def __init__(self, method_name: str, arg_name: str, got_value: Any, expected_type) -> None:
        self.method_name = method_name
        self.arg_name = arg_name
        self.got_value = got_value
        self.expected_type = expected_type
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        got_type = type(self.got_value)
        msg = (
            f"'adapter.{self.method_name}' expects argument "
            f"'{self.arg_name}' to be of type '{self.expected_type}', instead got "
            f"{self.got_value} ({got_type})"
        )
        return msg


class MacroResultError(CompilationError):
    def __init__(self, freshness_macro_name: str, table):
        self.freshness_macro_name = freshness_macro_name
        self.table = table
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f'Got an invalid result from "{self.freshness_macro_name}" macro: {[tuple(r) for r in self.table]}'

        return msg
