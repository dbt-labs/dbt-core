from typing import Optional

from dbt_common.exceptions import DbtRuntimeError


class IncompatibleSchemaError(DbtRuntimeError):
    def __init__(self, expected: str, found: Optional[str] = None) -> None:
        self.expected = expected
        self.found = found
        self.filename = "input file"

        super().__init__(msg=self.get_message())

    def add_filename(self, filename: str):
        self.filename = filename
        self.msg = self.get_message()

    def get_message(self) -> str:
        found_str = "nothing"
        if self.found is not None:
            found_str = f'"{self.found}"'

        msg = (
            f'Expected a schema version of "{self.expected}" in '
            f"{self.filename}, but found {found_str}. Are you running with a "
            f"different version of dbt?"
        )
        return msg

    CODE = 10014
    MESSAGE = "Incompatible Schema"
