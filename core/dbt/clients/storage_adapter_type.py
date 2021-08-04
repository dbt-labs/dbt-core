from typing import Any, Dict, Union
from typing_extensions import Literal


class StorageAdapterType:
    """Class that defines type hinting for storage adapters.

    This is needed because importlib (used in the storage client) and mypy aren't exactly friends.
    N.B: https://stackoverflow.com/questions/48976499/mypy-importlib-module-functions

    """

    @staticmethod
    def ready_check() -> bool:
        pass

    @staticmethod
    def read(path: str, strip: bool = ...) -> str:
        pass

    @staticmethod
    def write(
        path: str, content: Union[str, Dict[str, Any], None], overwrite: bool = ...
    ) -> bool:
        pass

    @staticmethod
    def delete(path: str) -> bool:
        pass

    @staticmethod
    def find() -> None:
        pass

    @staticmethod
    def info(path: str) -> Union[dict, Literal[False]]:
        pass
