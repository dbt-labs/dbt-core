from typing import List, Optional, Type

from dbt.adapters.base import Credentials
from dbt.exceptions import CompilationException
from dbt.adapters.protocol import AdapterProtocol


def project_name_from_path(include_path: str) -> str:
    # avoid an import cycle
    from dbt.config.project import Project

    partial = Project.partial_load(include_path)
    if partial.project_name is None:
        raise CompilationException(f"Invalid project at {include_path}: name not set!")
    return partial.project_name


class AdapterPlugin:
    """Defines the basic requirements for a dbt adapter plugin.

    :param include_path: The path to this adapter plugin's root
    :param dependencies: A list of adapter names that this adapter depends
        upon.
    """

    def __init__(
        self,
        adapter: Type[AdapterProtocol],
        credentials: Type[Credentials],
        include_path: str,
        dependencies: Optional[List[str]] = None,
    ):

        self.adapter: Type[AdapterProtocol] = adapter
        self.credentials: Type[Credentials] = credentials
        self.include_path: str = include_path
        self.project_name: str = project_name_from_path(include_path)
        self.dependencies: List[str]
        if dependencies is None:
            self.dependencies = []
        else:
            self.dependencies = dependencies
