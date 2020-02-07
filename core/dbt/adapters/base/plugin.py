from typing import List, Optional, Type

from dbt.adapters.base import BaseAdapter, Credentials
from dbt.exceptions import CompilationException


class AdapterPlugin:
    """Defines the basic requirements for a dbt adapter plugin.

    :param include_path: The path to this adapter plugin's root
    :param dependencies: A list of adapter names that this adapter depends
        upon.
    """
    def __init__(
        self,
        adapter: Type[BaseAdapter],
        credentials: Type[Credentials],
        include_path: str,
        dependencies: Optional[List[str]] = None
    ):
        # avoid an import cycle
        from dbt.config.project import Project

        self.adapter: Type[BaseAdapter] = adapter
        self.credentials: Type[Credentials] = credentials
        self.include_path: str = include_path
        partial = Project.partial_load(include_path)
        if partial.project_name is None:
            raise CompilationException(
                f'Invalid project at {include_path}: name not set!'
            )
        self.project_name: str = partial.project_name
        self.dependencies: List[str]
        if dependencies is None:
            self.dependencies = []
        else:
            self.dependencies = dependencies
