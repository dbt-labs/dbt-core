from dbt.config.project import Project


class AdapterPlugin:
    """Defines the basic requirements for a dbt adapter plugin.

    :param type adapter: An adapter class, derived from BaseAdapter
    :param type credentials: A credentials object, derived from Credentials
    :param str project_name: The name of this adapter plugin's associated dbt
        project.
    :param str include_path: The path to this adapter plugin's root
    :param Optional[List[str]] dependencies: A list of adapter names that this
        adapter depends upon.
    """
    def __init__(self, adapter, credentials, include_path, dependencies=None):
        self.adapter = adapter
        self.credentials = credentials
        self.include_path = include_path
        project = Project.from_project_root(include_path, {})
        self.project_name = project.project_name
        if dependencies is None:
            dependencies = []
        self.dependencies = dependencies
