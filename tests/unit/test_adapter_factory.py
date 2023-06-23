from pathlib import Path
import unittest
from unittest import mock
from dbt.adapters.factory import AdapterContainer
from dbt.adapters.base.plugin import AdapterPlugin
from dbt.include.global_project import (
    PROJECT_NAME as GLOBAL_PROJECT_NAME,
)


class TestGetPackageNames(unittest.TestCase):
    def setUp(self):
        with mock.patch("dbt.adapters.base.plugin.project_name_from_path") as get_name:
            get_name.return_value = "root"
            self.root_plugin = AdapterPlugin(
                adapter=mock.MagicMock(),
                credentials=mock.MagicMock(),
                include_path="/path/to/root/plugin",
                dependencies=["childa", "childb"],
            )
            get_name.return_value = "pkg_childa"
            self.childa = AdapterPlugin(
                adapter=mock.MagicMock(),
                credentials=mock.MagicMock(),
                include_path="/path/to/childa",
            )
            get_name.return_value = "pkg_childb"
            self.childb = AdapterPlugin(
                adapter=mock.MagicMock(),
                credentials=mock.MagicMock(),
                include_path="/path/to/childb",
                dependencies=["childc"],
            )
            get_name.return_value = "pkg_childc"
            self.childc = AdapterPlugin(
                adapter=mock.MagicMock(),
                credentials=mock.MagicMock(),
                include_path="/path/to/childc",
            )

        self._mock_modules = {
            "root": self.root_plugin,
            "childa": self.childa,
            "childb": self.childb,
            "childc": self.childc,
        }

        self.factory = AdapterContainer()

        self.load_patch = mock.patch.object(AdapterContainer, "load_plugin")
        self.mock_load = self.load_patch.start()

        def mock_load_plugin(name: str):
            try:
                plugin = self._mock_modules[name]
            except KeyError:
                raise RuntimeError(f"test could not find adapter type {name}!")
            self.factory.plugins[name] = plugin
            self.factory.packages[plugin.project_name] = Path(plugin.include_path)
            for dep in plugin.dependencies:
                self.factory.load_plugin(dep)

        self.mock_load.side_effect = mock_load_plugin

    def tearDown(self):
        self.load_patch.stop()

    def test_no_packages(self):
        assert self.factory.get_adapter_package_names(None) == [GLOBAL_PROJECT_NAME]

    def test_one_package(self):
        self.factory.load_plugin("childc")
        assert self.factory.get_adapter_package_names("childc") == [
            "pkg_childc",
            GLOBAL_PROJECT_NAME,
        ]

    def test_simple_child_packages(self):
        self.factory.load_plugin("childb")
        assert self.factory.get_adapter_package_names("childb") == [
            "pkg_childb",
            "pkg_childc",
            GLOBAL_PROJECT_NAME,
        ]

    def test_layered_child_packages(self):
        self.factory.load_plugin("root")
        assert self.factory.get_adapter_package_names("root") == [
            "root",
            "pkg_childa",
            "pkg_childb",
            "pkg_childc",
            GLOBAL_PROJECT_NAME,
        ]
