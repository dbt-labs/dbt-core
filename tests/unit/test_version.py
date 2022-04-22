import dbt.version
from dbt.ui import green, red, yellow


class TestGetVersionInformation:
    def test_all_versions_equal(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_core_ahead(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.1",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.1",
                f"  - latest:    1.0.0 - {yellow('Ahead of latest version!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_core_behind(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.1",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.1 - {yellow('Update available!')}",
                "",
                "  Your version of dbt-core is out of date!",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_core_no_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                "",
                "  The latest version of dbt-core could not be determined!",
                "  Make sure that the following URL is accessible:",
                "  https://pypi.org/pypi/dbt-core/json",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugins_none(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugins_multiple(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
                "bazqux": ("1.0.0", "1.0.0"),
                "quuxcorge": ("1.0.0", "1.0.0"),
                "graultgarply": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar:       1.0.0 - {green('Up to date!')}",
                f"  - bazqux:       1.0.0 - {green('Up to date!')}",
                f"  - quuxcorge:    1.0.0 - {green('Up to date!')}",
                f"  - graultgarply: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_match_core_match_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_match_core_no_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", None),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {yellow('Could not determine latest version')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_match_core_behind_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "2.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {yellow('Update available!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_match_core_ahead_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.0.0",
            latest="1.0.0",
            plugins={
                "foobar": ("1.0.0", "2.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {yellow('Update available!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_major_match_latest(self, mocker):
        mock_versions(
            mocker,
            installed="2.0.0",
            latest="2.0.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 2.0.0",
                f"  - latest:    2.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_major_no_latest(self, mocker):
        mock_versions(
            mocker,
            installed="2.0.0",
            latest="2.0.0",
            plugins={
                "foobar": ("1.0.0", None),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 2.0.0",
                f"  - latest:    2.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_major_ahead_latest(self, mocker):
        mock_versions(
            mocker,
            installed="2.0.0",
            latest="2.0.0",
            plugins={
                "foobar": ("1.0.0", "0.0.1"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 2.0.0",
                f"  - latest:    2.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_major_behind_latest(self, mocker):
        mock_versions(
            mocker,
            installed="2.0.0",
            latest="2.0.0",
            plugins={
                "foobar": ("1.0.0", "1.1.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 2.0.0",
                f"  - latest:    2.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_minor_match_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.1.0",
            latest="1.1.0",
            plugins={
                "foobar": ("1.0.0", "1.0.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.1.0",
                f"  - latest:    1.1.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_minor_no_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.1.0",
            latest="1.1.0",
            plugins={
                "foobar": ("1.0.0", None),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.1.0",
                f"  - latest:    1.1.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_minor_ahead_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.1.0",
            latest="1.1.0",
            plugins={
                "foobar": ("1.0.0", "0.0.1"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.1.0",
                f"  - latest:    1.1.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugin_diff_core_minor_behind_latest(self, mocker):
        mock_versions(
            mocker,
            installed="1.1.0",
            latest="1.1.0",
            plugins={
                "foobar": ("1.0.0", "1.0.1"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.1.0",
                f"  - latest:    1.1.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {red('Not compatible!')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugins_various(self, mocker):
        mock_versions(
            mocker,
            installed="2.1.0",
            latest="2.1.0",
            plugins={
                "foobar": ("2.1.0", "2.1.0"),
                "bazqux": ("2.1.0", None),
                "quuux": ("2.1.0", "2.1.0"),
                "corge": ("22.21.20", "22.21.21"),
                "grault": ("2.1.0", "2.1.1"),
                "garply": ("2.1.0-b1", None),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 2.1.0",
                f"  - latest:    2.1.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 2.1.0    - {green('Up to date!')}",
                f"  - bazqux: 2.1.0    - {yellow('Could not determine latest version')}",
                f"  - quuux:  2.1.0    - {green('Up to date!')}",
                f"  - corge:  22.21.20 - {red('Not compatible!')}",
                f"  - grault: 2.1.0    - {yellow('Update available!')}",
                f"  - garply: 2.1.0-b1 - {yellow('Could not determine latest version')}",
                "",
                "  At least one plugin is out of date or incompatible with dbt-core.",
                "  You can find instructions for upgrading here:",
                "  https://docs.getdbt.com/docs/installation",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_plugins_alignment(self, mocker):
        mock_versions(
            mocker,
            installed="1.1.1-b123",
            latest="1.1.1-b123",
            plugins={
                "foobar": ("1.1.0-b1", "1.1.0-b1"),
                "bazqux": ("1.1.1-b123", "1.1.1-b123"),
                "quuux": ("1.1.0", "1.1.0"),
            },
        )

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.1.1-b123",
                f"  - latest:    1.1.1-b123 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.1.0-b1   - {green('Up to date!')}",
                f"  - bazqux: 1.1.1-b123 - {green('Up to date!')}",
                f"  - quuux:  1.1.0      - {green('Up to date!')}",
                "",
                "",
            ]
        )

        assert expected == actual

    def test_file_globbing(self, mocker):
        mocker.patch("dbt.version.__version__", "1.0.0")
        mock_latest_versions(mocker, core_latest="1.0.0")

        mocked_spec = mocker.Mock()
        mocker.patch("importlib.util.find_spec").return_value = mocked_spec

        paths = [
            "/some/sort/of/path",
            "/another/path",
            "/yet/another/type/of/path",
        ]
        mocked_spec.submodule_search_locations = paths

        plugins = [
            "foobar",
            "bazqux",
            "quuux",
            "corge",
            "grault",
            "garply",
        ]

        version_paths = [
            [
                f"/some/sort/of/path/{plugins[0]}/__version__.py",
                f"/some/sort/of/path/{plugins[1]}/__version__.py",
            ],
            [
                f"/another/path/{plugins[2]}/__version__.py",
                f"/another/path/{plugins[3]}/__version__.py",
            ],
            [
                f"/yet/another/type/of/path/{plugins[4]}/__version__.py",
                f"/yet/another/type/of/path/{plugins[5]}/__version__.py",
            ],
        ]

        def mock_glob(*args, **kwargs):
            version_glob = args[0]
            for i, path in enumerate(paths):
                if path in version_glob:
                    return version_paths[i]

        mocker.patch("glob.glob").side_effect = mock_glob

        def mock_import(*args, **kwargs):
            mock_module = mocker.Mock()
            mock_module.version = "1.0.0"

            import_path = args[0]

            for plugin in plugins:
                if plugin in import_path:
                    return mock_module

            raise ImportError

        mocker.patch("importlib.import_module").side_effect = mock_import

        actual = dbt.version.get_version_information()
        expected = "\n".join(
            [
                "Core:",
                "  - installed: 1.0.0",
                f"  - latest:    1.0.0 - {green('Up to date!')}",
                "",
                "Plugins:",
                f"  - foobar: 1.0.0 - {yellow('Could not determine latest version')}",
                f"  - bazqux: 1.0.0 - {yellow('Could not determine latest version')}",
                f"  - quuux:  1.0.0 - {yellow('Could not determine latest version')}",
                f"  - corge:  1.0.0 - {yellow('Could not determine latest version')}",
                f"  - grault: 1.0.0 - {yellow('Could not determine latest version')}",
                f"  - garply: 1.0.0 - {yellow('Could not determine latest version')}",
                "",
                "",
            ]
        )

        assert expected == actual


def mock_versions(mocker, installed="1.0.0", latest=None, plugins={}):
    mocker.patch("dbt.version.__version__", installed)
    mock_plugins(mocker, plugins)
    mock_latest_versions(mocker, latest, plugins)


def mock_plugins(mocker, plugins):
    mock_find_spec = mocker.patch("importlib.util.find_spec")
    path = "/tmp/dbt/adapters"
    mock_find_spec.return_value.submodule_search_locations = [path]

    mocker.patch("glob.glob").return_value = [f"{path}/{name}/__version__.py" for name in plugins]

    def mock_import(*args, **kwargs):
        import_path = args[0]

        for plugin_name in plugins:
            if plugin_name in import_path:
                plugin_version = plugins.get(plugin_name)[0]

                module_mock = mocker.Mock()
                module_mock.version = plugin_version

                return module_mock

        raise ImportError

    mocker.patch("importlib.import_module").side_effect = mock_import


def mock_latest_versions(mocker, core_latest=None, plugins={}):
    def mock_get(*args, **kwargs):
        mocked_response = mocker.Mock()
        mocked_response.json.return_value = {"bad object": None}

        version_url = args[0]

        if version_url is dbt.version.PYPI_VERSION_URL:
            if core_latest:
                mocked_response.json.return_value = {"info": {"version": core_latest}}

            return mocked_response

        for plugin_name in plugins:
            if plugin_name in version_url:
                plugin_latest = plugins.get(plugin_name)[1]

                if plugin_latest:
                    mocked_response.json.return_value = {"info": {"version": plugin_latest}}

                return mocked_response

        return mocked_response

    mocker.patch("requests.get").side_effect = mock_get
