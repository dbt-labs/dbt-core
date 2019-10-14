"""Unit test utility functions.

Note that all imports should be inside the functions to avoid import/mocking
issues.
"""
import os
from unittest import mock
from unittest import TestCase

from hologram import ValidationError


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


class Obj:
    which = 'blah'


def mock_connection(name):
    conn = mock.MagicMock()
    conn.name = name
    return conn


def config_from_parts_or_dicts(project, profile, packages=None, cli_vars='{}'):
    from dbt.config import Project, Profile, RuntimeConfig
    from dbt.utils import parse_cli_vars
    from copy import deepcopy
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)
    if not isinstance(project, Project):
        project = Project.from_project_config(deepcopy(project), packages)
    if not isinstance(profile, Profile):
        profile = Profile.from_raw_profile_info(deepcopy(profile),
                                                project.profile_name,
                                                cli_vars)
    args = Obj()
    args.vars = repr(cli_vars)
    args.profile_dir = '/dev/null'
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )


def inject_adapter(value):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    from dbt.adapters.factory import FACTORY
    key = value.type()
    FACTORY.adapters[key] = value
    FACTORY.adapter_types[key] = type(value)


class ContractTestCase(TestCase):
    ContractType = None

    def setUp(self):
        self.maxDiff = None
        super().setUp()

    def assert_to_dict(self, obj, dct):
        self.assertEqual(obj.to_dict(), dct)

    def assert_from_dict(self, obj, dct, cls=None):
        if cls is None:
            cls = self.ContractType
        self.assertEqual(cls.from_dict(dct),  obj)

    def assert_symmetric(self, obj, dct, cls=None):
        self.assert_to_dict(obj, dct)
        self.assert_from_dict(obj, dct, cls)

    def assert_fails_validation(self, dct, cls=None):
        if cls is None:
            cls = self.ContractType

        with self.assertRaises(ValidationError):
            cls.from_dict(dct)
