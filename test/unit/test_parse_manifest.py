import unittest
from unittest import mock
from unittest.mock import patch

from .utils import config_from_parts_or_dicts, normalize

from dbt.contracts.files import SourceFile, FileHash, FilePath
from dbt.contracts.graph.manifest import Manifest, ManifestStateCheck
from dbt.parser.search import FileBlock
from dbt.parser import manifest


class MatchingHash(FileHash):
    def __init__(self):
        return super().__init__('', '')

    def __eq__(self, other):
        return True


class MismatchedHash(FileHash):
    def __init__(self):
        return super().__init__('', '')

    def __eq__(self, other):
        return False


class TestLoader(unittest.TestCase):
    def setUp(self):
        profile_data = {
            'target': 'test',
            'quoting': {},
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'host': 'localhost',
                    'schema': 'analytics',
                    'user': 'test',
                    'pass': 'test',
                    'dbname': 'test',
                    'port': 1,
                }
            }
        }

        root_project = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': normalize('/usr/src/app'),
            'config-version': 2,
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project,
            profile=profile_data,
            cli_vars='{"test_schema_name": "foo"}'
        )
        self.parser = mock.MagicMock()

        # Create the Manifest.state_check patcher
        @patch('dbt.parser.manifest.ManifestLoader.build_manifest_state_check')
        def _mock_state_check(self):
            config = self.root_project
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents('vars'),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents('profile'),
            )
        self.load_state_check = patch('dbt.parser.manifest.ManifestLoader.build_manifest_state_check')
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        self.loader = manifest.ManifestLoader(
            self.root_project_config,
            {'root': self.root_project_config}
        )

    def _new_manifest(self):
        state_check = ManifestStateCheck(MatchingHash(), MatchingHash, [])
        manifest = Manifest({}, {}, {}, {}, {}, {}, [], {})
        manifest.state_check = state_check
        return manifest

    def _mismatched_file(self, searched, name):
        return self._new_file(searched, name, False)

    def _matching_file(self, searched, name):
        return self._new_file(searched, name, True)

    def _new_file(self, searched, name, match):
        if match:
            checksum = MatchingHash()
        else:
            checksum = MismatchedHash()
        path = FilePath(
            searched_path=normalize(searched),
            relative_path=normalize(name),
            project_root=normalize(self.root_project_config.project_root),
        )
        return SourceFile(path=path, checksum=checksum)

    def test_model_no_cache(self):
        source_file = self._matching_file('models', 'model_1.sql')
        self.parser.load_file.return_value = source_file

        self.loader.old_manifest = None
        self.loader.parse_with_cache(source_file.path, self.parser)
        # there was nothing in the cache, so parse_file should get called
        # with a FileBlock that has the given source file in it
        self.parser.parse_file.assert_called_once_with(FileBlock(file=source_file))

    def test_model_cache_hit(self):
        source_file = self._matching_file('models', 'model_1.sql')
        self.parser.load_file.return_value = source_file

        source_file_dupe = self._matching_file('models', 'model_1.sql')
        source_file_dupe.nodes.append('model.root.model_1')

        old_manifest = self._new_manifest()
        old_manifest.files[source_file_dupe.path.search_key] = source_file_dupe
        self.loader.old_manifest = old_manifest
        self.loader.old_manifest.nodes = {'model.root.model_1': mock.MagicMock()}

        self.loader.parse_with_cache(source_file.path, self.parser)
        # there was a cache hit, so parse_file should never have been called
        self.parser.parse_file.assert_not_called()

    def test_model_cache_mismatch_checksum(self):
        source_file = self._mismatched_file('models', 'model_1.sql')
        self.parser.load_file.return_value = source_file

        source_file_dupe = self._mismatched_file('models', 'model_1.sql')
        source_file_dupe.nodes.append('model.root.model_1')

        old_manifest = self._new_manifest()
        old_manifest.files[source_file_dupe.path.search_key] = source_file_dupe
        old_manifest.nodes = {'model.root.model_1': mock.MagicMock()}
        self.loader.old_manifest = old_manifest

        self.loader.parse_with_cache(source_file.path, self.parser)
        # there was a cache checksum mismatch, so parse_file should get called
        # with a FileBlock that has the given source file in it
        self.parser.parse_file.assert_called_once_with(FileBlock(file=source_file))

    def test_model_cache_missing_file(self):
        source_file = self._matching_file('models', 'model_1.sql')
        self.parser.load_file.return_value = source_file

        source_file_different = self._matching_file('models', 'model_2.sql')
        source_file_different.nodes.append('model.root.model_2')

        old_manifest = self._new_manifest()
        old_manifest.files[source_file_different.path.search_key] = source_file_different
        old_manifest.nodes = {'model.root.model_2': mock.MagicMock()}

        self.loader.old_manifest = old_manifest
        self.loader.parse_with_cache(source_file.path, self.parser)
        # the filename wasn't in the cache, so parse_file should get called
        # with a  FileBlock that has the given source file in it.
        self.parser.parse_file.assert_called_once_with(FileBlock(file=source_file))
