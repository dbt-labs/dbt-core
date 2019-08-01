import unittest
from unittest import mock
from os.path import join as pjoin

from .utils import config_from_parts_or_dicts, normalize

from dbt.contracts.graph.manifest import FileHash, FilePath, SourceFile
from dbt.parser import ParseResult


class MatchingHash(FileHash):
    def __init__(self):
        return super().__init__('', '')

    def __eq__(self, other):
        return True


class TestCache(unittest.TestCase):
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
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project,
            profile=profile_data,
            cli_vars='{"test_schema_name": "foo"}'
        )

    def _matching_file(self, searched, name):
        path = FilePath(
            searched_path=normalize(searched),
            relative_path=normalize(name),
            absolute_path=normalize(pjoin(self.root_project_config.project_root, searched, name)),
        )
        return SourceFile(path=path, checksum=MatchingHash())

    def _new_results(self):
        return ParseResult(MatchingHash(), MatchingHash(), {})

    def _first_results(self):
        models = [
            ('model_1.sql', mock.MagicMock(unique_id='model.root.model_1')),
        ]
        snapshots = [
            ('snap_1.sql', [
                mock.MagicMock(unique_id='snapshot.root.snap_1'),
                mock.MagicMock(unique_id='snapshot.root.snap_2'),
            ])
        ]

        macros = [
            ('macros.sql', [
                mock.MagicMock(unique_id='macro.root.macro_1'),
                mock.MagicMock(unique_id='macro.root.macro_2'),
            ])
        ]
        return models, snapshots, macros

    def _second_results(self):
        models, snapshots, macros = self._first_results()
        models.append(
            ('model_2.sql', mock.MagicMock(unique_id='model.root.model_2'))
        )
        snapshots.append(
            ('more_snaps.sql', [
                mock.MagicMock(unique_id='snapshot.root.snap_3'),
                mock.MagicMock(unique_id='snapshot.root.snap_4'),
            ])
        )
        macros.append(
            ('more_macros.sql', [
                mock.MagicMock(unique_id='macro.root.macro_3'),
                mock.MagicMock(unique_id='macro.root.macro_4'),
            ])
        )
        return models, snapshots, macros

    def _populate_results(self, results, models=[], snapshots=[], macros=[]):
        for name, model in models:
            sf = self._matching_file('models', name)
            key = sf.path.search_key
            self.assertNotIn(key, results.files)
            sf.nodes = [model.unique_id]
            results.files[key] = sf
            self.assertNotIn(model.unique_id, results.nodes)
            results.nodes[model.unique_id] = model

        for name, snap_list in snapshots:
            sf = self._matching_file('snapshots', name)
            key = sf.path.search_key
            self.assertNotIn(key, results.files)
            sf.nodes = [s.unique_id for s in snap_list]
            results.files[key] = sf
            for snap in snap_list:
                self.assertNotIn(snap.unique_id, results.nodes)
                results.nodes[snap.unique_id] = snap

        for name, macro_list in macros:
            sf = self._matching_file('macros', name)
            key = sf.path.search_key
            self.assertNotIn(key, results.files)
            sf.macros = [m.unique_id for m in macro_list]
            results.files[key] = sf
            for macro in macro_list:
                self.assertNotIn(macro.unique_id, results.macros)
                results.macros[macro.unique_id] = macro

    def test_empty_cache_hit(self):
        new_result = self._new_results()
        full_result = self._new_results()
        models, snapshots, macros = self._first_results()
        self._populate_results(full_result, models, snapshots, macros)

        sf = self._matching_file('models', 'model_1.sql')
        self.assertNotIn(sf.path.search_key, new_result.files)
        self.assertEqual(new_result.files, {})
        found = new_result.sanitized_update(sf, full_result)
        self.assertTrue(found)
        self.assertIn(sf.path.search_key, new_result.files)
        self.assertEqual(
            new_result.files[sf.path.search_key].nodes,
            ['model.root.model_1']
        )
        self.assertEqual(new_result.files[sf.path.search_key].macros, [])
        self.assertEqual(new_result.files[sf.path.search_key].docs, [])
        self.assertEqual(new_result.files[sf.path.search_key].sources, [])
        self.assertEqual(new_result.files[sf.path.search_key].patches, [])

    def test_populated_cache_hit(self):
        partial_result = self._new_results()
        models, snapshots, macros = self._first_results()
        self._populate_results(partial_result, models, snapshots, macros)

        full_result = self._new_results()
        models, snapshots, macros = self._second_results()
        self._populate_results(full_result, models, snapshots, macros)

        of = self._matching_file('models', 'model_1.sql')

        sf = self._matching_file('models', 'model_2.sql')
        self.assertNotIn(sf.path.search_key, partial_result.files)
        self.assertIn(of.path.search_key, partial_result.files)
        self.assertIn(sf.path.search_key, full_result.files)
        found = partial_result.sanitized_update(sf, full_result)
        self.assertTrue(found)
        self.assertIn(sf.path.search_key, partial_result.files)
        self.assertEqual(
            partial_result.files[sf.path.search_key].nodes,
            ['model.root.model_2']
        )
        self.assertEqual(partial_result.files[sf.path.search_key].macros, [])
        self.assertEqual(partial_result.files[sf.path.search_key].docs, [])
        self.assertEqual(partial_result.files[sf.path.search_key].sources, [])
        self.assertEqual(partial_result.files[sf.path.search_key].patches, [])
