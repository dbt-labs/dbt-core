from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import yaml

from dbt.task.test import TestTask
from dbt.task.list import ListTask


class TestSelectionExpansion(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_selection_expansion_066"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "test-paths": ["tests"]
        }

    def list_tests_and_assert(self, include, exclude, expected_tests, indirect_selection='eager', selector_name=None):
        list_args = [ 'ls', '--resource-type', 'test']
        if include:
            list_args.extend(('--select', include))
        if exclude:
            list_args.extend(('--exclude', exclude))
        if indirect_selection:
            list_args.extend(('--indirect-selection', indirect_selection))
        if selector_name:
            list_args.extend(('--selector', selector_name))

        listed = self.run_dbt(list_args)
        assert len(listed) == len(expected_tests)

        test_names = [name.split('.')[-1] for name in listed]
        assert sorted(test_names) == sorted(expected_tests)

    def run_tests_and_assert(self, include, exclude, expected_tests, indirect_selection='eager', selector_name=None):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 2)

        test_args = ['test']
        if include:
            test_args.extend(('--models', include))
        if exclude:
            test_args.extend(('--exclude', exclude))
        if indirect_selection:
            test_args.extend(('--indirect-selection', indirect_selection))
        if selector_name:
            test_args.extend(('--selector', selector_name))

        results = self.run_dbt(test_args)
        tests_run = [r.node.name for r in results]
        assert len(tests_run) == len(expected_tests)

        assert sorted(tests_run) == sorted(expected_tests)

    @use_profile('postgres')
    def test__postgres__all_tests_no_specifiers(self):
        select = None
        exclude = None
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'source_unique_my_src_my_tbl_fun',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_alone(self):
        select = 'model_a'
        exclude = None
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_model_b(self):
        select = 'model_a model_b'
        exclude = None
        expected = [
            'cf_a_b','cf_a_src','just_a','unique_model_a_fun',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_'
        ]


        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_sources(self):
        select = 'model_a source:*'
        exclude = None
        expected = [
            'cf_a_b','cf_a_src','just_a','unique_model_a_fun',
            'source_unique_my_src_my_tbl_fun',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__exclude_model_b(self):
        select = None
        exclude = 'model_b'
        expected = [
            'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'source_unique_my_src_my_tbl_fun','unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_exclude_specific_test(self):
        select = 'model_a'
        exclude = 'unique_model_a_fun'
        expected = [
            'cf_a_b','cf_a_src','just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_exclude_specific_test_cautious(self):
        select = 'model_a'
        exclude = 'unique_model_a_fun'
        expected = ['just_a']
        indirect_selection = 'cautious'

        self.list_tests_and_assert(select, exclude, expected, indirect_selection)
        self.run_tests_and_assert(select, exclude, expected, indirect_selection)

    @use_profile('postgres')
    def test__postgres__only_generic(self):
        select = 'test_type:generic'
        exclude = None
        expected = [
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'source_unique_my_src_my_tbl_fun',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_only_singular_unset(self):
        select = 'model_a,test_type:singular'
        exclude = None
        expected = ['cf_a_b','cf_a_src','just_a']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_only_singular_eager(self):
        select = 'model_a,test_type:singular'
        exclude = None
        expected = ['cf_a_b','cf_a_src','just_a']
        indirect_selection = 'eager'

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_only_singular_cautious(self):
        select = 'model_a,test_type:singular'
        exclude = None
        expected = ['just_a']
        indirect_selection = 'cautious'

        self.list_tests_and_assert(select, exclude, expected, indirect_selection=indirect_selection)
        self.run_tests_and_assert(select, exclude, expected, indirect_selection=indirect_selection)

    @use_profile('postgres')
    def test__postgres__only_singular(self):
        select = 'test_type:singular'
        exclude = None
        expected = ['cf_a_b', 'cf_a_src', 'just_a']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_only_singular(self):
        select = 'model_a,test_type:singular'
        exclude = None
        expected = ['cf_a_b','cf_a_src','just_a']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__test_name_intersection(self):
        select = 'model_a,test_name:unique'
        exclude = None
        expected = ['unique_model_a_fun']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_tag_test_name_intersection(self):
        select = 'tag:a_or_b,test_name:relationships'
        exclude = None
        expected = [
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__select_column_level_tag(self):
        select = 'tag:column_level_tag'
        exclude = None
        expected = [
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__exclude_column_level_tag(self):
        select = None
        exclude = 'tag:column_level_tag'
        expected = ['cf_a_b','cf_a_src','just_a','source_unique_my_src_my_tbl_fun']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__test_level_tag(self):
        select = 'tag:test_level_tag'
        exclude = None
        expected = ['relationships_model_a_fun__fun__ref_model_b_']

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__exclude_data_test_tag(self):
        select = 'model_a'
        exclude = 'tag:data_test_tag'
        expected = [
            'cf_a_b', 'cf_a_src',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_indirect_selection(self):
        select = 'model_a'
        exclude = None
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(select, exclude, expected)

    @use_profile('postgres')
    def test__postgres__model_a_indirect_selection_eager(self):
        select = 'model_a'
        exclude = None
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]
        indirect_selection = 'eager'

        self.list_tests_and_assert(select, exclude, expected, indirect_selection)
        self.run_tests_and_assert(select, exclude, expected, indirect_selection)

    @use_profile('postgres')
    def test__postgres__model_a_indirect_selection_exclude_unique_tests(self):
        select = 'model_a'
        exclude = 'test_name:unique'
        indirect_selection = 'eager'
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
        ]

        self.list_tests_and_assert(select, exclude, expected, indirect_selection)
        self.run_tests_and_assert(select, exclude, expected, indirect_selection=indirect_selection)

class TestExpansionWithSelectors(TestSelectionExpansion):

    @property
    def selectors_config(self):
        return yaml.safe_load('''
            selectors:
            - name: model_a_unset_indirect_selection
              definition:
                method: fqn
                value: model_a
            - name: model_a_no_indirect_selection
              definition:
                method: fqn
                value: model_a
                indirect_selection: "cautious"
            - name: model_a_yes_indirect_selection
              definition:
                method: fqn
                value: model_a
                indirect_selection: "eager"
        ''')

    @use_profile('postgres')
    def test__postgres__selector_model_a_unset_indirect_selection(self):
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_unset_indirect_selection')
        self.run_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_unset_indirect_selection')

    @use_profile('postgres')
    def test__postgres__selector_model_a_no_indirect_selection(self):
        expected = ['just_a','unique_model_a_fun']

        self.list_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_no_indirect_selection')
        self.run_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_no_indirect_selection')


    @use_profile('postgres')
    def test__postgres__selector_model_a_yes_indirect_selection(self):
        expected = [
            'cf_a_b', 'cf_a_src', 'just_a',
            'relationships_model_a_fun__fun__ref_model_b_',
            'relationships_model_a_fun__fun__source_my_src_my_tbl_',
            'unique_model_a_fun'
        ]

        self.list_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_yes_indirect_selection')
        self.run_tests_and_assert(include=None, exclude=None, expected_tests=expected, selector_name='model_a_yes_indirect_selection')
