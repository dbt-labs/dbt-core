#! /usr/bin/env python

from argparse import ArgumentParser
import logging
import os
import re
import sys
import yaml

LOGGER = logging.getLogger('upgrade_dbt_schema')
LOGFILE = 'upgrade_dbt_schema_tests_v1_to_v2.txt'

COLUMN_NAME_PAT = re.compile(r'\A[a-zA-Z0-9_]+\Z')

# compatibility nonsense
try:
    basestring = basestring
except NameError:
    basestring = str


def is_column_name(value):
    if not isinstance(value, basestring):
        return False
    return COLUMN_NAME_PAT.match(value) is not None


class OperationalError(Exception):
    pass


def setup_logging(filename):
    LOGGER.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(filename=filename)
    file_handler.setLevel(logging.DEBUG)
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.WARNING)
    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(stderr_handler)


def parse_args(args):
    parser = ArgumentParser(description='dbt schema converter')
    parser.add_argument(
        '--logfile-path',
        dest='logfile_path',
        help='The path to write the logfile to',
        default=LOGFILE
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='if set, overwrite any existing file'
    )
    parser.add_argument(
        '--in-place',
        action='store_true',
        dest='in_place',
        help=('if set, overwrite the input file and generate a ".bak" file '
              'instead of generating a ".new" file')
    )
    parser.add_argument('--output-path', dest='output_path', default=None)
    parser.add_argument('--backup-path', dest='backup_path', default=None)
    parser.add_argument('input_path')
    parsed = parser.parse_args()
    if parsed.in_place:
        parsed.overwrite = True
    if parsed.output_path is None:
        if parsed.in_place:
            parsed.output_path = parsed.input_path
            parsed.backup_path = parsed.input_path + '.bak'
        else:
            parsed.output_path = parsed.input_path + '.new'
    return parsed


def backup_file(src, dst, overwrite):
    if not overwrite and os.path.exists(dst):
        raise OperationalError(
            'backup file at {} already exists and --overwrite was not passed'
            .format(dst)
        )
    LOGGER.debug('backing up file at {} to {}'.format(src, dst))
    with open(src, 'rb'), open(dst, 'wb') as ifp, ofp:
        ofp.write(ifp.read())
    LOGGER.debug('backup successful')


def validate_args(parsed):
    if not os.path.exists(parsed.input_path):
        raise OperationalError(
            'input file at {} does not exist!'.format(parsed.input_path)
        )

    if os.path.exists(parsed.output_path) and not parsed.overwrite:
        raise OperationalError(
            'output file at {} already exists, and --overwrite was not passed'
            .format(parsed.output_path)
        )
        return




def handle(parsed):
    """Try to handle the schema conversion. On failure, raise OperationalError
    and let the caller handle it.
    """
    validate_args(parsed)
    if parsed.backup_path:
        backup_file(parsed.output_path, parsed.backup_path, parsed.overwrite)

    LOGGER.info('loading input file at {}'.format(parsed.input_path))

    with open(parsed.input_path) as fp:
        initial = yaml.safe_load(fp)

    version = initial.get('version', 1)
    # the isinstance check is to handle the case of models named 'version'
    if version != 1 and isinstance(version, int):
        raise OperationalError(
            'input file is not a v1 yaml file (reports as {})'.format(version)
        )

    new_file = convert_schema(initial)

    LOGGER.debug(
        'writing converted schema to output file at {}'.format(
            parsed.output_path
        )
    )

    with open(parsed.output_path, 'w') as fp:
        yaml.safe_dump(new_file, fp)

    LOGGER.info(
        'successfully wrote v2 schema.yml file to {}'.format(
            parsed.output_path
        )
    )


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parsed = parse_args(args)
    setup_logging(parsed.logfile_path)
    try:
        handle(parsed)
    except OperationalError as exc:
        LOGGER.error(exc.message)
    except:
        LOGGER.exception('Fatal error during conversion attempt')
    else:
        LOGGER.info('successfully converted existing {} to {}'.format(
            parsed.input_path, parsed.output_path
        ))


def sort_keyfunc(item):
    if isinstance(item, basestring):
        return item
    else:
        return list(item)[0]


def sorted_column_list(column_dict):
    columns = []
    for column in sorted(column_dict.values(), key=lambda c: c['name']):
        # make the unit tests a lot nicer.
        column['tests'].sort(key=sort_keyfunc)
        columns.append(column)
    return columns


class ModelTestBuilder(object):
    SIMPLE_COLUMN_TESTS = {'unique', 'not_null'}
    # map test name -> the key that indicates column name
    COMPLEX_COLUMN_TESTS = {
        'relationships': 'from',
        'accepted_values': 'field',
    }
    def __init__(self, model_name):
        self.model_name = model_name
        self.columns = {}
        self.model_tests = []

    def get_column(self, column_name):
        if column_name in self.columns:
            return self.columns[column_name]
        column = {'name': column_name, 'tests': []}
        self.columns[column_name] = column
        return column

    def add_column_test(self, column_name, test_name):
        column = self.get_column(column_name)
        column['tests'].append(test_name)

    def add_table_test(self, test_name, test_value):
        self.model_tests.append({test_name: test_value})

    def handle_simple_column(self, test_name, test_values):
        for column_name in test_values:
            LOGGER.info(
                'found a {} test for model {}, column {}'.format(
                    test_name, self.model_name, column_name
                )
            )
            self.add_column_test(column_name, test_name)

    def handle_complex_column(self, test_name, test_values):
        """'complex' columns are lists of dicts, where each dict has a single
        key (the test name) and the value of that key is a dict of test values.
        """
        column_key = self.COMPLEX_COLUMN_TESTS[test_name]
        for dct in test_values:
            if column_key not in dct:
                raise OperationalError(
                    'got an invalid {} test in model {}, no "{}" value in {}'
                    .format(test_name, self.model_name, column_key, dct)
                )
            column_name = dct[column_key]
            # for syntax nice-ness reasons, we define these tests as single-key
            # dicts where the key is the test name.
            test_value = {k: v for k, v in dct.items() if k != column_key}
            value = {test_name: test_value}
            LOGGER.info(
                'found a test for model {}, column {} - arguments: {}'.format(
                    self.model_name, column_name, test_value
                )
            )
            self.add_column_test(column_name, value)

    def handle_unknown_test(self, test_name, test_values):
        for test_value in test_values:
            self.add_table_test(test_name, test_value)

    def populate_test(self, test_name, test_values):
        if not isinstance(test_values, list):
            raise OperationalError(
                'Expected type "list" for test values in constraints '
                'under test {} inside model {}, got "{}"'.format(
                    test_name, model_name, type(test_values)
                )
            )
        if test_name in self.SIMPLE_COLUMN_TESTS:
            self.handle_simple_column(test_name, test_values)
        elif test_name in self.COMPLEX_COLUMN_TESTS:
            self.handle_complex_column(test_name, test_values)
        else:
            if all(is_column_name(v) for v in test_values):
                # looks like a simple test to me!
                LOGGER.debug(
                    'Found custom test named {}, inferred that it only takes '
                    'columns as arguments'.format(test_name)
                )
                self.handle_simple_column(test_name, test_values)
            else:
                LOGGER.warning(
                    'Found a custom test named {} that appears to take extra '
                    'arguments. Converting it to a model-level test'.format(
                        test_name
                    )
                )
                self.handle_unknown_test(test_name, test_values)

    def populate_from_constraints(self, constraints):
        for test_name, test_values in constraints.items():
            self.populate_test(test_name, test_values)

    def generate_model_dict(self):
        model = {'name': self.model_name}
        if self.model_tests:
            model['tests'] = self.model_tests

        if self.columns:
            model['columns'] = sorted_column_list(self.columns)
        return model


def convert_schema(initial):
    models = []

    for model_name, model_data in initial.items():
        if 'constraints' not in model_data:
            # don't care about this model
            continue
        builder = ModelTestBuilder(model_name)
        builder.populate_from_constraints(model_data['constraints'])
        model = builder.generate_model_dict()
        models.append(model)

    return {
        'version': 2,
        'models': models,
    }



if __name__ == '__main__':
    main()

import unittest

SAMPLE_SCHEMA = '''
foo:
    constraints:
        not_null:
            - id
            - email
            - favorite_color
        unique:
            - id
            - email
        accepted_values:
            - { field: favorite_color, values: ['blue', 'green'] }
            - { field: likes_puppies, values: ['yes'] }
        simple_custom:
            - id
            - favorite_color
        # becomes a table-level test
        complex_custom:
            - { field: favorite_color, arg1: test, arg2: ref('bar') }

bar:
    constraints:
        not_null:
            - id
'''



class TestConvert(unittest.TestCase):
    maxDiff = None
    def test_convert(self):
        input_schema = yaml.safe_load(SAMPLE_SCHEMA)
        output_schema = convert_schema(input_schema)
        self.assertEqual(output_schema['version'], 2)
        sorted_models = sorted(output_schema['models'], key=lambda x: x['name'])
        expected = [
            {
                'name': 'bar',
                'columns': [
                    {
                        'name': 'id',
                        'tests': [
                            'not_null'
                        ]
                    }
                ]
            },
            {
                'name': 'foo',
                'columns': [
                    {
                        'name': 'email',
                        'tests': [
                            'not_null',
                            'unique',
                        ],
                    },
                    {
                        'name': 'favorite_color',
                        'tests': [
                            {'accepted_values': {'values': ['blue', 'green']}},
                            'not_null',
                            'simple_custom',
                        ],
                    },
                    {
                        'name': 'id',
                        'tests': [
                            'not_null',
                            'simple_custom',
                            'unique',
                        ],
                    },
                    {
                        'name': 'likes_puppies',
                        'tests': [
                            {'accepted_values': {'values': ['yes']}},
                        ]
                    },
                ],
                'tests': [
                    {'complex_custom': {
                        'field': 'favorite_color',
                        'arg1': 'test',
                        'arg2': "ref('bar')"
                    }},
                ],
            },
        ]
        self.assertEqual(sorted_models, expected)

