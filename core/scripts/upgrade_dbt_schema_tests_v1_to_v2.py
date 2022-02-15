#! /usr/bin/env python
from __future__ import print_function
from argparse import ArgumentParser
import logging
import os
import re
import sys
import yaml

LOGGER = logging.getLogger("upgrade_dbt_schema")
LOGFILE = "upgrade_dbt_schema_tests_v1_to_v2.txt"

COLUMN_NAME_PAT = re.compile(r"\A[a-zA-Z0-9_]+\Z")

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
    def __init__(self, message):
        self.message = message
        super().__init__(message)


def setup_logging(filename):
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s: %(asctime)s: %(message)s")
    file_handler = logging.FileHandler(filename=filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(stderr_handler)


def parse_args(args):
    parser = ArgumentParser(description="dbt schema converter")
    parser.add_argument(
        "--logfile-path",
        dest="logfile_path",
        help="The path to write the logfile to",
        default=LOGFILE,
    )
    parser.add_argument(
        "--no-backup",
        action="store_false",
        dest="backup",
        help='if set, do not generate ".backup" files.',
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=("if set, apply changes instead of just logging about found " "schema.yml files"),
    )
    parser.add_argument(
        "--complex-test",
        dest="extra_complex_tests",
        action="append",
        help='extra "complex" tests, as key:value pairs, where key is the '
        "test name and value is the test key that contains the column "
        "name.",
    )
    parser.add_argument(
        "--complex-test-file",
        dest="extra_complex_tests_file",
        default=None,
        help="The path to an optional yaml file of key/value pairs that does "
        "the same as --complex-test.",
    )
    parser.add_argument("search_directory")
    parsed = parser.parse_args(args)
    return parsed


def backup_file(src, dst):
    if not os.path.exists(src):
        LOGGER.debug("no file at {} - nothing to back up".format(src))
        return
    LOGGER.debug("backing up file at {} to {}".format(src, dst))
    with open(src, "rb") as ifp, open(dst, "wb") as ofp:
        ofp.write(ifp.read())
    LOGGER.debug("backup successful")


def validate_and_mutate_args(parsed):
    """Validate arguments, raising OperationalError on bad args. Also convert
    the complex tests from 'key:value' -> {'key': 'value'}.
    """
    if not os.path.exists(parsed.search_directory):
        raise OperationalError(
            "input directory at {} does not exist!".format(parsed.search_directory)
        )

    complex_tests = {}

    if parsed.extra_complex_tests_file:
        if not os.path.exists(parsed.extra_complex_tests_file):
            raise OperationalError(
                "complex tests definition file at {} does not exist".format(
                    parsed.extra_complex_tests_file
                )
            )
        with open(parsed.extra_complex_tests_file) as fp:
            extra_tests = yaml.safe_load(fp)
        if not isinstance(extra_tests, dict):
            raise OperationalError(
                "complex tests definition file at {} is not a yaml mapping".format(
                    parsed.extra_complex_tests_file
                )
            )
        complex_tests.update(extra_tests)

    if parsed.extra_complex_tests:
        for tst in parsed.extra_complex_tests:
            pair = tst.split(":", 1)
            if len(pair) != 2:
                raise OperationalError('Invalid complex test "{}"'.format(tst))
            complex_tests[pair[0]] = pair[1]

    parsed.extra_complex_tests = complex_tests


def handle(parsed):
    """Try to handle the schema conversion. On failure, raise OperationalError
    and let the caller handle it.
    """
    validate_and_mutate_args(parsed)
    with open(os.path.join(parsed.search_directory, "dbt_project.yml")) as fp:
        project = yaml.safe_load(fp)
    model_dirs = project.get("model-paths", ["models"])
    if parsed.apply:
        print("converting the following files to the v2 spec:")
    else:
        print("would convert the following files to the v2 spec:")
    for model_dir in model_dirs:
        search_path = os.path.join(parsed.search_directory, model_dir)
        convert_project(search_path, parsed.backup, parsed.apply, parsed.extra_complex_tests)
    if not parsed.apply:
        print(
            "Run with --apply to write these changes. Files with an error "
            "will not be converted."
        )


def find_all_yaml(path):
    for root, _, files in os.walk(path):
        for filename in files:
            if filename.endswith(".yml"):
                yield os.path.join(root, filename)


def convert_project(path, backup, write, extra_complex_tests):
    for filepath in find_all_yaml(path):
        try:
            convert_file(filepath, backup, write, extra_complex_tests)
        except OperationalError as exc:
            print("{} - could not convert: {}".format(filepath, exc.message))
            LOGGER.error(exc.message)


def convert_file(path, backup, write, extra_complex_tests):
    LOGGER.info("loading input file at {}".format(path))

    with open(path) as fp:
        initial = yaml.safe_load(fp)

    version = initial.get("version", 1)
    # the isinstance check is to handle the case of models named 'version'
    if version == 2:
        msg = "{} - already v2, no need to update".format(path)
        print(msg)
        LOGGER.info(msg)
        return
    elif version != 1 and isinstance(version, int):
        raise OperationalError("input file is not a v1 yaml file (reports as {})".format(version))

    new_file = convert_schema(initial, extra_complex_tests)

    if write:
        LOGGER.debug("writing converted schema to output file at {}".format(path))
        if backup:
            backup_file(path, path + ".backup")

        with open(path, "w") as fp:
            yaml.dump(new_file, fp, default_flow_style=False, indent=2)

        print("{} - UPDATED".format(path))
        LOGGER.info("successfully wrote v2 schema.yml file to {}".format(path))
    else:
        print("{} - Not updated (dry run)".format(path))
        LOGGER.info("would have written v2 schema.yml file to {}".format(path))


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parsed = parse_args(args)
    setup_logging(parsed.logfile_path)
    try:
        handle(parsed)
    except OperationalError as exc:
        LOGGER.error(exc.message)
    except:  # noqa: E722
        LOGGER.exception("Fatal error during conversion attempt")
    else:
        LOGGER.info("successfully converted files in {}".format(parsed.search_directory))


def sort_keyfunc(item):
    if isinstance(item, basestring):
        return item
    else:
        return list(item)[0]


def sorted_column_list(column_dict):
    columns = []
    for column in sorted(column_dict.values(), key=lambda c: c["name"]):
        # make the unit tests a lot nicer.
        column["tests"].sort(key=sort_keyfunc)
        columns.append(CustomSortedColumnsSchema(**column))
    return columns


class ModelTestBuilder:
    SIMPLE_COLUMN_TESTS = {"unique", "not_null"}
    # map test name -> the key that indicates column name
    COMPLEX_COLUMN_TESTS = {
        "relationships": "from",
        "accepted_values": "field",
    }

    def __init__(self, model_name, extra_complex_tests=None):
        self.model_name = model_name
        self.columns = {}
        self.model_tests = []
        self._simple_column_tests = self.SIMPLE_COLUMN_TESTS.copy()
        # overwrite with ours last so we always win.
        self._complex_column_tests = {}
        if extra_complex_tests:
            self._complex_column_tests.update(extra_complex_tests)
        self._complex_column_tests.update(self.COMPLEX_COLUMN_TESTS)

    def get_column(self, column_name):
        if column_name in self.columns:
            return self.columns[column_name]
        column = {"name": column_name, "tests": []}
        self.columns[column_name] = column
        return column

    def add_column_test(self, column_name, test_name):
        column = self.get_column(column_name)
        column["tests"].append(test_name)

    def add_table_test(self, test_name, test_value):
        if not isinstance(test_value, dict):
            test_value = {"arg": test_value}
        self.model_tests.append({test_name: test_value})

    def handle_simple_column_test(self, test_name, test_values):
        for column_name in test_values:
            LOGGER.info(
                "found a {} test for model {}, column {}".format(
                    test_name, self.model_name, column_name
                )
            )
            self.add_column_test(column_name, test_name)

    def handle_complex_column_test(self, test_name, test_values):
        """'complex' columns are lists of dicts, where each dict has a single
        key (the test name) and the value of that key is a dict of test values.
        """
        column_key = self._complex_column_tests[test_name]
        for dct in test_values:
            if column_key not in dct:
                raise OperationalError(
                    'got an invalid {} test in model {}, no "{}" value in {}'.format(
                        test_name, self.model_name, column_key, dct
                    )
                )
            column_name = dct[column_key]
            # for syntax nice-ness reasons, we define these tests as single-key
            # dicts where the key is the test name.
            test_value = {k: v for k, v in dct.items() if k != column_key}
            value = {test_name: test_value}
            LOGGER.info(
                "found a test for model {}, column {} - arguments: {}".format(
                    self.model_name, column_name, test_value
                )
            )
            self.add_column_test(column_name, value)

    def handle_unknown_test(self, test_name, test_values):
        if all(map(is_column_name, test_values)):
            LOGGER.debug(
                "Found custom test named {}, inferred that it only takes "
                "columns as arguments".format(test_name)
            )
            self.handle_simple_column_test(test_name, test_values)
        else:
            LOGGER.warning(
                "Found a custom test named {} that appears to take extra "
                "arguments. Converting it to a model-level test".format(test_name)
            )
            for test_value in test_values:
                self.add_table_test(test_name, test_value)

    def populate_test(self, test_name, test_values):
        if not isinstance(test_values, list):
            raise OperationalError(
                'Expected type "list" for test values in constraints '
                'under test {} inside model {}, got "{}"'.format(
                    test_name, self.model_name, type(test_values)
                )
            )
        if test_name in self._simple_column_tests:
            self.handle_simple_column_test(test_name, test_values)
        elif test_name in self._complex_column_tests:
            self.handle_complex_column_test(test_name, test_values)
        else:
            self.handle_unknown_test(test_name, test_values)

    def populate_from_constraints(self, constraints):
        for test_name, test_values in constraints.items():
            self.populate_test(test_name, test_values)

    def generate_model_dict(self):
        model = {"name": self.model_name}
        if self.model_tests:
            model["tests"] = self.model_tests

        if self.columns:
            model["columns"] = sorted_column_list(self.columns)
        return CustomSortedModelsSchema(**model)


def convert_schema(initial, extra_complex_tests):
    models = []

    for model_name, model_data in initial.items():
        if "constraints" not in model_data:
            # don't care about this model
            continue
        builder = ModelTestBuilder(model_name, extra_complex_tests)
        builder.populate_from_constraints(model_data["constraints"])
        model = builder.generate_model_dict()
        models.append(model)

    return CustomSortedRootSchema(version=2, models=models)


class CustomSortedSchema(dict):
    ITEMS_ORDER = NotImplemented

    @classmethod
    def _items_keyfunc(cls, items):
        key = items[0]
        if key not in cls.ITEMS_ORDER:
            return len(cls.ITEMS_ORDER)
        else:
            return cls.ITEMS_ORDER.index(key)

    @staticmethod
    def representer(self, data):
        """Note that 'self' here is NOT an instance of CustomSortedSchema, but
        of some yaml thing.
        """
        parent_iter = data.items()
        good_iter = sorted(parent_iter, key=data._items_keyfunc)
        return self.represent_mapping("tag:yaml.org,2002:map", good_iter)


class CustomSortedRootSchema(CustomSortedSchema):
    ITEMS_ORDER = ["version", "models"]


class CustomSortedModelsSchema(CustomSortedSchema):
    ITEMS_ORDER = ["name", "columns", "tests"]


class CustomSortedColumnsSchema(CustomSortedSchema):
    ITEMS_ORDER = ["name", "tests"]


for cls in (CustomSortedRootSchema, CustomSortedModelsSchema, CustomSortedColumnsSchema):
    yaml.add_representer(cls, cls.representer)


if __name__ == "__main__":
    main()

else:
    # a cute trick so we only import/run these things under nose.

    import mock  # noqa
    import unittest  # noqa

    SAMPLE_SCHEMA = """
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
            known_complex_custom:
                - { field: likes_puppies, arg1: test }
            # becomes a table-level test
            complex_custom:
                - { field: favorite_color, arg1: test, arg2: ref('bar') }

    bar:
        constraints:
            not_null:
                - id
    """

    EXPECTED_OBJECT_OUTPUT = [
        {"name": "bar", "columns": [{"name": "id", "tests": ["not_null"]}]},
        {
            "name": "foo",
            "columns": [
                {
                    "name": "email",
                    "tests": [
                        "not_null",
                        "unique",
                    ],
                },
                {
                    "name": "favorite_color",
                    "tests": [
                        {"accepted_values": {"values": ["blue", "green"]}},
                        "not_null",
                        "simple_custom",
                    ],
                },
                {
                    "name": "id",
                    "tests": [
                        "not_null",
                        "simple_custom",
                        "unique",
                    ],
                },
                {
                    "name": "likes_puppies",
                    "tests": [
                        {"accepted_values": {"values": ["yes"]}},
                        {"known_complex_custom": {"arg1": "test"}},
                    ],
                },
            ],
            "tests": [
                {
                    "complex_custom": {
                        "field": "favorite_color",
                        "arg1": "test",
                        "arg2": "ref('bar')",
                    }
                },
            ],
        },
    ]

    class TestConvert(unittest.TestCase):
        maxDiff = None

        def test_convert(self):
            input_schema = yaml.safe_load(SAMPLE_SCHEMA)
            output_schema = convert_schema(input_schema, {"known_complex_custom": "field"})
            self.assertEqual(output_schema["version"], 2)
            sorted_models = sorted(output_schema["models"], key=lambda x: x["name"])
            self.assertEqual(sorted_models, EXPECTED_OBJECT_OUTPUT)

        def test_parse_validate_and_mutate_args_simple(self):
            args = ["my-input"]
            parsed = parse_args(args)
            self.assertEqual(parsed.search_directory, "my-input")
            with self.assertRaises(OperationalError):
                validate_and_mutate_args(parsed)
            with mock.patch("os.path.exists") as exists:
                exists.return_value = True
                validate_and_mutate_args(parsed)
            # validate will mutate this to be a dict
            self.assertEqual(parsed.extra_complex_tests, {})

        def test_parse_validate_and_mutate_args_extra_tests(self):
            args = [
                "--complex-test",
                "known_complex_custom:field",
                "--complex-test",
                "other_complex_custom:column",
                "my-input",
            ]
            parsed = parse_args(args)
            with mock.patch("os.path.exists") as exists:
                exists.return_value = True
                validate_and_mutate_args(parsed)
                self.assertEqual(
                    parsed.extra_complex_tests,
                    {"known_complex_custom": "field", "other_complex_custom": "column"},
                )
