from datetime import timedelta
import pickle
import pytest

from dbt.contracts.graph.unparsed import (
    UnparsedNode,
    UnparsedRunHook,
    UnparsedMacro,
    Time,
    TimePeriod,
    FreshnessThreshold,
    Quoting,
    UnparsedSourceDefinition,
    UnparsedSourceTableDefinition,
    UnparsedDocumentationFile,
    UnparsedColumn,
    UnparsedNodeUpdate,
    UnparsedModelUpdate,
    Docs,
    UnparsedExposure,
    MaturityType,
    Owner,
    ExposureType,
    UnparsedMetric,
    UnparsedMetricTypeParams,
    UnparsedMetricInputMeasure,
    UnparsedVersion,
)
from dbt.contracts.results import FreshnessStatus
from dbt.node_types import NodeType
from .utils import ContractTestCase


class TestUnparsedMacro(ContractTestCase):
    ContractType = UnparsedMacro

    def test_ok(self):
        macro_dict = {
            "path": "/root/path.sql",
            "original_file_path": "/root/path.sql",
            "package_name": "test",
            "language": "sql",
            "raw_code": "{% macro foo() %}select 1 as id{% endmacro %}",
            "resource_type": "macro",
        }
        macro = self.ContractType(
            path="/root/path.sql",
            original_file_path="/root/path.sql",
            package_name="test",
            language="sql",
            raw_code="{% macro foo() %}select 1 as id{% endmacro %}",
            resource_type=NodeType.Macro,
        )
        self.assert_symmetric(macro, macro_dict)
        pickle.loads(pickle.dumps(macro))

    def test_invalid_missing_field(self):
        macro_dict = {
            "path": "/root/path.sql",
            "original_file_path": "/root/path.sql",
            # 'package_name': 'test',
            "language": "sql",
            "raw_code": "{% macro foo() %}select 1 as id{% endmacro %}",
            "resource_type": "macro",
        }
        self.assert_fails_validation(macro_dict)

    def test_invalid_extra_field(self):
        macro_dict = {
            "path": "/root/path.sql",
            "original_file_path": "/root/path.sql",
            "package_name": "test",
            "language": "sql",
            "raw_code": "{% macro foo() %}select 1 as id{% endmacro %}",
            "extra": "extra",
            "resource_type": "macro",
        }
        self.assert_fails_validation(macro_dict)


class TestUnparsedNode(ContractTestCase):
    ContractType = UnparsedNode

    def test_ok(self):
        node_dict = {
            "name": "foo",
            "resource_type": NodeType.Model,
            "path": "/root/x/path.sql",
            "original_file_path": "/root/path.sql",
            "package_name": "test",
            "language": "sql",
            "raw_code": 'select * from {{ ref("thing") }}',
        }
        node = self.ContractType(
            package_name="test",
            path="/root/x/path.sql",
            original_file_path="/root/path.sql",
            language="sql",
            raw_code='select * from {{ ref("thing") }}',
            name="foo",
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertFalse(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)
        pickle.loads(pickle.dumps(node))

    def test_empty(self):
        node_dict = {
            "name": "foo",
            "resource_type": NodeType.Model,
            "path": "/root/x/path.sql",
            "original_file_path": "/root/path.sql",
            "package_name": "test",
            "language": "sql",
            "raw_code": "  \n",
        }
        node = UnparsedNode(
            package_name="test",
            path="/root/x/path.sql",
            original_file_path="/root/path.sql",
            language="sql",
            raw_code="  \n",
            name="foo",
            resource_type=NodeType.Model,
        )
        self.assert_symmetric(node, node_dict)
        self.assertTrue(node.empty)

        self.assert_fails_validation(node_dict, cls=UnparsedRunHook)
        self.assert_fails_validation(node_dict, cls=UnparsedMacro)


class TestUnparsedRunHook(ContractTestCase):
    ContractType = UnparsedRunHook

    def test_ok(self):
        node_dict = {
            "name": "foo",
            "resource_type": NodeType.Operation,
            "path": "/root/dbt_project.yml",
            "original_file_path": "/root/dbt_project.yml",
            "package_name": "test",
            "language": "sql",
            "raw_code": "GRANT select on dbt_postgres",
            "index": 4,
        }
        node = self.ContractType(
            package_name="test",
            path="/root/dbt_project.yml",
            original_file_path="/root/dbt_project.yml",
            language="sql",
            raw_code="GRANT select on dbt_postgres",
            name="foo",
            resource_type=NodeType.Operation,
            index=4,
        )
        self.assert_symmetric(node, node_dict)
        self.assert_fails_validation(node_dict, cls=UnparsedNode)
        pickle.loads(pickle.dumps(node))

    def test_bad_type(self):
        node_dict = {
            "name": "foo",
            "resource_type": NodeType.Model,  # invalid
            "path": "/root/dbt_project.yml",
            "original_file_path": "/root/dbt_project.yml",
            "package_name": "test",
            "language": "sql",
            "raw_code": "GRANT select on dbt_postgres",
            "index": 4,
        }
        self.assert_fails_validation(node_dict)


class TestFreshnessThreshold(ContractTestCase):
    ContractType = FreshnessThreshold

    def test_empty(self):
        empty = self.ContractType()
        self.assert_symmetric(empty, {"error_after": {}, "warn_after": {}})
        self.assertEqual(empty.status(float("Inf")), FreshnessStatus.Pass)
        self.assertEqual(empty.status(0), FreshnessStatus.Pass)

    def test_both(self):
        threshold = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        dct = {
            "error_after": {"count": 2, "period": "day"},
            "warn_after": {"count": 18, "period": "hour"},
        }
        self.assert_symmetric(threshold, dct)

        error_seconds = timedelta(days=3).total_seconds()
        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(error_seconds), FreshnessStatus.Error)
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)
        pickle.loads(pickle.dumps(threshold))

    def test_merged(self):
        t1 = self.ContractType(
            warn_after=Time(count=36, period=TimePeriod.hour),
            error_after=Time(count=2, period=TimePeriod.day),
        )
        t2 = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
        )
        threshold = self.ContractType(
            warn_after=Time(count=18, period=TimePeriod.hour),
            error_after=Time(count=None, period=None),
        )
        self.assertEqual(threshold, t1.merged(t2))

        warn_seconds = timedelta(days=1).total_seconds()
        pass_seconds = timedelta(hours=3).total_seconds()
        self.assertEqual(threshold.status(warn_seconds), FreshnessStatus.Warn)
        self.assertEqual(threshold.status(pass_seconds), FreshnessStatus.Pass)


class TestQuoting(ContractTestCase):
    ContractType = Quoting

    def test_empty(self):
        empty = self.ContractType()
        self.assert_symmetric(empty, {})

    def test_partial(self):
        a = self.ContractType(None, True, False)
        b = self.ContractType(True, False, None)
        self.assert_symmetric(a, {"schema": True, "identifier": False})
        self.assert_symmetric(b, {"database": True, "schema": False})

        c = a.merged(b)
        self.assertEqual(c, self.ContractType(True, False, False))
        self.assert_symmetric(c, {"database": True, "schema": False, "identifier": False})
        pickle.loads(pickle.dumps(c))


class TestUnparsedSourceDefinition(ContractTestCase):
    ContractType = UnparsedSourceDefinition

    def test_defaults(self):
        minimum = self.ContractType(name="foo")
        from_dict = {"name": "foo"}
        to_dict = {
            "name": "foo",
            "description": "",
            "freshness": {"error_after": {}, "warn_after": {}},
            "quoting": {},
            "tables": [],
            "loader": "",
            "meta": {},
            "tags": [],
            "config": {},
        }
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        empty = self.ContractType(
            name="foo",
            description="a description",
            quoting=Quoting(database=False),
            loader="some_loader",
            freshness=FreshnessThreshold(),
            tables=[],
            meta={},
        )
        dct = {
            "name": "foo",
            "description": "a description",
            "quoting": {"database": False},
            "loader": "some_loader",
            "freshness": {"error_after": {}, "warn_after": {}},
            "tables": [],
            "meta": {},
            "tags": [],
            "config": {},
        }
        self.assert_symmetric(empty, dct)

    def test_table_defaults(self):
        table_1 = UnparsedSourceTableDefinition(name="table1")
        table_2 = UnparsedSourceTableDefinition(
            name="table2",
            description="table 2",
            quoting=Quoting(database=True),
        )
        source = self.ContractType(name="foo", tables=[table_1, table_2])
        from_dict = {
            "name": "foo",
            "tables": [
                {"name": "table1"},
                {
                    "name": "table2",
                    "description": "table 2",
                    "quoting": {"database": True},
                },
            ],
        }
        to_dict = {
            "name": "foo",
            "description": "",
            "config": {},
            "loader": "",
            "freshness": {"error_after": {}, "warn_after": {}},
            "quoting": {},
            "meta": {},
            "tables": [
                {
                    "name": "table1",
                    "description": "",
                    "config": {},
                    "docs": {"show": True},
                    "tests": [],
                    "columns": [],
                    "constraints": [],
                    "quoting": {},
                    "freshness": {"error_after": {}, "warn_after": {}},
                    "meta": {},
                    "tags": [],
                },
                {
                    "name": "table2",
                    "description": "table 2",
                    "config": {},
                    "docs": {"show": True},
                    "tests": [],
                    "columns": [],
                    "constraints": [],
                    "quoting": {"database": True},
                    "freshness": {"error_after": {}, "warn_after": {}},
                    "meta": {},
                    "tags": [],
                },
            ],
            "tags": [],
        }
        self.assert_from_dict(source, from_dict)
        self.assert_symmetric(source, to_dict)
        pickle.loads(pickle.dumps(source))


class TestUnparsedDocumentationFile(ContractTestCase):
    ContractType = UnparsedDocumentationFile

    def test_ok(self):
        doc = self.ContractType(
            package_name="test",
            path="/root/docs",
            original_file_path="/root/docs/doc.md",
            file_contents="blah blah blah",
        )
        doc_dict = {
            "package_name": "test",
            "path": "/root/docs",
            "original_file_path": "/root/docs/doc.md",
            "file_contents": "blah blah blah",
        }
        self.assert_symmetric(doc, doc_dict)
        self.assertEqual(doc.resource_type, NodeType.Documentation)
        self.assert_fails_validation(doc_dict, UnparsedNode)
        pickle.loads(pickle.dumps(doc))

    def test_extra_field(self):
        self.assert_fails_validation({})
        doc_dict = {
            "package_name": "test",
            "path": "/root/docs",
            "original_file_path": "/root/docs/doc.md",
            "file_contents": "blah blah blah",
            "resource_type": "docs",
        }
        self.assert_fails_validation(doc_dict)


class TestUnparsedNodeUpdate(ContractTestCase):
    ContractType = UnparsedNodeUpdate

    def test_defaults(self):
        minimum = self.ContractType(
            name="foo",
            yaml_key="models",
            original_file_path="/some/fake/path",
            package_name="test",
        )
        from_dict = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
        }
        to_dict = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "columns": [],
            "description": "",
            "docs": {"show": True},
            "tests": [],
            "meta": {},
            "config": {},
            "constraints": [],
        }
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        update = self.ContractType(
            name="foo",
            yaml_key="models",
            original_file_path="/some/fake/path",
            package_name="test",
            description="a description",
            tests=["table_test"],
            meta={"key": ["value1", "value2"]},
            columns=[
                UnparsedColumn(
                    name="x",
                    description="x description",
                    meta={"key2": "value3"},
                ),
                UnparsedColumn(
                    name="y",
                    description="y description",
                    tests=["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    meta={},
                    tags=["a", "b"],
                ),
            ],
            docs=Docs(show=False),
        )
        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "constraints": [],
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                    "tags": [],
                    "constraints": [],
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "tags": ["a", "b"],
                    "constraints": [],
                },
            ],
            "docs": {"show": False},
            "config": {},
        }
        self.assert_symmetric(update, dct)
        pickle.loads(pickle.dumps(update))

    def test_bad_test_type(self):
        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": [100, {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)

        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                # column missing a name
                {
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)

        # missing a name
        dct = {
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)


class TestUnparsedModelUpdate(ContractTestCase):
    ContractType = UnparsedModelUpdate

    def test_defaults(self):
        minimum = self.ContractType(
            name="foo",
            yaml_key="models",
            original_file_path="/some/fake/path",
            package_name="test",
        )
        from_dict = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
        }
        to_dict = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "columns": [],
            "description": "",
            "docs": {"show": True},
            "tests": [],
            "meta": {},
            "config": {},
            "constraints": [],
            "versions": [],
        }
        self.assert_from_dict(minimum, from_dict)
        self.assert_to_dict(minimum, to_dict)

    def test_contents(self):
        update = self.ContractType(
            name="foo",
            yaml_key="models",
            original_file_path="/some/fake/path",
            package_name="test",
            description="a description",
            tests=["table_test"],
            meta={"key": ["value1", "value2"]},
            columns=[
                UnparsedColumn(
                    name="x",
                    description="x description",
                    meta={"key2": "value3"},
                ),
                UnparsedColumn(
                    name="y",
                    description="y description",
                    tests=["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    meta={},
                    tags=["a", "b"],
                ),
            ],
            docs=Docs(show=False),
            versions=[UnparsedVersion(v=2)],
        )
        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "constraints": [],
            "versions": [
                {
                    "v": 2,
                    "description": "",
                    "columns": [],
                    "config": {},
                    "constraints": [],
                    "docs": {"show": True},
                }
            ],
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                    "tags": [],
                    "constraints": [],
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "tags": ["a", "b"],
                    "constraints": [],
                },
            ],
            "docs": {"show": False},
            "config": {},
        }
        self.assert_symmetric(update, dct)
        pickle.loads(pickle.dumps(update))

    def test_bad_test_type(self):
        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": [100, {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)

        dct = {
            "name": "foo",
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                # column missing a name
                {
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)

        # missing a name
        dct = {
            "yaml_key": "models",
            "original_file_path": "/some/fake/path",
            "package_name": "test",
            "description": "a description",
            "tests": ["table_test"],
            "meta": {"key": ["value1", "value2"]},
            "columns": [
                {
                    "name": "x",
                    "description": "x description",
                    "docs": {"show": True},
                    "tests": [],
                    "meta": {"key2": "value3"},
                },
                {
                    "name": "y",
                    "description": "y description",
                    "docs": {"show": True},
                    "tests": ["unique", {"accepted_values": {"values": ["blue", "green"]}}],
                    "meta": {},
                    "yaml_key": "models",
                    "original_file_path": "/some/fake/path",
                },
            ],
            "docs": {"show": True},
        }
        self.assert_fails_validation(dct)


class TestUnparsedExposure(ContractTestCase):
    ContractType = UnparsedExposure

    def get_ok_dict(self):
        return {
            "name": "my_exposure",
            "type": "dashboard",
            "owner": {"name": "example", "email": "name@example.com", "slack": "#channel"},
            "maturity": "medium",
            "meta": {"tool": "my_tool"},
            "tags": ["my_department"],
            "url": "https://example.com/dashboards/1",
            "description": "A exposure",
            "config": {},
            "depends_on": [
                'ref("my_model")',
                'source("raw", "source_table")',
            ],
        }

    def test_ok(self):
        exposure = self.ContractType(
            name="my_exposure",
            type=ExposureType.Dashboard,
            owner=Owner(name="example", email="name@example.com", _extra={"slack": "#channel"}),
            maturity=MaturityType.Medium,
            url="https://example.com/dashboards/1",
            description="A exposure",
            config={},
            meta={"tool": "my_tool"},
            tags=["my_department"],
            depends_on=['ref("my_model")', 'source("raw", "source_table")'],
        )
        dct = self.get_ok_dict()
        self.assert_symmetric(exposure, dct)
        pickle.loads(pickle.dumps(exposure))

    def test_ok_exposures(self):
        for exposure_allowed in ("dashboard", "notebook", "analysis", "ml", "application"):
            tst = self.get_ok_dict()
            tst["type"] = exposure_allowed
            assert self.ContractType.from_dict(tst).type == exposure_allowed

    def test_bad_exposure(self):
        # bad exposure: None isn't allowed
        for exposure_not_allowed in (None, "not an exposure"):
            tst = self.get_ok_dict()
            tst["type"] = exposure_not_allowed
            self.assert_fails_validation(tst)

    def test_no_exposure(self):
        tst = self.get_ok_dict()
        del tst["type"]
        self.assert_fails_validation(tst)

    def test_ok_maturities(self):
        for maturity_allowed in (None, "low", "medium", "high"):
            tst = self.get_ok_dict()
            tst["maturity"] = maturity_allowed
            assert self.ContractType.from_dict(tst).maturity == maturity_allowed

        tst = self.get_ok_dict()
        del tst["maturity"]
        assert self.ContractType.from_dict(tst).maturity is None

    def test_bad_maturity(self):
        tst = self.get_ok_dict()
        tst["maturity"] = "invalid maturity"
        self.assert_fails_validation(tst)

    def test_bad_owner_missing_things(self):
        tst = self.get_ok_dict()
        del tst["owner"]["email"]
        del tst["owner"]["name"]
        self.assert_fails_validation(tst)

        del tst["owner"]
        self.assert_fails_validation(tst)

    def test_bad_tags(self):
        tst = self.get_ok_dict()
        tst["tags"] = [123]
        self.assert_fails_validation(tst)


class TestUnparsedMetric(ContractTestCase):
    ContractType = UnparsedMetric

    def get_ok_dict(self):
        return {
            "name": "new_customers",
            "label": "New Customers",
            "description": "New customers",
            "type": "simple",
            "type_params": {
                "measure": {
                    "name": "customers",
                    "filter": "is_new = true",
                    "join_to_timespine": False,
                },
            },
            "config": {},
            "tags": [],
            "meta": {"is_okr": True},
        }

    def test_ok(self):
        metric = self.ContractType(
            name="new_customers",
            label="New Customers",
            description="New customers",
            type="simple",
            type_params=UnparsedMetricTypeParams(
                measure=UnparsedMetricInputMeasure(
                    name="customers",
                    filter="is_new = true",
                )
            ),
            config={},
            meta={"is_okr": True},
        )
        dct = self.get_ok_dict()
        self.assert_symmetric(metric, dct)
        pickle.loads(pickle.dumps(metric))

    def test_bad_metric_no_type_params(self):
        tst = self.get_ok_dict()
        del tst["type_params"]
        self.assert_fails_validation(tst)

    def test_bad_tags(self):
        tst = self.get_ok_dict()
        tst["tags"] = [123]
        self.assert_fails_validation(tst)


class TestUnparsedVersion(ContractTestCase):
    ContractType = UnparsedVersion

    def get_ok_dict(self):
        return {
            "v": 2,
            "defined_in": "test_defined_in",
            "description": "A version",
            "config": {},
            "constraints": [],
            "docs": {"show": False},
            "tests": [],
            "columns": [],
        }

    def test_ok(self):
        version = self.ContractType(
            v=2,
            defined_in="test_defined_in",
            description="A version",
            config={},
            constraints=[],
            docs=Docs(show=False),
            tests=[],
            columns=[],
        )
        dct = self.get_ok_dict()
        self.assert_symmetric(version, dct)
        pickle.loads(pickle.dumps(version))

    def test_bad_version_no_v(self):
        version = self.get_ok_dict()
        del version["v"]
        self.assert_fails_validation(version)


@pytest.mark.parametrize(
    "left,right,expected_lt",
    [
        # same types
        (2, 12, True),
        (12, 2, False),
        ("a", "b", True),
        ("b", "a", False),
        # mismatched types - numeric
        (2, 12.0, True),
        (12.0, 2, False),
        (2, "12", True),
        ("12", 2, False),
        # mismatched types
        (1, "test", True),
        ("test", 1, False),
    ],
)
def test_unparsed_version_lt(left, right, expected_lt):
    assert (UnparsedVersion(left) < UnparsedVersion(right)) == expected_lt
