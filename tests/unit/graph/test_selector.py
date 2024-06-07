import os
import string
import unittest
from argparse import Namespace
from queue import Empty
from unittest.mock import MagicMock, patch

import networkx as nx
import pytest

import dbt.compilation
import dbt.config
import dbt.exceptions
import dbt.graph.cli as graph_cli
import dbt.graph.selector as graph_selector
import dbt.parser
import dbt.parser.manifest
import dbt.utils
import dbt_common.exceptions
from dbt import tracking
from dbt.adapters.factory import register_adapter, reset_adapters
from dbt.adapters.postgres import Plugin as PostgresPlugin
from dbt.cli.flags import convert_config
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.files import FileHash, FilePath, SourceFile
from dbt.contracts.graph.manifest import MacroManifest, ManifestStateCheck
from dbt.contracts.project import ProjectFlags
from dbt.events.logging import setup_event_logger
from dbt.flags import get_flags, set_from_args
from dbt.graph import NodeSelector, parse_difference
from dbt.mp_context import get_mp_context
from dbt.node_types import NodeType
from tests.unit.utils import (
    config_from_parts_or_dicts,
    generate_name_macros,
    inject_plugin,
)
from tests.unit.utils.manifest import make_manifest, make_model

set_from_args(Namespace(WARN_ERROR=False), None)


def _get_graph():
    integer_graph = nx.balanced_tree(2, 2, nx.DiGraph())

    package_mapping = {
        i: "m." + ("X" if i % 2 == 0 else "Y") + "." + letter
        for (i, letter) in enumerate(string.ascii_lowercase)
    }

    # Edges: [(X.a, Y.b), (X.a, X.c), (Y.b, Y.d), (Y.b, X.e), (X.c, Y.f), (X.c, X.g)]
    return graph_selector.Graph(nx.relabel_nodes(integer_graph, package_mapping))


def _get_manifest(graph):
    nodes = {}
    for unique_id in graph:
        fqn = unique_id.split(".")
        node = MagicMock(
            unique_id=unique_id,
            fqn=fqn,
            package_name=fqn[0],
            tags=[],
            resource_type=NodeType.Model,
            empty=False,
            config=MagicMock(enabled=True),
            is_versioned=False,
        )
        nodes[unique_id] = node

    nodes["m.X.a"].tags = ["abc"]
    nodes["m.Y.b"].tags = ["abc", "bcef"]
    nodes["m.X.c"].tags = ["abc", "bcef"]
    nodes["m.Y.d"].tags = []
    nodes["m.X.e"].tags = ["efg", "bcef"]
    nodes["m.Y.f"].tags = ["efg", "bcef"]
    nodes["m.X.g"].tags = ["efg"]
    return MagicMock(nodes=nodes)


@pytest.fixture
def graph():
    return _get_graph()


@pytest.fixture
def mock_manifest(graph):
    return _get_manifest(graph)


def id_macro(arg):
    if isinstance(arg, str):
        return arg
    try:
        return "_".join(arg)
    except TypeError:
        return arg


run_specs = [
    # include by fqn
    (["X.a"], [], {"m.X.a"}),
    # include by tag
    (["tag:abc"], [], {"m.X.a", "m.Y.b", "m.X.c"}),
    # exclude by tag
    (["*"], ["tag:abc"], {"m.Y.d", "m.X.e", "m.Y.f", "m.X.g"}),
    # tag + fqn
    (["tag:abc", "a"], [], {"m.X.a", "m.Y.b", "m.X.c"}),
    (["tag:abc", "d"], [], {"m.X.a", "m.Y.b", "m.X.c", "m.Y.d"}),
    # multiple node selection across packages
    (["X.a", "b"], [], {"m.X.a", "m.Y.b"}),
    (["X.a+"], ["b"], {"m.X.a", "m.X.c", "m.Y.d", "m.X.e", "m.Y.f", "m.X.g"}),
    # children
    (["X.c+"], [], {"m.X.c", "m.Y.f", "m.X.g"}),
    (["X.a+1"], [], {"m.X.a", "m.Y.b", "m.X.c"}),
    (["X.a+"], ["tag:efg"], {"m.X.a", "m.Y.b", "m.X.c", "m.Y.d"}),
    # parents
    (["+Y.f"], [], {"m.X.c", "m.Y.f", "m.X.a"}),
    (["1+Y.f"], [], {"m.X.c", "m.Y.f"}),
    # childrens parents
    (["@X.c"], [], {"m.X.a", "m.X.c", "m.Y.f", "m.X.g"}),
    # multiple selection/exclusion
    (["tag:abc", "tag:bcef"], [], {"m.X.a", "m.Y.b", "m.X.c", "m.X.e", "m.Y.f"}),
    (["tag:abc", "tag:bcef"], ["tag:efg"], {"m.X.a", "m.Y.b", "m.X.c"}),
    (["tag:abc", "tag:bcef"], ["tag:efg", "a"], {"m.Y.b", "m.X.c"}),
    # intersections
    (["a,a"], [], {"m.X.a"}),
    (["+c,c+"], [], {"m.X.c"}),
    (["a,b"], [], set()),
    (["tag:abc,tag:bcef"], [], {"m.Y.b", "m.X.c"}),
    (["*,tag:abc,a"], [], {"m.X.a"}),
    (["a,tag:abc,*"], [], {"m.X.a"}),
    (["tag:abc,tag:bcef"], ["c"], {"m.Y.b"}),
    (["tag:bcef,tag:efg"], ["tag:bcef,@b"], {"m.Y.f"}),
    (["tag:bcef,tag:efg"], ["tag:bcef,@a"], set()),
    (["*,@a,+b"], ["*,tag:abc,tag:bcef"], {"m.X.a"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], [], {"m.X.a", "m.Y.b", "m.X.c", "m.X.e", "m.Y.f"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], ["e"], {"m.X.a", "m.Y.b", "m.X.c", "m.Y.f"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], ["e"], {"m.X.a", "m.Y.b", "m.X.c", "m.Y.f"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], ["e", "f"], {"m.X.a", "m.Y.b", "m.X.c"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], ["tag:abc,tag:bcef"], {"m.X.a", "m.X.e", "m.Y.f"}),
    (["tag:bcef,tag:efg", "*,tag:abc"], ["tag:abc,tag:bcef", "tag:abc,a"], {"m.X.e", "m.Y.f"}),
]


@pytest.mark.parametrize("include,exclude,expected", run_specs, ids=id_macro)
def test_run_specs(include, exclude, expected, graph, mock_manifest):
    selector = graph_selector.NodeSelector(graph, mock_manifest)
    spec = graph_cli.parse_difference(include, exclude)
    selected, _ = selector.select_nodes(spec)

    assert selected == expected


param_specs = [
    ("a", False, None, False, None, "fqn", "a", False),
    ("+a", True, None, False, None, "fqn", "a", False),
    ("256+a", True, 256, False, None, "fqn", "a", False),
    ("a+", False, None, True, None, "fqn", "a", False),
    ("a+256", False, None, True, 256, "fqn", "a", False),
    ("+a+", True, None, True, None, "fqn", "a", False),
    ("16+a+32", True, 16, True, 32, "fqn", "a", False),
    ("@a", False, None, False, None, "fqn", "a", True),
    ("a.b", False, None, False, None, "fqn", "a.b", False),
    ("+a.b", True, None, False, None, "fqn", "a.b", False),
    ("256+a.b", True, 256, False, None, "fqn", "a.b", False),
    ("a.b+", False, None, True, None, "fqn", "a.b", False),
    ("a.b+256", False, None, True, 256, "fqn", "a.b", False),
    ("+a.b+", True, None, True, None, "fqn", "a.b", False),
    ("16+a.b+32", True, 16, True, 32, "fqn", "a.b", False),
    ("@a.b", False, None, False, None, "fqn", "a.b", True),
    ("a.b.*", False, None, False, None, "fqn", "a.b.*", False),
    ("+a.b.*", True, None, False, None, "fqn", "a.b.*", False),
    ("256+a.b.*", True, 256, False, None, "fqn", "a.b.*", False),
    ("a.b.*+", False, None, True, None, "fqn", "a.b.*", False),
    ("a.b.*+256", False, None, True, 256, "fqn", "a.b.*", False),
    ("+a.b.*+", True, None, True, None, "fqn", "a.b.*", False),
    ("16+a.b.*+32", True, 16, True, 32, "fqn", "a.b.*", False),
    ("@a.b.*", False, None, False, None, "fqn", "a.b.*", True),
    ("tag:a", False, None, False, None, "tag", "a", False),
    ("+tag:a", True, None, False, None, "tag", "a", False),
    ("256+tag:a", True, 256, False, None, "tag", "a", False),
    ("tag:a+", False, None, True, None, "tag", "a", False),
    ("tag:a+256", False, None, True, 256, "tag", "a", False),
    ("+tag:a+", True, None, True, None, "tag", "a", False),
    ("16+tag:a+32", True, 16, True, 32, "tag", "a", False),
    ("@tag:a", False, None, False, None, "tag", "a", True),
    ("source:a", False, None, False, None, "source", "a", False),
    ("source:a+", False, None, True, None, "source", "a", False),
    ("source:a+1", False, None, True, 1, "source", "a", False),
    ("source:a+32", False, None, True, 32, "source", "a", False),
    ("@source:a", False, None, False, None, "source", "a", True),
]


@pytest.mark.parametrize(
    "spec,parents,parents_depth,children,children_depth,filter_type,filter_value,childrens_parents",
    param_specs,
    ids=id_macro,
)
def test_parse_specs(
    spec,
    parents,
    parents_depth,
    children,
    children_depth,
    filter_type,
    filter_value,
    childrens_parents,
):
    parsed = graph_selector.SelectionCriteria.from_single_spec(spec)
    assert parsed.parents == parents
    assert parsed.parents_depth == parents_depth
    assert parsed.children == children
    assert parsed.children_depth == children_depth
    assert parsed.method == filter_type
    assert parsed.value == filter_value
    assert parsed.childrens_parents == childrens_parents


invalid_specs = [
    "@a+",
    "@a.b+",
    "@a.b*+",
    "@tag:a+",
    "@source:a+",
]


@pytest.mark.parametrize("invalid", invalid_specs, ids=lambda k: str(k))
def test_invalid_specs(invalid):
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
        graph_selector.SelectionCriteria.from_single_spec(invalid)


class TestCompiler:
    def test_single_model(self, runtime_config: RuntimeConfig):
        model = make_model(pkg="pkg", name="model_one", code="SELECT * FROM events")
        manifest = make_manifest(nodes=[model])

        compiler = dbt.compilation.Compiler(config=runtime_config)
        linker = compiler.compile(manifest)

        assert linker.nodes() == {model.unique_id}
        assert linker.edges() == set()

    def test_two_models_simple_ref(self, runtime_config: RuntimeConfig):
        model_one = make_model(pkg="pkg", name="model_one", code="SELECT * FROM events")
        model_two = make_model(
            pkg="pkg",
            name="model_two",
            code="SELECT * FROM {{ref('model_one')}}",
            refs=[model_one],
        )
        models = [model_one, model_two]
        manifest = make_manifest(nodes=models)

        compiler = dbt.compilation.Compiler(config=runtime_config)
        linker = compiler.compile(manifest)

        expected_nodes = [model.unique_id for model in models]
        assert linker.nodes() == set(expected_nodes)
        assert list(linker.edges()) == [tuple(expected_nodes)]


class TestNodeSelector:
    def test_dependency_list(self, runtime_config: RuntimeConfig):
        model_one = make_model(pkg="pkg", name="model_one", code="SELECT * FROM events")
        model_two = make_model(
            pkg="pkg",
            name="model_two",
            code="SELECT * FROM {{ref('model_one')}}",
            refs=[model_one],
        )
        model_three = make_model(
            pkg="pkg",
            name="model_three",
            code="""
                SELECT * FROM {{ ref("model_1") }}
                union all
                SELECT * FROM {{ ref("model_2") }}
            """,
            refs=[model_one, model_two],
        )
        model_four = make_model(
            pkg="pkg",
            name="model_four",
            code="SELECT * FROM {{ref('model_three')}}",
            refs=[model_three],
        )
        models = [model_one, model_two, model_three, model_four]
        manifest = make_manifest(nodes=models)

        # Get the graph
        compiler = dbt.compilation.Compiler(runtime_config)
        graph = compiler.compile(manifest)

        # Create the selector and get the queue
        selector = NodeSelector(graph, manifest)
        queue = selector.get_graph_queue(
            parse_difference(
                None,
                None,
            )
        )

        for model in models:
            assert not queue.empty()
            got = queue.get(block=False)
            assert got.unique_id == model.unique_id
            with pytest.raises(Empty):
                queue.get(block=False)
            queue.mark_done(got.unique_id)
        assert queue.empty()


class GraphTest(unittest.TestCase):
    def tearDown(self):
        self.mock_filesystem_search.stop()
        self.load_state_check.stop()
        self.load_source_file_patcher.stop()
        reset_adapters()

    def setUp(self):
        # create various attributes
        self.graph_result = None
        tracking.do_not_track()
        self.profile = {
            "outputs": {
                "test": {
                    "type": "postgres",
                    "threads": 4,
                    "host": "thishostshouldnotexist",
                    "port": 5432,
                    "user": "root",
                    "pass": "password",
                    "dbname": "dbt",
                    "schema": "dbt_test",
                }
            },
            "target": "test",
        }
        self.macro_manifest = MacroManifest(
            {n.unique_id: n for n in generate_name_macros("test_models_compile")}
        )
        self.mock_models = []  # used by filesystem_searcher

        # Create file filesystem searcher
        self.filesystem_search = patch("dbt.parser.read_files.filesystem_search")

        def mock_filesystem_search(project, relative_dirs, extension, ignore_spec):
            if "sql" not in extension:
                return []
            if "models" not in relative_dirs:
                return []
            return [model.path for model in self.mock_models]

        self.mock_filesystem_search = self.filesystem_search.start()
        self.mock_filesystem_search.side_effect = mock_filesystem_search

        # Create the Manifest.state_check patcher
        @patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                project_env_vars_hash=FileHash.from_contents(""),
                profile_env_vars_hash=FileHash.from_contents(""),
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = patch(
            "dbt.parser.manifest.ManifestLoader.build_manifest_state_check"
        )
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        # Create the source file patcher
        self.load_source_file_patcher = patch("dbt.parser.read_files.load_source_file")
        self.mock_source_file = self.load_source_file_patcher.start()

        def mock_load_source_file(path, parse_file_type, project_name, saved_files):
            for sf in self.mock_models:
                if sf.path == path:
                    source_file = sf
            source_file.project_name = project_name
            source_file.parse_file_type = parse_file_type
            return source_file

        self.mock_source_file.side_effect = mock_load_source_file

        # Create hookparser source file patcher
        self.load_source_file_manifest_patcher = patch("dbt.parser.manifest.load_source_file")
        self.mock_source_file_manifest = self.load_source_file_manifest_patcher.start()

        def mock_load_source_file_manifest(path, parse_file_type, project_name, saved_files):
            return []

        self.mock_source_file_manifest.side_effect = mock_load_source_file_manifest

    def get_config(self, extra_cfg=None):
        if extra_cfg is None:
            extra_cfg = {}

        cfg = {
            "name": "test_models_compile",
            "version": "0.1",
            "profile": "test",
            "project-root": os.path.abspath("."),
            "config-version": 2,
        }
        cfg.update(extra_cfg)

        config = config_from_parts_or_dicts(project=cfg, profile=self.profile)
        set_from_args(Namespace(), ProjectFlags())
        flags = get_flags()
        setup_event_logger(flags)
        object.__setattr__(flags, "PARTIAL_PARSE", False)
        for arg_name, args_param_value in vars(flags).items():
            args_param_value = convert_config(arg_name, args_param_value)
            object.__setattr__(config.args, arg_name.upper(), args_param_value)
            object.__setattr__(config.args, arg_name.lower(), args_param_value)

        return config

    def get_compiler(self, project):
        return dbt.compilation.Compiler(project)

    def use_models(self, models):
        for k, v in models.items():
            path = FilePath(
                searched_path="models",
                project_root=os.path.normcase(os.getcwd()),
                relative_path="{}.sql".format(k),
                modification_time=0.0,
            )
            # FileHash can't be empty or 'search_key' will be None
            source_file = SourceFile(path=path, checksum=FileHash.from_contents("abc"))
            source_file.contents = v
            self.mock_models.append(source_file)

    def load_manifest(self, config):
        inject_plugin(PostgresPlugin)
        register_adapter(config, get_mp_context())
        loader = dbt.parser.manifest.ManifestLoader(config, {config.project_name: config})
        loader.manifest.macros = self.macro_manifest.macros
        loader.load()
        return loader.manifest

    # TODO move / delete: This is testing partial parsing, not the selector nor compiler
    def test__partial_parse(self):
        config = self.get_config()

        manifest = self.load_manifest(config)

        # we need a loader to compare the two manifests
        loader = dbt.parser.manifest.ManifestLoader(config, {config.project_name: config})
        loader.manifest = manifest.deepcopy()

        is_partial_parsable, _ = loader.is_partial_parsable(manifest)
        self.assertTrue(is_partial_parsable)
        manifest.metadata.dbt_version = "0.0.1a1"
        is_partial_parsable, _ = loader.is_partial_parsable(manifest)
        self.assertFalse(is_partial_parsable)
        manifest.metadata.dbt_version = "99999.99.99"
        is_partial_parsable, _ = loader.is_partial_parsable(manifest)
        self.assertFalse(is_partial_parsable)
