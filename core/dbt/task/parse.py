# This task is intended to be used for diagnosis, development and
# performance analysis.
# It separates out the parsing flows for easier logging and
# debugging.
# To store cProfile performance data, execute with the '-r'
# flag and an output file: dbt -r dbt.cprof parse.
# Use a visualizer such as snakeviz to look at the output:
# snakeviz dbt.cprof
from dbt.task.base import ConfiguredTask
from dbt.adapters.factory import get_adapter
from dbt.parser.manifest import Manifest, ManifestLoader, _check_manifest
from dbt.logger import DbtProcessState
from dbt.clients.system import write_file
from dbt.events.types import (
    ManifestDependenciesLoaded,
    ManifestLoaderCreated,
    ManifestLoaded,
    ManifestChecked,
    ManifestFlatGraphBuilt,
    ParsingStart,
    ParsingCompiling,
    ParsingWritingManifest,
    ParsingDone,
    ReportPerformancePath,
)
from dbt.events.functions import fire_event
from dbt.graph import Graph
import time
from typing import Optional
import os
import json
import dbt.utils

MANIFEST_FILE_NAME = "manifest.json"
PERF_INFO_FILE_NAME = "perf_info.json"
PARSING_STATE = DbtProcessState("parsing")


class ParseTask(ConfiguredTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.manifest: Optional[Manifest] = None
        self.graph: Optional[Graph] = None
        self.loader: Optional[ManifestLoader] = None

    def write_manifest(self):
        path = os.path.join(self.config.target_path, MANIFEST_FILE_NAME)
        self.manifest.write(path)

    def write_perf_info(self):
        path = os.path.join(self.config.target_path, PERF_INFO_FILE_NAME)
        write_file(path, json.dumps(self.loader._perf_info, cls=dbt.utils.JSONEncoder, indent=4))
        fire_event(ReportPerformancePath(path=path))

    # This method takes code that normally exists in other files
    # and pulls it in here, to simplify logging and make the
    # parsing flow-of-control easier to understand and manage,
    # with the downside that if changes happen in those other methods,
    # similar changes might need to be made here.
    # ManifestLoader.get_full_manifest
    # ManifestLoader.load
    # ManifestLoader.load_all

    def get_full_manifest(self):
        adapter = get_adapter(self.config)  # type: ignore
        root_config = self.config
        macro_hook = adapter.connections.set_query_header
        with PARSING_STATE:
            start_load_all = time.perf_counter()
            projects = root_config.load_dependencies()
            fire_event(ManifestDependenciesLoaded())
            loader = ManifestLoader(root_config, projects, macro_hook)
            fire_event(ManifestLoaderCreated())
            manifest = loader.load()
            fire_event(ManifestLoaded())
            _check_manifest(manifest, root_config)
            fire_event(ManifestChecked())
            manifest.build_flat_graph()
            fire_event(ManifestFlatGraphBuilt())
            loader._perf_info.load_all_elapsed = time.perf_counter() - start_load_all

        self.loader = loader
        self.manifest = manifest
        fire_event(ManifestLoaded())

    def compile_manifest(self):
        adapter = get_adapter(self.config)
        compiler = adapter.get_compiler()
        self.graph = compiler.compile(self.manifest)

    def run(self):
        fire_event(ParsingStart())
        self.get_full_manifest()
        if self.args.compile:
            fire_event(ParsingCompiling())
            self.compile_manifest()
        if self.args.write_manifest:
            fire_event(ParsingWritingManifest())
            self.write_manifest()

        self.write_perf_info()
        fire_event(ParsingDone())
