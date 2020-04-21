from typing import (
    Iterable,
    Dict,
    Optional,
)
from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import (
    UnpatchedSourceDefinition,
    ParsedSourceDefinition,
    ParsedSchemaTestNode,
)
from dbt.contracts.graph.unparsed import (
    UnparsedSourceDefinition,
    SourcePatch,
    SourceTablePatch,
    UnparsedSourceTableDefinition,
)

from dbt.parser.schemas import SchemaParser, ParserRef
from dbt.parser.results import ParseResult


class SourcePatcher:
    def __init__(
        self,
        results: ParseResult,
        root_project: RuntimeConfig,
    ) -> None:
        self.results = results
        self.root_project = root_project
        self.macro_manifest = Manifest.from_macros(
            macros=self.results.macros,
            files=self.results.files
        )
        self.schema_parsers: Dict[str, SchemaParser] = {}

    def patch_source(
        self,
        unpatched: UnpatchedSourceDefinition,
        patch: Optional[SourcePatch],
    ) -> UnpatchedSourceDefinition:

        source_dct = unpatched.source.to_dict()
        table_dct = unpatched.table.to_dict()

        source_table_patch: Optional[SourceTablePatch] = None

        if patch is not None:
            source_table_patch = patch.get_table_named(unpatched.table.name)
            source_dct.update(patch.to_patch_dict())

        if source_table_patch is not None:
            table_dct.update(source_table_patch.to_patch_dict())

        source = UnparsedSourceDefinition.from_dict(source_dct)
        table = UnparsedSourceTableDefinition.from_dict(table_dct)
        return unpatched.replace(source=source, table=table)

    def parse_source_docs(self, block: UnpatchedSourceDefinition) -> ParserRef:
        refs = ParserRef()
        for column in block.columns:
            description = column.description
            data_type = column.data_type
            meta = column.meta
            refs.add(column, description, data_type, meta)
        return refs

    def get_schema_parser_for(self, package_name: str) -> 'SchemaParser':
        if package_name in self.schema_parsers:
            schema_parser = self.schema_parsers[package_name]
        else:
            all_projects = self.root_project.load_dependencies()
            project = all_projects[package_name]
            schema_parser = SchemaParser(
                self.results, project, self.root_project, self.macro_manifest
            )
            self.schema_parsers[package_name] = schema_parser
        return schema_parser

    def get_source_tests(
        self, target: UnpatchedSourceDefinition
    ) -> Iterable[ParsedSchemaTestNode]:
        schema_parser = self.get_schema_parser_for(target.package_name)
        for test, column in target.get_tests():
            yield schema_parser.parse_source_test(
                target=target,
                test=test,
                column=column,
            )

    def construct_sources(self) -> Dict[str, ParsedSourceDefinition]:
        sources: Dict[str, ParsedSourceDefinition] = {}

        # given the UnpatchedSourceDefinition and SourcePatches, combine them
        # to make a beautiful baby ParsedSourceDefinition.
        for unique_id, unpatched in self.results.sources.items():
            key = (unpatched.package_name, unpatched.source.name)
            patch: Optional[SourcePatch] = self.results.source_patches.get(key)
            patched = self.patch_source(unpatched, patch)
            # now use the patched UnpatchedSourceDefinition to extract test
            # data.
            for test in self.get_source_tests(patched):
                if test.config.enabled:
                    self.results.add_node_nofile(test)
                else:
                    self.results.add_disabled_nofile(test)

            schema_parser = self.get_schema_parser_for(unpatched.package_name)
            parsed = schema_parser.parse_source(patched)
            if parsed.config.enabled:
                sources[unique_id] = parsed
            else:
                self.results.add_disabled_nofile(parsed)
        return sources


def patch_sources(
    results: ParseResult,
    root_project: RuntimeConfig,
) -> Dict[str, ParsedSourceDefinition]:
    """Patch all the sources found in the results. Updates results.disabled and
    results.nodes.

    Return a dict of ParsedSourceDefinitions, suitable for use in
    manifest.sources.
    """
    patcher = SourcePatcher(results, root_project)
    return patcher.construct_sources()
