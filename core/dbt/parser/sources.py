from pathlib import Path
from typing import (
    Iterable,
    Dict,
    Optional,
    Set,
)
from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest, SourceKey
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
from dbt.exceptions import warn_or_error

from dbt.parser.schemas import SchemaParser, ParserRef
from dbt.parser.results import ParseResult
from dbt import ui


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
        self.patches_used: Dict[SourceKey, Set[str]] = {}
        self.sources: Dict[str, ParsedSourceDefinition] = {}

    def patch_source(
        self,
        unpatched: UnpatchedSourceDefinition,
        patch: Optional[SourcePatch],
    ) -> UnpatchedSourceDefinition:
        if patch is None:
            return unpatched

        source_dct = unpatched.source.to_dict()
        table_dct = unpatched.table.to_dict()
        patch_path: Optional[Path] = None

        source_table_patch: Optional[SourceTablePatch] = None

        if patch is not None:
            source_table_patch = patch.get_table_named(unpatched.table.name)
            source_dct.update(patch.to_patch_dict())
            patch_path = patch.path

        if source_table_patch is not None:
            table_dct.update(source_table_patch.to_patch_dict())

        source = UnparsedSourceDefinition.from_dict(source_dct)
        table = UnparsedSourceTableDefinition.from_dict(table_dct)
        return unpatched.replace(
            source=source, table=table, patch_path=patch_path
        )

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

    def get_patch_for(
        self,
        unpatched: UnpatchedSourceDefinition,
    ) -> Optional[SourcePatch]:
        key = (unpatched.package_name, unpatched.source.name)
        patch: Optional[SourcePatch] = self.results.source_patches.get(key)
        if patch is None:
            return None
        if key not in self.patches_used:
            # mark the key as used
            self.patches_used[key] = set()
        if patch.get_table_named(unpatched.table.name) is not None:
            self.patches_used[key].add(unpatched.table.name)
        return patch

    def construct_sources(self) -> None:
        # given the UnpatchedSourceDefinition and SourcePatches, combine them
        # to make a beautiful baby ParsedSourceDefinition.
        for unique_id, unpatched in self.results.sources.items():
            patch = self.get_patch_for(unpatched)

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
                self.sources[unique_id] = parsed
            else:
                self.results.add_disabled_nofile(parsed)

        self.warn_unused()

    def warn_unused(self) -> None:
        unused_tables: Dict[SourceKey, Optional[Set[str]]] = {}
        for patch in self.results.source_patches.values():
            key = (patch.overrides, patch.name)
            if key not in self.patches_used:
                unused_tables[key] = None
            elif patch.tables is not None:
                table_patches = {t.name for t in patch.tables}
                unused = table_patches - self.patches_used[key]
                # don't add unused tables, the
                if unused:
                    # because patches are required to be unique, we can safely
                    # write without looking
                    unused_tables[key] = unused

        if unused_tables:
            msg = self.get_unused_msg(unused_tables)
            warn_or_error(msg, log_fmt=ui.warning_tag('{}'))

    def get_unused_msg(
        self,
        unused_tables: Dict[SourceKey, Optional[Set[str]]],
    ) -> str:
        msg = [
            'During parsing, dbt encountered source overrides that had no '
            'target:',
        ]
        for key, table_names in unused_tables.items():
            patch = self.results.source_patches[key]
            patch_name = f'{patch.overrides}.{patch.name}'
            if table_names is None:
                msg.append(
                    f'  - Source {patch_name} (in {patch.path})'
                )
            else:
                for table_name in sorted(table_names):
                    msg.append(
                        f'  - Source table {patch_name}.{table_name} '
                        f'(in {patch.path})'
                    )
        msg.append('')
        return '\n'.join(msg)


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
    patcher.construct_sources()
    return patcher.sources
