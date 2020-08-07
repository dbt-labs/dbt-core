from dataclasses import dataclass, field
from typing import TypeVar, MutableMapping, Mapping, Union, List

from hologram import JsonSchemaMixin

from dbt.contracts.files import RemoteFile, FileHash, SourceFile
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.parsed import (
    HasUniqueID,
    ParsedAnalysisNode,
    ParsedDataTestNode,
    ParsedDocumentation,
    ParsedHookNode,
    ParsedMacro,
    ParsedMacroPatch,
    ParsedModelNode,
    ParsedNodePatch,
    ParsedRPCNode,
    ParsedSeedNode,
    ParsedSchemaTestNode,
    ParsedSnapshotNode,
    UnpatchedSourceDefinition,
)
from dbt.contracts.graph.unparsed import SourcePatch
from dbt.contracts.util import Writable, Replaceable, MacroKey, SourceKey
from dbt.exceptions import (
    raise_duplicate_resource_name, raise_duplicate_patch_name,
    raise_duplicate_macro_patch_name, CompilationException, InternalException,
    raise_compiler_error, raise_duplicate_source_patch_name
)
from dbt.node_types import NodeType
from dbt.ui import line_wrap_message
from dbt.version import __version__


# Parsers can return anything as long as it's a unique ID
ParsedValueType = TypeVar('ParsedValueType', bound=HasUniqueID)


def _check_duplicates(
    value: HasUniqueID, src: Mapping[str, HasUniqueID]
):
    if value.unique_id in src:
        raise_duplicate_resource_name(value, src[value.unique_id])


ManifestNodes = Union[
    ParsedAnalysisNode,
    ParsedDataTestNode,
    ParsedHookNode,
    ParsedModelNode,
    ParsedRPCNode,
    ParsedSchemaTestNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
]


def dict_field():
    return field(default_factory=dict)


@dataclass
class ParseResult(JsonSchemaMixin, Writable, Replaceable):
    vars_hash: FileHash
    profile_hash: FileHash
    project_hashes: MutableMapping[str, FileHash]
    nodes: MutableMapping[str, ManifestNodes] = dict_field()
    sources: MutableMapping[str, UnpatchedSourceDefinition] = dict_field()
    docs: MutableMapping[str, ParsedDocumentation] = dict_field()
    macros: MutableMapping[str, ParsedMacro] = dict_field()
    macro_patches: MutableMapping[MacroKey, ParsedMacroPatch] = dict_field()
    patches: MutableMapping[str, ParsedNodePatch] = dict_field()
    source_patches: MutableMapping[SourceKey, SourcePatch] = dict_field()
    files: MutableMapping[str, SourceFile] = dict_field()
    disabled: MutableMapping[str, List[CompileResultNode]] = dict_field()
    dbt_version: str = __version__

    def get_file(self, source_file: SourceFile) -> SourceFile:
        key = source_file.search_key
        if key is None:
            return source_file
        if key not in self.files:
            self.files[key] = source_file
        return self.files[key]

    def add_source(
        self, source_file: SourceFile, source: UnpatchedSourceDefinition
    ):
        # sources can't be overwritten!
        _check_duplicates(source, self.sources)
        self.sources[source.unique_id] = source
        self.get_file(source_file).sources.append(source.unique_id)

    def add_node_nofile(self, node: ManifestNodes):
        # nodes can't be overwritten!
        _check_duplicates(node, self.nodes)
        self.nodes[node.unique_id] = node

    def add_node(self, source_file: SourceFile, node: ManifestNodes):
        self.add_node_nofile(node)
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_disabled_nofile(self, node: CompileResultNode):
        if node.unique_id in self.disabled:
            self.disabled[node.unique_id].append(node)
        else:
            self.disabled[node.unique_id] = [node]

    def add_disabled(self, source_file: SourceFile, node: CompileResultNode):
        self.add_disabled_nofile(node)
        self.get_file(source_file).nodes.append(node.unique_id)

    def add_macro(self, source_file: SourceFile, macro: ParsedMacro):
        if macro.unique_id in self.macros:
            # detect that the macro exists and emit an error
            other_path = self.macros[macro.unique_id].original_file_path
            # subtract 2 for the "Compilation Error" indent
            # note that the line wrap eats newlines, so if you want newlines,
            # this is the result :(
            msg = line_wrap_message(
                f'''\
                dbt found two macros named "{macro.name}" in the project
                "{macro.package_name}".


                To fix this error, rename or remove one of the following
                macros:

                    - {macro.original_file_path}

                    - {other_path}
                ''',
                subtract=2
            )
            raise_compiler_error(msg)

        self.macros[macro.unique_id] = macro
        self.get_file(source_file).macros.append(macro.unique_id)

    def add_doc(self, source_file: SourceFile, doc: ParsedDocumentation):
        _check_duplicates(doc, self.docs)
        self.docs[doc.unique_id] = doc
        self.get_file(source_file).docs.append(doc.unique_id)

    def add_patch(
        self, source_file: SourceFile, patch: ParsedNodePatch
    ) -> None:
        # patches can't be overwritten
        if patch.name in self.patches:
            raise_duplicate_patch_name(patch, self.patches[patch.name])
        self.patches[patch.name] = patch
        self.get_file(source_file).patches.append(patch.name)

    def add_macro_patch(
        self, source_file: SourceFile, patch: ParsedMacroPatch
    ) -> None:
        # macros are fully namespaced
        key = (patch.package_name, patch.name)
        if key in self.macro_patches:
            raise_duplicate_macro_patch_name(patch, self.macro_patches[key])
        self.macro_patches[key] = patch
        self.get_file(source_file).macro_patches.append(key)

    def add_source_patch(
        self, source_file: SourceFile, patch: SourcePatch
    ) -> None:
        # source patches must be unique
        key = (patch.overrides, patch.name)
        if key in self.source_patches:
            raise_duplicate_source_patch_name(patch, self.source_patches[key])
        self.source_patches[key] = patch
        self.get_file(source_file).source_patches.append(key)

    def _get_disabled(
        self,
        unique_id: str,
        match_file: SourceFile,
    ) -> List[CompileResultNode]:
        if unique_id not in self.disabled:
            raise InternalException(
                'called _get_disabled with id={}, but it does not exist'
                .format(unique_id)
            )
        return [
            n for n in self.disabled[unique_id]
            if n.original_file_path == match_file.path.original_file_path
        ]

    def _process_node(
        self,
        node_id: str,
        source_file: SourceFile,
        old_file: SourceFile,
        old_result: 'ParseResult',
    ) -> None:
        """Nodes are a special kind of complicated - there can be multiple
        with the same name, as long as all but one are disabled.

        Only handle nodes where the matching node has the same resource type
        as the current parser.
        """
        source_path = source_file.path.original_file_path
        found: bool = False
        if node_id in old_result.nodes:
            old_node = old_result.nodes[node_id]
            if old_node.original_file_path == source_path:
                self.add_node(source_file, old_node)
                found = True

        if node_id in old_result.disabled:
            matches = old_result._get_disabled(node_id, source_file)
            for match in matches:
                self.add_disabled(source_file, match)
                found = True

        if not found:
            raise CompilationException(
                'Expected to find "{}" in cached "manifest.nodes" or '
                '"manifest.disabled" based on cached file information: {}!'
                .format(node_id, old_file)
            )

    def sanitized_update(
        self,
        source_file: SourceFile,
        old_result: 'ParseResult',
        resource_type: NodeType,
    ) -> bool:
        """Perform a santized update. If the file can't be updated, invalidate
        it and return false.
        """
        if isinstance(source_file.path, RemoteFile):
            return False

        old_file = old_result.get_file(source_file)
        for doc_id in old_file.docs:
            doc = _expect_value(doc_id, old_result.docs, old_file, "docs")
            self.add_doc(source_file, doc)

        for macro_id in old_file.macros:
            macro = _expect_value(
                macro_id, old_result.macros, old_file, "macros"
            )
            self.add_macro(source_file, macro)

        for source_id in old_file.sources:
            source = _expect_value(
                source_id, old_result.sources, old_file, "sources"
            )
            self.add_source(source_file, source)

        # because we know this is how we _parsed_ the node, we can safely
        # assume if it's disabled it was done by the project or file, and
        # we can keep our old data
        # the node ID could be in old_result.disabled AND in old_result.nodes.
        # In that case, we have to make sure the path also matches.
        for node_id in old_file.nodes:
            # cheat: look at the first part of the node ID and compare it to
            # the parser resource type. On a mismatch, bail out.
            if resource_type != node_id.split('.')[0]:
                continue
            self._process_node(node_id, source_file, old_file, old_result)

        patched = False
        for name in old_file.patches:
            patch = _expect_value(
                name, old_result.patches, old_file, "patches"
            )
            self.add_patch(source_file, patch)
            patched = True
        if patched:
            self.get_file(source_file).patches.sort()

        macro_patched = False
        for key in old_file.macro_patches:
            macro_patch = _expect_value(
                key, old_result.macro_patches, old_file, "macro_patches"
            )
            self.add_macro_patch(source_file, macro_patch)
            macro_patched = True
        if macro_patched:
            self.get_file(source_file).macro_patches.sort()

        return True

    def has_file(self, source_file: SourceFile) -> bool:
        key = source_file.search_key
        if key is None:
            return False
        if key not in self.files:
            return False
        my_checksum = self.files[key].checksum
        return my_checksum == source_file.checksum

    @classmethod
    def rpc(cls):
        # ugh!
        return cls(FileHash.empty(), FileHash.empty(), {})


K_T = TypeVar('K_T')
V_T = TypeVar('V_T')


def _expect_value(
    key: K_T, src: Mapping[K_T, V_T], old_file: SourceFile, name: str
) -> V_T:
    if key not in src:
        raise CompilationException(
            'Expected to find "{}" in cached "result.{}" based '
            'on cached file information: {}!'
            .format(key, name, old_file)
        )
    return src[key]
