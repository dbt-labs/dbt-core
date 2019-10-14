from typing import Optional

import dbt.exceptions
import dbt.utils
from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import ColumnInfo
from dbt.config import Project


def docs(node, manifest, current_project: str, column_name=None):
    """Return a function that will process `doc()` references in jinja, look
    them up in the manifest, and return the appropriate block contents.
    """
    def do_docs(*args: str):
        if len(args) == 1:
            doc_package_name = None
            doc_name = args[0]
        elif len(args) == 2:
            doc_package_name, doc_name = args
        else:
            dbt.exceptions.doc_invalid_args(node, args)

        target_doc = ParserUtils.resolve_doc(
            manifest, doc_name, doc_package_name, current_project,
            node.package_name
        )

        if target_doc is None:
            dbt.exceptions.doc_target_not_found(node, doc_name,
                                                doc_package_name)

        return target_doc.block_contents

    return do_docs


class ParserUtils:
    DISABLED = object()

    @classmethod
    def resolve_source(
        cls, manifest, target_source_name: Optional[str],
        target_table_name: Optional[str], current_project: str,
        node_package: str
    ):
        candidate_targets = [current_project, node_package, None]
        target_source = None
        for candidate in candidate_targets:
            target_source = manifest.find_source_by_name(
                target_source_name,
                target_table_name,
                candidate
            )
            if target_source is not None:
                return target_source

        return None

    @classmethod
    def resolve_ref(
        cls, manifest, target_model_name: Optional[str],
        target_model_package: Optional[str], current_project: str,
        node_package: str
    ):
        if target_model_package is not None:
            return manifest.find_refable_by_name(
                target_model_name,
                target_model_package)

        target_model = None
        disabled_target = None

        # first pass: look for models in the current_project
        # second pass: look for models in the node's package
        # final pass: look for models in any package
        # todo: exclude the packages we have already searched. overriding
        # a package model in another package doesn't necessarily work atm
        candidates = [current_project, node_package, None]
        for candidate in candidates:
            target_model = manifest.find_refable_by_name(
                target_model_name,
                candidate)

            if target_model is not None and dbt.utils.is_enabled(target_model):
                return target_model

            # it's possible that the node is disabled
            if disabled_target is None:
                disabled_target = manifest.find_disabled_by_name(
                    target_model_name, candidate
                )

        if disabled_target is not None:
            return cls.DISABLED
        return None

    @classmethod
    def resolve_doc(
        cls, manifest, target_doc_name: str, target_doc_package: Optional[str],
        current_project: str, node_package: str
    ):
        """Resolve the given documentation. This follows the same algorithm as
        resolve_ref except the is_enabled checks are unnecessary as docs are
        always enabled.
        """
        if target_doc_package is not None:
            return manifest.find_docs_by_name(target_doc_name,
                                              target_doc_package)

        candidate_targets = [current_project, node_package, None]
        target_doc = None
        for candidate in candidate_targets:
            target_doc = manifest.find_docs_by_name(target_doc_name, candidate)
            if target_doc is not None:
                break
        return target_doc

    @classmethod
    def _get_node_column(cls, node, column_name):
        """Given a ParsedNode, add some fields that might be missing. Return a
        reference to the dict that refers to the given column, creating it if
        it doesn't yet exist.
        """
        if column_name in node.columns:
            column = node.columns[column_name]
        else:
            node.columns[column_name] = ColumnInfo(name=column_name)
            node.columns[column_name] = column

        return column

    @classmethod
    def process_docs_for_node(cls, manifest, current_project: str, node):
        for docref in node.docrefs:
            column_name = docref.column_name

            if column_name is None:
                obj = node
            else:
                obj = cls._get_node_column(node, column_name)

            context = {
                'doc': docs(node, manifest, current_project, column_name),
            }

            raw = obj.description or ''
            # At this point, we know that our documentation string has a
            # 'docs("...")' pointing at it. We want to render it.
            obj.description = dbt.clients.jinja.get_rendered(raw, context)

    @classmethod
    def process_docs_for_source(cls, manifest, current_project: str, source):
        context = {
            'doc': docs(source, manifest, current_project),
        }
        table_description = source.description
        source_description = source.source_description
        table_description = dbt.clients.jinja.get_rendered(table_description,
                                                           context)
        source_description = dbt.clients.jinja.get_rendered(source_description,
                                                            context)
        source.description = table_description
        source.source_description = source_description

        for column in source.columns.values():
            column_desc = column.description
            column_desc = dbt.clients.jinja.get_rendered(column_desc, context)
            column.description = column_desc

    @classmethod
    def process_docs(cls, manifest, current_project: str):
        for node in manifest.nodes.values():
            if node.resource_type == NodeType.Source:
                cls.process_docs_for_source(manifest, current_project, node)
            else:
                cls.process_docs_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def process_refs_for_node(cls, manifest, current_project: str, node):
        """Given a manifest and a node in that manifest, process its refs"""
        for ref in node.refs:
            target_model = None
            target_model_name = None
            target_model_package = None

            if len(ref) == 1:
                target_model_name = ref[0]
            elif len(ref) == 2:
                target_model_package, target_model_name = ref

            target_model = cls.resolve_ref(
                manifest,
                target_model_name,
                target_model_package,
                current_project,
                node.package_name)

            if target_model is None or target_model is cls.DISABLED:
                # This may raise. Even if it doesn't, we don't want to add
                # this node to the graph b/c there is no destination node
                node.config.enabled = False
                dbt.utils.invalid_ref_fail_unless_test(
                    node, target_model_name, target_model_package,
                    disabled=(target_model is cls.DISABLED)
                )

                continue

            target_model_id = target_model.unique_id

            node.depends_on.nodes.append(target_model_id)
            # TODO: I think this is extraneous, node should already be the same
            # as manifest.nodes[node.unique_id] (we're mutating node here, not
            # making a new one)
            manifest.update_node(node)

    @classmethod
    def process_refs(cls, manifest, current_project: str):
        # process_refs_for_node will mutate this
        all_nodes = list(manifest.nodes.values())
        for node in all_nodes:
            cls.process_refs_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def process_sources_for_node(cls, manifest, current_project: str, node):
        target_source = None
        for source_name, table_name in node.sources:
            target_source = cls.resolve_source(
                manifest,
                source_name,
                table_name,
                current_project,
                node.package_name)

            if target_source is None:
                # this folows the same pattern as refs
                node.config.enabled = False
                dbt.utils.invalid_source_fail_unless_test(
                    node,
                    source_name,
                    table_name)
                continue
            target_source_id = target_source.unique_id
            node.depends_on.nodes.append(target_source_id)
            manifest.update_node(node)

    @classmethod
    def process_sources(cls, manifest, current_project: str):
        all_nodes = list(manifest.nodes.values())
        for node in all_nodes:
            cls.process_sources_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def add_new_refs(cls, manifest, current_project: Project, node, macros):
        """Given a new node that is not in the manifest, copy the manifest and
        insert the new node into it as if it were part of regular ref
        processing
        """
        manifest = manifest.deepcopy()
        # it's ok for macros to silently override a local project macro name
        manifest.macros.update(macros)

        manifest.add_nodes({node.unique_id: node})
        cls.process_sources_for_node(
            manifest, current_project.project_name, node
        )
        cls.process_refs_for_node(manifest, current_project.project_name, node)
        cls.process_docs_for_node(manifest, current_project.project_name, node)
        return manifest
