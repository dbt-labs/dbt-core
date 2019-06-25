
import dbt.exceptions
import dbt.utils
from dbt.node_types import NodeType


def docs(node, manifest, config, column_name=None):
    """Return a function that will process `doc()` references in jinja, look
    them up in the manifest, and return the appropriate block contents.
    """
    current_project = config.project_name

    def do_docs(*args):
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
    def resolve_source(cls, manifest, target_source_name,
                       target_table_name, current_project, node_package):
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
    def resolve_ref(cls, manifest, target_model_name, target_model_package,
                    current_project, node_package):
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
    def resolve_doc(cls, manifest, target_doc_name, target_doc_package,
                    current_project, node_package):
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
        if not hasattr(node, 'columns'):
            node.set('columns', {})

        if column_name in node.columns:
            column = node.columns[column_name]
        else:
            column = {'name': column_name, 'description': ''}
            node.columns[column_name] = column

        return column

    @classmethod
    def process_docs_for_node(cls, manifest, current_project, node):
        for docref in node.get('docrefs', []):
            column_name = docref.get('column_name')
            if column_name is None:
                description = node.get('description', '')
            else:
                column = cls._get_node_column(node, column_name)
                description = column.get('description', '')
            context = {
                'doc': docs(node, manifest, current_project, column_name),
            }

            # At this point, target_doc is a ParsedDocumentation, and we
            # know that our documentation string has a 'docs("...")'
            # pointing at it. We want to render it.
            description = dbt.clients.jinja.get_rendered(description,
                                                         context)
            # now put it back.
            if column_name is None:
                node.set('description', description)
            else:
                column['description'] = description

    @classmethod
    def process_docs_for_source(cls, manifest, current_project, source):
        context = {
            'doc': docs(source, manifest, current_project),
        }
        table_description = source.get('description', '')
        source_description = source.get('source_description', '')
        table_description = dbt.clients.jinja.get_rendered(table_description,
                                                           context)
        source_description = dbt.clients.jinja.get_rendered(source_description,
                                                            context)
        source.set('description', table_description)
        source.set('source_description', source_description)

    @classmethod
    def process_docs(cls, manifest, current_project):
        for node in manifest.nodes.values():
            if node.resource_type == NodeType.Source:
                cls.process_docs_for_source(manifest, current_project, node)
            else:
                cls.process_docs_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def process_refs_for_node(cls, manifest, current_project, node):
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
                node.get('package_name'))

            if target_model is None or target_model is cls.DISABLED:
                # This may raise. Even if it doesn't, we don't want to add
                # this node to the graph b/c there is no destination node
                node.config['enabled'] = False
                dbt.utils.invalid_ref_fail_unless_test(
                    node, target_model_name, target_model_package,
                    disabled=(target_model is cls.DISABLED)
                )

                continue

            target_model_id = target_model.get('unique_id')

            node.depends_on['nodes'].append(target_model_id)
            manifest.nodes[node['unique_id']] = node

    @classmethod
    def process_refs(cls, manifest, current_project):
        for node in manifest.nodes.values():
            cls.process_refs_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def process_sources_for_node(cls, manifest, current_project, node):
        target_source = None
        for source_name, table_name in node.sources:
            target_source = cls.resolve_source(
                manifest,
                source_name,
                table_name,
                current_project,
                node.get('package_name'))

            if target_source is None:
                # this folows the same pattern as refs
                node.config['enabled'] = False
                dbt.utils.invalid_source_fail_unless_test(
                    node,
                    source_name,
                    table_name)
                continue
            target_source_id = target_source.unique_id
            node.depends_on['nodes'].append(target_source_id)
            manifest.nodes[node['unique_id']] = node

    @classmethod
    def process_sources(cls, manifest, current_project):
        for node in manifest.nodes.values():
            cls.process_sources_for_node(manifest, current_project, node)
        return manifest

    @classmethod
    def add_new_refs(cls, manifest, current_project, node, macros):
        """Given a new node that is not in the manifest, copy the manifest and
        insert the new node into it as if it were part of regular ref
        processing
        """
        manifest = manifest.deepcopy(config=current_project)
        # it's ok for macros to silently override a local project macro name
        manifest.macros.update(macros)

        if node.unique_id in manifest.nodes:
            # this should be _impossible_ due to the fact that rpc calls get
            # a unique ID that starts with 'rpc'!
            raise dbt.exceptions.raise_duplicate_resource_name(
                manifest.nodes[node.unique_id], node
            )
        manifest.nodes[node.unique_id] = node
        cls.process_sources_for_node(manifest, current_project, node)
        cls.process_refs_for_node(manifest, current_project, node)
        cls.process_docs_for_node(manifest, current_project, node)
        return manifest
