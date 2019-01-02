
import dbt.exceptions
import dbt.utils


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

        target_doc_id = target_doc.unique_id

        return target_doc.block_contents

    return do_docs


class ParserUtils(object):
    DISABLED = object()

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
    def process_docs(cls, manifest, current_project):
        for _, node in manifest.nodes.items():
            target_doc = None
            target_doc_name = None
            target_doc_package = None
            for docref in node.get('docrefs', []):
                column_name = docref.get('column_name')
                if column_name is None:
                    description = node.get('description', '')
                else:
                    column = cls._get_node_column(node, column_name)
                    description = column.get('description', '')
                target_doc_name = docref['documentation_name']
                target_doc_package = docref['documentation_package']
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
        return manifest

    @classmethod
    def process_refs(cls, manifest, current_project):
        for _, node in manifest.nodes.items():
            target_model = None
            target_model_name = None
            target_model_package = None

            for ref in node.refs:
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

        return manifest
