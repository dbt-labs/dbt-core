
import dbt.utils


class ParserUtils(object):
    @classmethod
    def resolve_ref(cls, flat_graph, target_model_name, target_model_package,
                    current_project, node_package):

        if target_model_package is not None:
            return dbt.utils.find_refable_by_name(
                flat_graph,
                target_model_name,
                target_model_package)

        target_model = None

        # first pass: look for models in the current_project
        target_model = dbt.utils.find_refable_by_name(
            flat_graph,
            target_model_name,
            current_project)

        if target_model is not None and dbt.utils.is_enabled(target_model):
            return target_model

        # second pass: look for models in the node's package
        target_model = dbt.utils.find_refable_by_name(
            flat_graph,
            target_model_name,
            node_package)

        if target_model is not None and dbt.utils.is_enabled(target_model):
            return target_model

        # final pass: look for models in any package
        # todo: exclude the packages we have already searched. overriding
        # a package model in another package doesn't necessarily work atm
        return dbt.utils.find_refable_by_name(
            flat_graph,
            target_model_name,
            None)

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
    def process_docs(cls, manifest, current_project):
        for _, node in manifest.nodes.items():
            target_doc = None
            target_doc_name = None
            target_doc_package = None
            # TODO: attach 'docrefs' to ParsedNodePatches when generating the
            # docs in the schema parser, then make sure they get patched in.
        raise NotImplementedError('TODO: finish this')

    @classmethod
    def process_refs(cls, manifest, current_project):
        flat_graph = manifest.to_flat_graph()
        for _, node in manifest.nodes.items():
            target_model = None
            target_model_name = None
            target_model_package = None

            for ref in node.get('refs', []):
                if len(ref) == 1:
                    target_model_name = ref[0]
                elif len(ref) == 2:
                    target_model_package, target_model_name = ref

                target_model = cls.resolve_ref(
                    flat_graph,
                    target_model_name,
                    target_model_package,
                    current_project,
                    node.get('package_name'))

                if target_model is None:
                    # This may raise. Even if it doesn't, we don't want to add
                    # this node to the graph b/c there is no destination node
                    node.get('config', {})['enabled'] = False
                    dbt.utils.invalid_ref_fail_unless_test(
                            node, target_model_name, target_model_package)

                    continue

                target_model_id = target_model.get('unique_id')

                node['depends_on']['nodes'].append(target_model_id)
                flat_graph['nodes'][node['unique_id']] = node

        return manifest

