from dbt.api import APIObject
from dbt.contracts.graph.unparsed import UNPARSED_NODE_CONTRACT
from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT, \
    PARSED_MACRO_CONTRACT, PARSED_DOCUMENTATION_CONTRACT, \
    PARSED_SOURCE_DEFINITION_CONTRACT
from dbt.contracts.graph.compiled import COMPILED_NODE_CONTRACT, CompiledNode
from dbt.exceptions import raise_duplicate_resource_name
from dbt.node_types import NodeType
from dbt.logger import GLOBAL_LOGGER as logger
from dbt import tracking
import dbt.utils

# We allow either parsed or compiled nodes, or parsed sources, as some
# 'compile()' calls in the runner actually just return the original parsed
# node they were given.
COMPILE_RESULT_NODE_CONTRACT = {
    'anyOf': [
        PARSED_NODE_CONTRACT,
        COMPILED_NODE_CONTRACT,
        PARSED_SOURCE_DEFINITION_CONTRACT,
    ]
}


COMPILE_RESULT_NODES_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the parsed nodes, stored by their unique IDs.'
    ),
    'patternProperties': {
        '.*': COMPILE_RESULT_NODE_CONTRACT
    },
}


PARSED_MACROS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the parsed macros, stored by their unique IDs.'
    ),
    'patternProperties': {
        '.*': PARSED_MACRO_CONTRACT
    },
}


PARSED_DOCUMENTATIONS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the parsed docs, stored by their uniqe IDs.'
    ),
    'patternProperties': {
        '.*': PARSED_DOCUMENTATION_CONTRACT,
    },
}


NODE_EDGE_MAP = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'A map of node relationships',
    'patternProperties': {
        '.*': {
            'type': 'array',
            'items': {
                'type': 'string',
                'description': 'A node name',
            }
        }
    }
}


PARSED_MANIFEST_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'The full parsed manifest of the graph, with both the required nodes'
        ' and required macros.'
    ),
    'properties': {
        'nodes': COMPILE_RESULT_NODES_CONTRACT,
        'macros': PARSED_MACROS_CONTRACT,
        'docs': PARSED_DOCUMENTATIONS_CONTRACT,
        'disabled': {
            'type': 'array',
            'items': PARSED_NODE_CONTRACT,
            'description': 'An array of disabled nodes',
        },
        'generated_at': {
            'type': 'string',
            'format': 'date-time',
            'description': (
                'The time at which the manifest was generated'
            ),
        },
        'parent_map': NODE_EDGE_MAP,
        'child_map': NODE_EDGE_MAP,
        'metadata': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'project_id': {
                    'type': ('string', 'null'),
                    'description': (
                        'The anonymized ID of the project. Persists as long '
                        'as the project name stays the same.'
                    ),
                    'pattern': '[0-9a-f]{32}',
                },
                'user_id': {
                    'type': ('string', 'null'),
                    'description': (
                        'The user ID assigned by dbt. Persists per-user as '
                        'long as the user cookie file remains in place.'
                    ),
                    'pattern': (
                        '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-'
                        '[0-9a-f]{12}'
                    ),
                },
                'send_anonymous_usage_stats': {
                    'type': ('boolean', 'null'),
                    'description': (
                        'Whether or not to send anonymized usage statistics.'
                    ),
                },
            },
            'required': [
                'project_id', 'user_id', 'send_anonymous_usage_stats',
            ],
        },
    },
    'required': ['nodes', 'macros', 'docs', 'generated_at', 'metadata'],
}


class CompileResultNode(CompiledNode):
    SCHEMA = COMPILE_RESULT_NODE_CONTRACT


def _sort_values(dct):
    """Given a dictionary, sort each value. This makes output deterministic,
    which helps for tests.
    """
    return {k: sorted(v) for k, v in dct.items()}


def build_edges(nodes):
    """Build the forward and backward edges on the given list of ParsedNodes
    and return them as two separate dictionaries, each mapping unique IDs to
    lists of edges.
    """
    backward_edges = {}
    # pre-populate the forward edge dict for simplicity
    forward_edges = {node.unique_id: [] for node in nodes}
    for node in nodes:
        backward_edges[node.unique_id] = node.depends_on_nodes[:]
        for unique_id in node.depends_on_nodes:
            forward_edges[unique_id].append(node.unique_id)
    return _sort_values(forward_edges), _sort_values(backward_edges)


class Manifest(APIObject):
    SCHEMA = PARSED_MANIFEST_CONTRACT
    """The manifest for the full graph, after parsing and during compilation.
    Nodes may be either ParsedNodes or CompiledNodes or a mix, depending upon
    the current state of the compiler. Macros will always be ParsedMacros and
    docs will always be ParsedDocumentations.
    """
    def __init__(self, nodes, macros, docs, generated_at, disabled,
                 config=None):
        """The constructor. nodes and macros are dictionaries mapping unique
        IDs to ParsedNode/CompiledNode and ParsedMacro objects, respectively.
        docs is a dictionary mapping unique IDs to ParsedDocumentation objects.
        generated_at is a text timestamp in RFC 3339 format.
        disabled is a list of disabled FQNs (as strings).
        """
        metadata = self.get_metadata(config)
        self.nodes = nodes
        self.macros = macros
        self.docs = docs
        self.generated_at = generated_at
        self.metadata = metadata
        self.disabled = disabled
        super(Manifest, self).__init__()

    @staticmethod
    def get_metadata(config):
        project_id = None
        user_id = None
        send_anonymous_usage_stats = None

        if config is not None:
            project_id = config.hashed_name()

        if tracking.active_user is not None:
            user_id = tracking.active_user.id
            send_anonymous_usage_stats = not tracking.active_user.do_not_track

        return {
            'project_id': project_id,
            'user_id': user_id,
            'send_anonymous_usage_stats': send_anonymous_usage_stats,
        }

    def serialize(self):
        """Convert the parsed manifest to a nested dict structure that we can
        safely serialize to JSON.
        """
        forward_edges, backward_edges = build_edges(self.nodes.values())

        return {
            'nodes': {k: v.serialize() for k, v in self.nodes.items()},
            'macros': {k: v.serialize() for k, v in self.macros.items()},
            'docs': {k: v.serialize() for k, v in self.docs.items()},
            'parent_map': backward_edges,
            'child_map': forward_edges,
            'generated_at': self.generated_at,
            'metadata': self.metadata,
            'disabled': [v.serialize() for v in self.disabled],
        }

    def find_disabled_by_name(self, name, package=None):
        return dbt.utils.find_in_list_by_name(self.disabled, name, package,
                                              NodeType.refable())

    def _find_by_name(self, name, package, subgraph, nodetype):
        """

        Find a node by its given name in the appropriate sugraph. If package is
        None, all pacakges will be searched.
        nodetype should be a list of NodeTypes to accept.
        """
        if subgraph == 'nodes':
            search = self.nodes
        elif subgraph == 'macros':
            search = self.macros
        else:
            raise NotImplementedError(
                'subgraph search for {} not implemented'.format(subgraph)
            )
        return dbt.utils.find_in_subgraph_by_name(
            search,
            name,
            package,
            nodetype)

    def find_docs_by_name(self, name, package=None):
        for unique_id, doc in self.docs.items():
            parts = unique_id.split('.')
            if len(parts) != 2:
                msg = "documentation names cannot contain '.' characters"
                dbt.exceptions.raise_compiler_error(msg, doc)

            found_package, found_node = parts

            if (name == found_node and package in {None, found_package}):
                return doc
        return None

    def find_macro_by_name(self, name, package):
        """Find a macro in the graph by its name and package name, or None for
        any package.
        """
        return self._find_by_name(name, package, 'macros', [NodeType.Macro])

    def find_refable_by_name(self, name, package):
        """Find any valid target for "ref()" in the graph by its name and
        package name, or None for any package.
        """
        return self._find_by_name(name, package, 'nodes', NodeType.refable())

    def find_source_by_name(self, source_name, table_name, package):
        """Find any valid target for "source()" in the graph by its name and
        package name, or None for any package.
        """
        name = '{}.{}'.format(source_name, table_name)
        return self._find_by_name(name, package, 'nodes', [NodeType.Source])

    def get_materialization_macro(self, materialization_name,
                                  adapter_type=None):
        macro_name = dbt.utils.get_materialization_macro_name(
            materialization_name=materialization_name,
            adapter_type=adapter_type,
            with_prefix=False)

        macro = self.find_macro_by_name(
            macro_name,
            None)

        if adapter_type not in ('default', None) and macro is None:
            macro_name = dbt.utils.get_materialization_macro_name(
                materialization_name=materialization_name,
                adapter_type='default',
                with_prefix=False)
            macro = self.find_macro_by_name(
                macro_name,
                None)

        return macro

    def get_resource_fqns(self):
        resource_fqns = {}
        for unique_id, node in self.nodes.items():
            if node.resource_type == NodeType.Source:
                continue  # sources have no FQNs and can't be configured
            resource_type_plural = node.resource_type + 's'
            if resource_type_plural not in resource_fqns:
                resource_fqns[resource_type_plural] = set()
            resource_fqns[resource_type_plural].add(tuple(node.fqn))

        return resource_fqns

    def _filter_subgraph(self, subgraph, predicate):
        """
        Given a subgraph of the manifest, and a predicate, filter
        the subgraph using that predicate. Generates a list of nodes.
        """
        to_return = []

        for unique_id, item in subgraph.items():
            if predicate(item):
                to_return.append(item)

        return to_return

    def _model_matches_schema_and_table(self, schema, table, model):
        if model.resource_type == NodeType.Source:
            return (model.schema.lower() == schema.lower() and
                    model.identifier.lower() == table.lower())
        return (model.schema.lower() == schema.lower() and
                model.alias.lower() == table.lower())

    def get_unique_ids_for_schema_and_table(self, schema, table):
        """
        Given a schema and table, find matching models, and return
        their unique_ids. A schema and table may have more than one
        match if the relation matches both a source and a seed, for instance.
        """
        def predicate(model):
            return self._model_matches_schema_and_table(schema, table, model)

        matching = list(self._filter_subgraph(self.nodes, predicate))
        return [match.get('unique_id') for match in matching]

    def add_nodes(self, new_nodes):
        """Add the given dict of new nodes to the manifest."""
        for unique_id, node in new_nodes.items():
            if unique_id in self.nodes:
                raise_duplicate_resource_name(node, self.nodes[unique_id])
            self.nodes[unique_id] = node

    def patch_nodes(self, patches):
        """Patch nodes with the given dict of patches. Note that this consumes
        the input!
        """
        # because we don't have any mapping from node _names_ to nodes, and we
        # only have the node name in the patch, we have to iterate over all the
        # nodes looking for matching names. We could use _find_by_name if we
        # were ok with doing an O(n*m) search (one nodes scan per patch)
        for node in self.nodes.values():
            if node.resource_type != NodeType.Model:
                continue
            patch = patches.pop(node.name, None)
            if not patch:
                continue
            node.patch(patch)

        # log debug-level warning about nodes we couldn't find
        if patches:
            for patch in patches.values():
                # since patches aren't nodes, we can't use the existing
                # target_not_found warning
                logger.debug((
                    'WARNING: Found documentation for model "{}" which was '
                    'not found or is disabled').format(patch.name)
                )

    def to_flat_graph(self):
        """Convert the parsed manifest to the 'flat graph' that the compiler
        expects.

        Kind of hacky note: everything in the code is happy to deal with
        macros as ParsedMacro objects (in fact, it's been changed to require
        that), so those can just be returned without any work. Nodes sadly
        require a lot of work on the compiler side.

        Ideally in the future we won't need to have this method.
        """
        return {
            'nodes': {k: v.to_shallow_dict() for k, v in self.nodes.items()},
            'macros': self.macros,
        }

    def __getattr__(self, name):
        raise AttributeError("'{}' object has no attribute '{}'".format(
            type(self).__name__, name)
        )

    def get_used_schemas(self, resource_types=None):
        return frozenset({
            (node.database, node.schema)
            for node in self.nodes.values()
            if not resource_types or node.resource_type in resource_types
        })

    def get_used_databases(self):
        return frozenset(node.database for node in self.nodes.values())
