from dbt.api import APIObject
from dbt.utils import deep_merge
from dbt.node_types import NodeType

import dbt.clients.jinja

from dbt.contracts.graph.unparsed import UNPARSED_NODE_CONTRACT, \
    UNPARSED_MACRO_CONTRACT

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


HOOK_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'sql': {
            'type': 'string',
        },
        'transaction': {
            'type': 'boolean',
        },
        'index': {
            'type': 'integer',
        }
    },
    'required': ['sql', 'transaction', 'index'],
}


CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': True,
    'properties': {
        'enabled': {
            'type': 'boolean',
        },
        'materialized': {
            'type': 'string',
        },
        'post-hook': {
            'type': 'array',
            'items': HOOK_CONTRACT,
        },
        'pre-hook': {
            'type': 'array',
            'items': HOOK_CONTRACT,
        },
        'vars': {
            'type': 'object',
            'additionalProperties': True,
        },
        'quoting': {
            'type': 'object',
            'additionalProperties': True,
        },
        'column_types': {
            'type': 'object',
            'additionalProperties': True,
        },
    },
    'required': [
        'enabled', 'materialized', 'post-hook', 'pre-hook', 'vars',
        'quoting', 'column_types'
    ]
}


PARSED_NODE_CONTRACT = deep_merge(
    UNPARSED_NODE_CONTRACT,
    {
        'properties': {
            'unique_id': {
                'type': 'string',
                'minLength': 1,
            },
            'fqn': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
            'schema': {
                'type': 'string',
                'description': (
                    'The actual database string that this will build into.'
                )
            },
            'alias': {
                'type': 'string',
                'description': (
                    'The name of the relation that this will build into'
                )
            },
            'refs': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'description': (
                        'The list of arguments passed to a single ref call.'
                    ),
                },
                'description': (
                    'The list of call arguments, one list of arguments per '
                    'call.'
                )
            },
            'depends_on': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'nodes': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'description': (
                                'A node unique ID that this depends on.'
                            )
                        }
                    },
                    'macros': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'description': (
                                'A macro unique ID that this depends on.'
                            )
                        }
                    },
                },
                'description': (
                    'A list of unique IDs for nodes and macros that this '
                    'node depends upon.'
                ),
                'required': ['nodes', 'macros'],
            },
            # TODO: move this into a class property.
            'empty': {
                'type': 'boolean',
                'description': 'True if the SQL is empty',
            },
            'config': CONFIG_CONTRACT,
            'tags': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
        },
        'required': UNPARSED_NODE_CONTRACT['required'] + [
            'unique_id', 'fqn', 'schema', 'refs', 'depends_on', 'empty',
            'config', 'tags', 'alias',
        ]
    }
)


PARSED_NODES_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the parsed nodes, stored by their unique IDs.'
    ),
    'patternProperties': {
        '.*': PARSED_NODE_CONTRACT
    },
}


PARSED_MACRO_CONTRACT = deep_merge(
    UNPARSED_MACRO_CONTRACT,
    {
        # This is required for the 'generator' field to work.
        # TODO: fix before release
        'additionalProperties': True,
        'properties': {
            'name': {
                'type': 'string',
                'description': (
                    'Name of this node. For models, this is used as the '
                    'identifier in the database.'),
                'minLength': 1,
                'maxLength': 127,
            },
            'resource_type': {
                'enum': [
                    NodeType.Macro,
                    NodeType.Operation,
                ],
            },
            'unique_id': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 255,
            },
            'tags': {
                'description': (
                    'An array of arbitrary strings to use as tags.'
                ),
                'type': 'array',
                'items': {
                    'type': 'string',
                },
            },
            'depends_on': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'macros': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 255,
                            'description': 'A single macro unique ID.'
                        }
                    }
                },
                'description': 'A list of all macros this macro depends on.',
                'required': ['macros'],
            },
        },
        'required': UNPARSED_MACRO_CONTRACT['required'] + [
            'resource_type', 'unique_id', 'tags', 'depends_on', 'name',
        ]
    }
)

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

PARSED_MANIFEST_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'The full parsed manifest of the graph, with both the required nodes'
        ' and required macros.'
    ),
    'properties': {
        'nodes': PARSED_NODES_CONTRACT,
        'macros': PARSED_MACROS_CONTRACT,
    },
    'required': ['nodes', 'macros'],
}


class ParsedNode(APIObject):
    SCHEMA = PARSED_NODE_CONTRACT

    def __init__(self, agate_table=None, **kwargs):
        self.agate_table = agate_table
        super(ParsedNode, self).__init__(**kwargs)

    @property
    def depends_on_nodes(self):
        """Return the list of node IDs that this node depends on."""
        return self._contents['depends_on']['nodes']

    def to_dict(self):
        """Similar to 'serialize', but tacks the agate_table attribute in too.

        Why we need this:
            - networkx demands that the attr_dict it gets (the node) be a dict
                or subclass and does not respect the abstract Mapping class
            - many jinja things access the agate_table attribute (member) of
                the node dict.
            - the nodes are passed around between those two contexts in a way
                that I don't quite have clear enough yet.
        """
        ret = self.serialize()
        # note: not a copy/deep copy.
        ret['agate_table'] = self.agate_table
        return ret


class ParsedMacro(APIObject):
    SCHEMA = PARSED_MACRO_CONTRACT

    def __init__(self, template=None, **kwargs):
        self.template = template
        super(ParsedMacro, self).__init__(**kwargs)

    @property
    def generator(self):
        """
        Returns a function that can be called to render the macro results.
        """
        # TODO: we can generate self.template from the other properties
        # available in this class. should we just generate this here?
        return dbt.clients.jinja.macro_generator(
            self.template, self._contents)


class ParsedNodes(APIObject):
    SCHEMA = PARSED_NODES_CONTRACT


class Hook(APIObject):
    SCHEMA = HOOK_CONTRACT


class ParsedMacros(APIObject):
    SCHEMA = PARSED_MACROS_CONTRACT


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
    return forward_edges, backward_edges


class ParsedManifest(object):
    """The final result of parsing all macros and nodes in a graph."""
    def __init__(self, nodes, macros):
        """The constructor. nodes and macros are dictionaries mapping unique
        IDs to ParsedNode and ParsedMacro objects, respectively.
        """
        self.nodes = nodes
        self.macros = macros

    def serialize(self):
        """Convert the parsed manifest to a nested dict structure that we can
        safely serialize to JSON.
        """
        forward_edges, backward_edges = build_edges(self.nodes.values())

        return {
            'nodes': {k: v.serialize() for k, v in self.nodes.items()},
            'macros': {k: v.serialize() for k, v in self.macros.items()},
            'parent_map': backward_edges,
            'child_map': forward_edges,
        }

    def _find_by_name(self, name, package, subgraph, nodetype):
        """

        Find a node by its given name in the appropraite sugraph.
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

    def find_operation_by_name(self, name, package):
        return self._find_by_name(name, package, 'macros',
                                  [NodeType.Operation])

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
            'nodes': {k: v.to_dict() for k, v in self.nodes.items()},
            'macros': self.macros,
        }
