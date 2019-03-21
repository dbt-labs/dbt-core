from dbt.api import APIObject
from dbt.utils import deep_merge
from dbt.node_types import NodeType

import dbt.clients.jinja

from dbt.contracts.graph.unparsed import UNPARSED_NODE_CONTRACT, \
    UNPARSED_MACRO_CONTRACT, UNPARSED_DOCUMENTATION_FILE_CONTRACT, \
    UNPARSED_BASE_CONTRACT, TIME_CONTRACT

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


# TODO: which of these do we _really_ support? or is it both?
HOOK_CONTRACT = {
    'anyOf': [
        {
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
            'required': ['sql', 'transaction'],
        },
        {
            'type': 'string',
        },
    ],
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
        'tags': {
            'anyOf': [
                {
                    'type': 'array',
                    'items': {
                        'type': 'string'
                    },
                },
                {
                    'type': 'string'
                }
            ]
        },
    },
    'required': [
        'enabled', 'materialized', 'post-hook', 'pre-hook', 'vars',
        'quoting', 'column_types', 'tags'
    ]
}


#  Note that description must be present, but may be empty.
COLUMN_INFO_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'Information about a single column in a model',
    'properties': {
        'name': {
            'type': 'string',
            'description': 'The column name',
        },
        'description': {
            'type': 'string',
            'description': 'A description of the column',
        },
    },
    'required': ['name', 'description'],
}


# Docrefs are not quite like regular references, as they indicate what they
# apply to as well as what they are referring to (so the doc package + doc
# name, but also the column name if relevant). This is because column
# descriptions are rendered separately from their models.
DOCREF_CONTRACT = {
    'type': 'object',
    'properties': {
        'documentation_name': {
            'type': 'string',
            'description': 'The name of the documentation block referred to',
        },
        'documentation_package': {
            'type': 'string',
            'description': (
                'If provided, the documentation package name referred to'
            ),
        },
        'column_name': {
            'type': 'string',
            'description': (
                'If the documentation refers to a column instead of the '
                'model, the column name should be set'
            ),
        },
    },
    'required': ['documentation_name', 'documentation_package']
}


HAS_FQN_CONTRACT = {
    'properties': {
        'fqn': {
            'type': 'array',
            'items': {
                'type': 'string',
            }
        },
    },
    'required': ['fqn'],
}


HAS_UNIQUE_ID_CONTRACT = {
    'properties': {
        'unique_id': {
            'type': 'string',
            'minLength': 1,
        },
    },
    'required': ['unique_id'],
}

CAN_REF_CONTRACT = {
    'properties': {
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
        'sources': {
            'type': 'array',
            'items': {
                'type': 'array',
                'description': (
                    'The list of arguments passed to a single source call.'
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
    },
    'required': ['refs', 'sources', 'depends_on'],
}


HAS_DOCREFS_CONTRACT = {
    'properties': {
        'docrefs': {
            'type': 'array',
            'items': DOCREF_CONTRACT,
        },
    },
}


HAS_DESCRIPTION_CONTRACT = {
    'properties': {
        'description': {
            'type': 'string',
            'description': 'A user-supplied description of the model',
        },
        'columns': {
            'type': 'object',
            'properties': {
                '.*': COLUMN_INFO_CONTRACT,
            },
        },
    },
    'required': ['description', 'columns'],
}

# does this belong inside another contract?
HAS_CONFIG_CONTRACT = {
    'properties': {
        'config': CONFIG_CONTRACT,
    },
    'required': ['config'],
}


COLUMN_TEST_CONTRACT = {
    'properties': {
        'column_name': {
            'type': 'string',
            'description': (
                'In tests parsed from a v2 schema, the column the test is '
                'associated with (if there is one)'
            )
        },
    }
}


HAS_RELATION_METADATA_CONTRACT = {
    'properties': {
        'database': {
            'type': 'string',
            'description': (
                'The actual database string that this will build into.'
            )
        },
        'schema': {
            'type': 'string',
            'description': (
                'The actual schema string that this will build into.'
            )
        },
    },
    'required': ['database', 'schema'],
}


PARSED_NODE_CONTRACT = deep_merge(
    UNPARSED_NODE_CONTRACT,
    HAS_UNIQUE_ID_CONTRACT,
    HAS_FQN_CONTRACT,
    CAN_REF_CONTRACT,
    HAS_DOCREFS_CONTRACT,
    HAS_DESCRIPTION_CONTRACT,
    HAS_CONFIG_CONTRACT,
    COLUMN_TEST_CONTRACT,
    HAS_RELATION_METADATA_CONTRACT,
    {
        'properties': {
            'alias': {
                'type': 'string',
                'description': (
                    'The name of the relation that this will build into'
                )
            },
            # TODO: move this into a class property.
            'empty': {
                'type': 'boolean',
                'description': 'True if the SQL is empty',
            },
            'tags': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
            # this is really nodes-only
            'patch_path': {
                'type': 'string',
                'description': (
                    'The path to the patch source if the node was patched'
                ),
            },
            'build_path': {
                'type': 'string',
                'description': (
                    'In seeds, the path to the source file used during build.'
                ),
            },
        },
        'required': ['empty', 'tags', 'alias'],
    }
)


class ParsedNode(APIObject):
    SCHEMA = PARSED_NODE_CONTRACT

    def __init__(self, agate_table=None, **kwargs):
        self.agate_table = agate_table
        kwargs.setdefault('columns', {})
        kwargs.setdefault('description', '')
        super(ParsedNode, self).__init__(**kwargs)

    @property
    def is_refable(self):
        return self.resource_type in NodeType.refable()

    @property
    def is_ephemeral(self):
        return self.get('config', {}).get('materialized') == 'ephemeral'

    @property
    def is_ephemeral_model(self):
        return self.is_refable and self.is_ephemeral

    @property
    def depends_on_nodes(self):
        """Return the list of node IDs that this node depends on."""
        return self.depends_on['nodes']

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

    def to_shallow_dict(self):
        ret = self._contents.copy()
        ret['agate_table'] = self.agate_table
        return ret

    def patch(self, patch):
        """Given a ParsedNodePatch, add the new information to the node."""
        # explicitly pick out the parts to update so we don't inadvertently
        # step on the model name or anything
        self._contents.update({
            'patch_path': patch.original_file_path,
            'description': patch.description,
            'columns': patch.columns,
            'docrefs': patch.docrefs,
        })
        # patches always trigger re-validation
        self.validate()

    def get_materialization(self):
        return self.config.get('materialized')

    @property
    def build_path(self):
        return self._contents.get('build_path')

    @build_path.setter
    def build_path(self, value):
        self._contents['build_path'] = value

    @property
    def database(self):
        return self._contents['database']

    @database.setter
    def database(self, value):
        self._contents['database'] = value

    @property
    def schema(self):
        return self._contents['schema']

    @schema.setter
    def schema(self, value):
        self._contents['schema'] = value

    @property
    def alias(self):
        return self._contents['alias']

    @alias.setter
    def alias(self, value):
        self._contents['alias'] = value

    @property
    def config(self):
        return self._contents['config']

    @config.setter
    def config(self, value):
        self._contents['config'] = value


# The parsed node update is only the 'patch', not the test. The test became a
# regular parsed node. Note that description and columns must be present, but
# may be empty.
PARSED_NODE_PATCH_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': 'A collection of values that can be set on a node',
    'properties': {
        'name': {
            'type': 'string',
            'description': 'The name of the node this modifies',
        },
        'description': {
            'type': 'string',
            'description': 'The description of the node to add',
        },
        'original_file_path': {
            'type': 'string',
            'description': (
                'Relative path to the originating file path for the patch '
                'from the project root'
            ),
        },
        'columns': {
            'type': 'object',
            'properties': {
                '.*': COLUMN_INFO_CONTRACT,
            }
        },
        'docrefs': {
            'type': 'array',
            'items': DOCREF_CONTRACT,
        }
    },
    'required': [
        'name', 'original_file_path', 'description', 'columns', 'docrefs'
    ],
}


class ParsedNodePatch(APIObject):
    SCHEMA = PARSED_NODE_PATCH_CONTRACT


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
        'required': [
            'resource_type', 'unique_id', 'tags', 'depends_on', 'name',
        ]
    }
)


class ParsedMacro(APIObject):
    SCHEMA = PARSED_MACRO_CONTRACT

    @property
    def generator(self):
        """
        Returns a function that can be called to render the macro results.
        """
        # TODO: we can generate self.template from the other properties
        # available in this class. should we just generate this here?
        return dbt.clients.jinja.macro_generator(self._contents)


# This is just the file + its ID
PARSED_DOCUMENTATION_CONTRACT = deep_merge(
    UNPARSED_DOCUMENTATION_FILE_CONTRACT,
    {
        'properties': {
            'name': {
                'type': 'string',
                'description': (
                    'Name of this node, as referred to by doc() references'
                ),
            },
            'unique_id': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 255,
                'description': (
                    'The unique ID of this node as stored in the manifest'
                ),
            },
            'block_contents': {
                'type': 'string',
                'description': 'The contents of just the docs block',
            },
        },
        'required': ['name', 'unique_id', 'block_contents'],
    }
)


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


class ParsedDocumentation(APIObject):
    SCHEMA = PARSED_DOCUMENTATION_CONTRACT


class Hook(APIObject):
    SCHEMA = HOOK_CONTRACT


FRESHNESS_CONTRACT = {
    'properties': {
        'loaded_at_field': {
            'type': ['null', 'string'],
            'description': 'The field to use as the "loaded at" timestamp',
        },
        'freshness': {
            'anyOf': [
                {'type': 'null'},
                {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'warn_after': TIME_CONTRACT,
                        'error_after': TIME_CONTRACT,
                    },
                },
            ],
        },
    },
}


QUOTING_CONTRACT = {
    'properties': {
        'quoting': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'database': {'type': 'boolean'},
                'schema': {'type': 'boolean'},
                'identifier': {'type': 'boolean'},
            },
        },
    },
    'required': ['quoting'],
}


PARSED_SOURCE_DEFINITION_CONTRACT = deep_merge(
    UNPARSED_BASE_CONTRACT,
    FRESHNESS_CONTRACT,
    QUOTING_CONTRACT,
    HAS_DESCRIPTION_CONTRACT,
    HAS_UNIQUE_ID_CONTRACT,
    HAS_DOCREFS_CONTRACT,
    HAS_RELATION_METADATA_CONTRACT,
    {
        'description': (
            'A source table definition, as parsed from the one provided in the'
            '"tables" subsection of the "sources" section of schema.yml'
        ),
        'properties': {
            'name': {
                'type': 'string',
                'description': (
                    'The name of this node, which is the name of the model it'
                    'refers to'
                ),
                'minLength': 1,
            },
            'source_name': {
                'type': 'string',
                'description': 'The reference name of the source definition',
                'minLength': 1,
            },
            'source_description': {
                'type': 'string',
                'description': 'The user-supplied description of the source',
            },
            'loader': {
                'type': 'string',
                'description': 'The user-defined loader for this source',
            },
            'identifier': {
                'type': 'string',
                'description': 'The identifier for the source table',
                'minLength': 1,
            },
            # the manifest search stuff really requires this, sadly
            'resource_type': {
                'enum': [NodeType.Source],
            },
        },
        # note that while required, loaded_at_field and freshness may be null
        'required': [
            'source_name', 'source_description', 'loaded_at_field', 'loader',
            'freshness', 'description', 'columns', 'docrefs', 'identifier',
        ],
    }
)


class ParsedSourceDefinition(APIObject):
    SCHEMA = PARSED_SOURCE_DEFINITION_CONTRACT
    is_ephemeral_model = False

    def to_shallow_dict(self):
        return self._contents.copy()

    # provide some emtpy/meaningless properties so these look more like
    # ParsedNodes
    @property
    def depends_on_nodes(self):
        return []

    @property
    def refs(self):
        return []

    @property
    def sources(self):
        return []

    @property
    def tags(self):
        return []

    @property
    def has_freshness(self):
        return bool(self.freshness) and self.loaded_at_field is not None
