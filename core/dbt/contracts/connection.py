from dbt.api.object import APIObject
from dbt.contracts.common import named_property


CONNECTION_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'type': {
            'type': 'string',
            # valid python identifiers only
            'pattern': r'^[A-Za-z_][A-Za-z0-9_]+$',
        },
        'name': {
            'type': ['null', 'string'],
        },
        'state': {
            'enum': ['init', 'open', 'closed', 'fail'],
        },
        'transaction_open': {
            'type': 'boolean',
        },
        # we can't serialize this so we can't require it as part of the
        # contract.
        # 'handle': {
        #     'type': ['null', 'object'],
        # },
        # credentials are validated separately by the adapter packages
        'credentials': {
            'description': (
                'The credentials object here should match the connection type.'
            ),
            'type': 'object',
            'additionalProperties': True,
        }
    },
    'required': [
        'type', 'name', 'state', 'transaction_open', 'credentials'
    ],
}


class Connection(APIObject):
    SCHEMA = CONNECTION_CONTRACT

    def __init__(self, credentials, *args, **kwargs):
        # we can't serialize handles
        self._handle = kwargs.pop('handle')
        super().__init__(credentials=credentials.serialize(), *args, **kwargs)
        # this will validate itself in its own __init__.
        self._credentials = credentials

    @property
    def credentials(self):
        return self._credentials

    @property
    def handle(self):
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value

    name = named_property('name', 'The name of this connection')
    state = named_property('state', 'The state of the connection')
    transaction_open = named_property(
        'transaction_open',
        'True if there is an open transaction, False otherwise.'
    )
