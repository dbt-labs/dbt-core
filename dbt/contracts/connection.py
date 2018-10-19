import dbt.exceptions
from dbt.api.object import APIObject
from dbt.contracts.common import named_property
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


CREDENTIALS_MAPPING = {}


CONNECTION_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'type': {
            'enum': [],
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
        'credentials': {
            'description': (
                'The credentials object here should match the connection type.'
            ),
            'anyOf': [],
        }
    },
    'required': [
        'type', 'name', 'state', 'transaction_open', 'credentials'
    ],
}


def update_connection_contract(typename, connection):
    properties = CONNECTION_CONTRACT['properties']
    properties['type']['enum'].append(typename)
    properties['credentials']['anyOf'].append(connection.SCHEMA)
    CREDENTIALS_MAPPING[typename] = connection


def create_credentials(typename, credentials):
    if typename not in CREDENTIALS_MAPPING:
        dbt.exceptions.raise_unrecognized_credentials_type(
            typename, CREDENTIALS_MAPPING.keys()
        )
    cls = CREDENTIALS_MAPPING[typename]
    return cls(**credentials)


class Connection(APIObject):
    SCHEMA = CONNECTION_CONTRACT

    def __init__(self, credentials, *args, **kwargs):
        # this is a bit clunky but we deserialize and then reserialize for now
        if hasattr(credentials, 'serialize'):
            credentials = credentials.serialize()
        # we can't serialize handles
        self._handle = kwargs.pop('handle')
        super(Connection, self).__init__(credentials=credentials,
                                         *args, **kwargs)
        # this will validate itself in its own __init__.
        self._credentials = create_credentials(self.type,
                                               self._contents['credentials'])

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
