from dbt.contracts.connection import update_connection_contract
from dbt.contracts.project import update_config_contract


def register_conection_contract(typename, connection):
    """Given an adapter type name and its connection type, register it with the
    various contracts.
    """
    update_connection_contract(typename, connection)
    update_config_contract(typename, connection)
