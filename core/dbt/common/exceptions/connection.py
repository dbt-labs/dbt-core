class ConnectionError(Exception):
    """
    There was a problem with the connection that returned a bad response,
    timed out, or resulted in a file that is corrupt.
    """

    pass
