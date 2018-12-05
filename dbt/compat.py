import codecs
import json
import warnings
import decimal

try:
    import cdecimal
except ImportError:
    DECIMALS = (decimal.Decimal,)
else:
    DECIMALS = (decimal.Decimal, cdecimal.Decimal)

WHICH_PYTHON = None

try:
    basestring
    WHICH_PYTHON = 2
except NameError:
    WHICH_PYTHON = 3

if WHICH_PYTHON == 2:
    basestring = basestring
    bigint = long
else:
    basestring = str
    bigint = int

if WHICH_PYTHON == 2:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer
    from Queue import PriorityQueue
else:
    from http.server import SimpleHTTPRequestHandler
    from socketserver import TCPServer
    from queue import PriorityQueue


def to_unicode(s):
    if WHICH_PYTHON == 2:
        return unicode(s)
    else:
        return str(s)


def to_string(s):
    if WHICH_PYTHON == 2:
        if isinstance(s, unicode):
            return s
        elif isinstance(s, basestring):
            return to_unicode(s)
        else:
            return to_unicode(str(s))
    else:
        if isinstance(s, basestring):
            return s
        else:
            return str(s)


def write_file(path, s):
    if WHICH_PYTHON == 2:
        with codecs.open(path, 'w', encoding='utf-8') as f:
            return f.write(to_string(s))
    else:
        with open(path, 'w') as f:
            return f.write(to_string(s))


def suppress_warnings():
    # in python 2, ResourceWarnings don't exist.
    # in python 3, suppress ResourceWarnings about unclosed sockets, as the
    # bigquery library never closes them.
    if WHICH_PYTHON == 3:
        warnings.filterwarnings("ignore", category=ResourceWarning,
                                message="unclosed.*<socket.socket.*>")
