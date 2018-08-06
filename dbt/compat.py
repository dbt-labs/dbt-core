import codecs
import json

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
else:
    from http.server import SimpleHTTPRequestHandler


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


def _open(path, mode):
    if WHICH_PYTHON == 2:
        return codecs.open(path, mode, encoding='utf-8')
    else:
        return open(path, 'w')


def write_file(path, s):
    with _open(path, 'w') as fp:
        return fp.write(to_string(s))


def write_json(path, data, **kwargs):
    with _open(path, 'w') as fp:
        json.dump(data, fp, **kwargs)
