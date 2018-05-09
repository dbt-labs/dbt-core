import codecs

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
