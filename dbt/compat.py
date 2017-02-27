# python 2+3 check for stringiness
try:
    basestring = basestring
except NameError:
    basestring = str
