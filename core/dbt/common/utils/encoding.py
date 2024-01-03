import datetime
import decimal
import hashlib
import json
from typing import Tuple, Type, Any

import jinja2
import sys

DECIMALS: Tuple[Type[Any], ...]
try:
    import cdecimal  # typing: ignore
except ImportError:
    DECIMALS = (decimal.Decimal,)
else:
    DECIMALS = (decimal.Decimal, cdecimal.Decimal)


def md5(string, charset="utf-8"):
    if sys.version_info >= (3, 9):
        return hashlib.md5(string.encode(charset), usedforsecurity=False).hexdigest()
    else:
        return hashlib.md5(string.encode(charset)).hexdigest()


class JSONEncoder(json.JSONEncoder):
    """A 'custom' json encoder that does normal json encoder things, but also
    handles `Decimal`s and `Undefined`s. Decimals can lose precision because
    they get converted to floats. Undefined's are serialized to an empty string
    """

    def default(self, obj):
        if isinstance(obj, DECIMALS):
            return float(obj)
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, jinja2.Undefined):
            return ""
        elif isinstance(obj, Exception):
            return repr(obj)
        elif hasattr(obj, "to_dict"):
            # if we have a to_dict we should try to serialize the result of
            # that!
            return obj.to_dict(omit_none=True)
        else:
            return super().default(obj)


class ForgivingJSONEncoder(JSONEncoder):
    def default(self, obj):
        # let dbt's default JSON encoder handle it if possible, fallback to
        # str()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)
