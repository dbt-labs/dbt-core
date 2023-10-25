# This is useful for proto generated classes in particular, since
# the default for protobuf for strings is the empty string, so
# Optional[str] types don't work for generated Python classes.
from typing import Optional


def cast_to_str(string: Optional[str]) -> str:
    if string is None:
        return ""
    else:
        return string


def cast_to_int(integer: Optional[int]) -> int:
    if integer is None:
        return 0
    else:
        return integer


def cast_dict_to_dict_of_strings(dct):
    new_dct = {}
    for k, v in dct.items():
        new_dct[str(k)] = str(v)
    return new_dct
