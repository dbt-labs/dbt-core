#!/usr/bin/env python
try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader  # noqa: F401
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import Dumper, Loader, SafeLoader  # noqa: F401

if Loader.__name__ == "CLoader":
    print("libyaml is working")
elif Loader.__name__ == "Loader":
    print("libyaml is not working")
    print("Check the python executable and pyyaml for libyaml support")
