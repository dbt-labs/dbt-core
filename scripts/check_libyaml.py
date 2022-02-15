#!/usr/bin/env python
try:
    from yaml import CLoader as Loader, CSafeLoader as SafeLoader, CDumper as Dumper  # noqa: F401
except ImportError:
    from yaml import Loader, SafeLoader, Dumper  # noqa: F401

if Loader.__name__ == "CLoader":
    print("libyaml is working")
elif Loader.__name__ == "Loader":
    print("libyaml is not working")
    print("Check the python executable and pyyaml for libyaml support")
