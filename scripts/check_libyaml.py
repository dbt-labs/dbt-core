#!/usr/bin/env python
try:
    from yaml import (
        CLoader as Loader,
        CSafeLoader as SafeLoader,
        CDumper as Dumper
    )
except ImportError:
    from yaml import (
        Loader, SafeLoader, Dumper
    )

if Loader.__name__ == 'CLoader':
    print("libyaml is working")
elif Loader.__name__ == 'Loader':
    print("libyaml is not working")
    print("Check the python executable and pyyaml for libyaml support")
