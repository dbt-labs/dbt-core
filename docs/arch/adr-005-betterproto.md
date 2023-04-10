# Use of betterproto package for generating Python message classes

## Context
We are providing proto definitions for our structured logging messages, and as part of that we need to also have Python classes for use in our Python codebase

### Options, August 30, 2022

#### Google protobuf package

You can use the google protobuf package to generate Python "classes", using the protobuf compiler, "protoc" with the "--python_out" option.

* It's not readable. There are no identifiable classes in the output.
* A "class" is generated using a metaclass when it is used.
* You can't subclass the generated classes, which don't act much like Python objects
* Since you can't put defaults or methods of any kind in these classes, and you can't subclass them, they aren't very usable in Python.
* Generated classes are not easily importable
* Serialization is via external utilities.
* Mypy and flake8 totally fail so you have to exclude the generated files in the pre-commit config.

#### betterproto package

* It generates readable "dataclass" classes.
* You can subclass the generated classes. (Though you still can't add additional attributes. But if we really needed to we might be able to modify the source code to do so.)
* Integrates much more easily with our codebase.
* Serialization (to_dict and to_json) is built in.
* Mypy and flake8 work on generated files.

* Additional benefits listed: [betterproto](https://github.com/danielgtaylor/python-betterproto)


## Revisited, March 21, 2023

We are switching away from using betterproto because of the following reasons:
* betterproto only suppports Optional fields in a beta release
* betterproto has had only beta releases for a few years
* betterproto doesn't support Struct, which we really need
* betterproto started changing our message names to be more "pythonic"

Steps taken to mitigate the drawbacks of Google protobuf from above:
* We are using a wrapping class around the logging events to enable a constructor that looks more like a Python constructor, as long as only keyword arguments are used.
* The generated file is skipped in the pre-commit config
* We can live with the awkward interfaces. It's just code.

Advantages of Google protobuf:
* Message can be constructed from a dictionary of all message values. With betterproto you had to pre-construct nested message objects, which kind of forced you to sprinkle generated message objects through the codebase.
* The Struct support works really well
* Type errors are caught much earlier and more consistently. Betterproto would accept fields of the wrong types, which was sometimes caught on serialization to a dictionary, and sometimes not until serialized to a binary string. Sometimes not at all.

Disadvantages of Google protobuf:
* You can't just set nested message objects, you have to use CopyFrom. Just code, again.
* If you try to stringify parts of the message (like in the constructed event message) it outputs in a bizarre "user friendly" format. Really bad for Struct, in particular. 
* Python messages aren't really Python. You can't expect them to *act* like normal Python objects. So they are best kept isolated to the logging code only.
* As part of the not-really-Python, you can't use added classes to act like flags (Cache, NoFile, etc), since you can only use the bare generated message to construct other messages.
