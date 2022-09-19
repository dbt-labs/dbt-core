# Use of betterproto package for generating Python message classes

## Context
We are providing proto definitions for our structured logging messages, and as part of that we need to also have Python classes for use in our Python codebase

### Options

#### Google protobuf package

You can use the google protobuf package to generate Python "classes", using the protobuf compiler, "protoc" with the "--python_out" option.

* It's not readable. There are no identifiable classes in the output.
* A "class" is generated using a metaclass when it is used.
* There are lots of warnings about not subclassing the generated "classes".
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



## Status
Implementing

# Consequences
