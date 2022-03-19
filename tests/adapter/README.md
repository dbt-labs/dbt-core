# dbt-tests-adapter

This is where we store the adapter tests that will be used by
plugin repos. It should be included in the dbt-core and plugin
repos by installing using pip.

Tests in this repo will be packaged as classes, with a base class
that can be imported by adapter test repositories. Tests that might
need to be overridden by a plugin will be separated into separate
test methods or test classes as they are discovered.

This plugin is installed in the dbt-core repo by pip install -e tests/adapter,
which is included in the editable-requirements.txt file.

The dbt.tests.adapter.basic tests originally came from the earlier
dbt-adapter-tests repository. Additional test directories will be
added as they are converted from dbt-core integration tests, so that
they can be used in adapter test suites without copying and pasting.

This is packaged as a plugin using a python namespace package so it
cannot have an __init__.py file in the part of the hierarchy to which it
needs to be attached.
