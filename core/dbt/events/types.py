
from typing import NamedTuple, Union


# The following classes represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. The transformation to these forms will live in outside
# functions.
#
# Until we drop support for Python 3.6 we must use NamedTuples over
# frozen dataclasses.


# base class used for type-level membership checking only
class ParsingProgressBase(NamedTuple):
    pass


class ParsingStart(ParsingProgressBase):
    pass


class ParsingCompiling(ParsingProgressBase):
    pass


class ParsingWritingManifest(ParsingProgressBase):
    pass


class ParsingDone(ParsingProgressBase):
    pass


# using a union instead of inheritance means that this set cannot
# be extended outside this file, and thus mypy can do exhaustiveness
# checks for us.

# type for parsing progress events
ParsingProgress = Union[
    ParsingStart,
    ParsingCompiling,
    ParsingWritingManifest,
    ParsingDone
]


# base class used for type-level membership checking only
class ManifestProgressBase(NamedTuple):
    pass


class ManifestDependenciesLoaded(ManifestProgressBase):
    pass


class ManifestLoaderCreated(ManifestProgressBase):
    pass


class ManifestLoaded(ManifestProgressBase):
    pass


class ManifestChecked(ManifestProgressBase):
    pass


class ManifestFlatGraphBuilt(ManifestProgressBase):
    pass


# type for manifest loading progress events
ManifestProgress = Union[
    ManifestDependenciesLoaded,
    ManifestLoaderCreated,
    ManifestLoaded,
    ManifestChecked,
    ManifestFlatGraphBuilt
]

# top-level event type for all events that go to the CLI
CliEvent = Union[
    ParsingProgress,
    ManifestProgress
]

# top-level event type for all events
Event = Union[
    ParsingProgress,
    ManifestProgress
]
