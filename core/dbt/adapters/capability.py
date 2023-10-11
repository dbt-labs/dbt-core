from collections import UserDict
from dataclasses import dataclass
from enum import Enum
from typing import Optional, TYPE_CHECKING


class Capability(str, Enum):
    """Enumeration of optional adapter features which can be probed using BaseAdapter.has_feature()"""

    SchemaMetadataByRelations = "SchemaMetadataByRelations"
    """Indicates efficient support for retrieving schema metadata for a list of relations, rather than always retrieving
    all the relations in a schema."""

    TableLastModifiedMetadata = "TableLastModifiedMetadata"
    """Indicates support for determining the time of the last table modification by querying database metadata."""


class Support(str, Enum):
    Unknown = "Unknown"
    """The adapter has not declared whether this capability is a feature of the underlying DBMS."""

    Unsupported = "Unsupported"
    """This capability is not possible with the underlying DBMS, so the adapter does not implement related macros."""

    NotImplemented = "NotImplemented"
    """This capability is available in the underlying DBMS, but support has not yet been implemented in the adapter."""

    Versioned = "Versioned"
    """Some versions of the DBMS supported by the adapter support this capability and the adapter has implemented any
    macros needed to use it."""

    Full = "Full"
    """All versions of the DBMS supported by the adapter support this capability and the adapter has implemented any
    macros needed to use it."""


@dataclass
class CapabilitySupport:
    support: Support
    first_version: Optional[str] = None

    def __bool__(self):
        return self.support == Support.Versioned or self.support == Support.Full


# Make python 3.8 happy with this generic type, remove when we support 3.9+
if TYPE_CHECKING:
    CapabilityUserDict = UserDict[Capability, CapabilitySupport]
else:
    CapabilityUserDict = UserDict


class CapabilityDict(CapabilityUserDict):
    def __missing__(self, key):
        return CapabilitySupport(support=Support.Unknown)
