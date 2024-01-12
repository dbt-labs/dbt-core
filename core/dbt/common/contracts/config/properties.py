from dataclasses import dataclass, field
from typing import Dict, Any

from dbt.common.dataclass_schema import ExtensibleDbtClassMixin


class AdditionalPropertiesMixin:
    """Make this class an extensible property.

    The underlying class definition must include a type definition for a field
    named '_extra' that is of type `Dict[str, Any]`.
    """

    ADDITIONAL_PROPERTIES = True

    # This takes attributes in the dictionary that are
    # not in the class definitions and puts them in an
    # _extra dict in the class
    @classmethod
    def __pre_deserialize__(cls, data):
        # dir() did not work because fields with
        # metadata settings are not found
        # The original version of this would create the
        # object first and then update extra with the
        # extra keys, but that won't work here, so
        # we're copying the dict so we don't insert the
        # _extra in the original data. This also requires
        # that Mashumaro actually build the '_extra' field
        cls_keys = cls._get_field_names()
        new_dict = {}
        for key, value in data.items():
            # The pre-hook/post-hook mess hasn't been converted yet... That happens in
            # the super().__pre_deserialize__ below...
            if key not in cls_keys and key not in ["_extra", "pre-hook", "post-hook"]:
                if "_extra" not in new_dict:
                    new_dict["_extra"] = {}
                new_dict["_extra"][key] = value
            else:
                new_dict[key] = value
        data = new_dict
        data = super().__pre_deserialize__(data)
        return data

    def __post_serialize__(self, dct):
        data = super().__post_serialize__(dct)
        data.update(self.extra)
        if "_extra" in data:
            del data["_extra"]
        return data

    def replace(self, **kwargs):
        dct = self.to_dict(omit_none=False)
        dct.update(kwargs)
        return self.from_dict(dct)

    @property
    def extra(self):
        return self._extra


@dataclass
class AdditionalPropertiesAllowed(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    _extra: Dict[str, Any] = field(default_factory=dict)
