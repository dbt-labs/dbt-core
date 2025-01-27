from datetime import datetime

from attr import dataclass

from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class SampleWindow(dbtClassMixin):
    start: datetime
    end: datetime

    def __post_serialize__(self, data, context):
        # This is insane, but necessary, I apologize. Mashumaro handles the
        # dictification of this class via a compile time generated `to_dict`
        # method based off of the _typing_ of th class. By default `datetime`
        # types are converted to strings. We don't want that, we want them to
        # stay datetimes.
        # Note: This is safe because the `BatchContext` isn't part of the artifact
        # and thus doesn't get written out.
        new_data = super().__post_serialize__(data, context)
        new_data["start"] = self.start
        new_data["end"] = self.end
        return new_data
