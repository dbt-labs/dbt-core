from __future__ import annotations

from datetime import datetime

import pytz
from attr import dataclass

from dbt.artifacts.resources.types import BatchSize
from dbt.event_time.event_time import offset_timestamp
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError


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
        # Note: This is safe because the `SampleWindow` isn't part of the artifact
        # and thus doesn't get written out.
        new_data = super().__post_serialize__(data, context)
        new_data["start"] = self.start
        new_data["end"] = self.end
        return new_data

    @classmethod
    def from_relative_string(cls, relative_string: str) -> SampleWindow:
        end = datetime.now(tz=pytz.UTC)

        relative_window = relative_string.split(" ")
        if len(relative_window) != 2:
            raise DbtRuntimeError(
                f"Cannot load SAMPLE_WINDOW from '{relative_string}'. Must be of form 'DAYS_INT GRAIN_SIZE'."
            )

        try:
            lookback = int(relative_window[0])
        except Exception:
            raise DbtRuntimeError(f"Unable to convert '{relative_window[0]}' to an integer.")

        try:
            batch_size_string = relative_window[1].lower().rstrip("s")
            batch_size = BatchSize[batch_size_string]
        except Exception:
            grains = [size.value for size in BatchSize]
            grain_plurals = [BatchSize.plural(size) for size in BatchSize]
            valid_grains = grains + grain_plurals
            raise DbtRuntimeError(
                f"Invalid grain size '{relative_window[1]}'. Must be one of {valid_grains}."
            )

        start = offset_timestamp(timestamp=end, batch_size=batch_size, offset=-1 * lookback)

        return cls(start=start, end=end)
