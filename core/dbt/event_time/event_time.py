from datetime import datetime

from dateutil.relativedelta import relativedelta

from dbt.artifacts.resources.types import BatchSize
from dbt_common.exceptions import DbtRuntimeError


def offset_timestamp(timestamp=datetime, batch_size=BatchSize, offset=int) -> datetime:
    """Offsets the passed in timestamp based on the batch_size and offset.

    Note: THIS IS DIFFERENT FROM MicrobatchBuilder.offset_timestamp. That function first
    `truncates` the timestamp, and then does delta addition subtraction from there. This
    function _doesn't_ truncate the timestamp and uses `relativedelta` for specific edge
    case handling (months, years), which may produce different results than the delta math
    done in `MicrobatchBuilder.offset_timestamp`

    Examples
    2024-09-17 16:06:00 + Batchsize.hour -1 -> 2024-09-17 15:06:00
    2024-09-17 16:06:00 + Batchsize.hour +1 -> 2024-09-17 17:06:00
    2024-09-17 16:06:00 + Batchsize.day -1 -> 2024-09-16 16:06:00
    2024-09-17 16:06:00 + Batchsize.day +1 -> 2024-09-18 16:06:00
    2024-09-17 16:06:00 + Batchsize.month -1 -> 2024-08-17 16:06:00
    2024-09-17 16:06:00 + Batchsize.month +1 -> 2024-10-17 16:06:00
    2024-09-17 16:06:00 + Batchsize.year -1 -> 2023-09-17 16:06:00
    2024-09-17 16:06:00 + Batchsize.year +1 -> 2025-09-17 16:06:00
    2024-01-31 16:06:00 + Batchsize.month +1 -> 2024-02-29 16:06:00
    2024-02-29 16:06:00 + Batchsize.year +1 -> 2025-02-28 16:06:00
    """

    if batch_size == BatchSize.hour:
        return timestamp + relativedelta(hours=offset)
    elif batch_size == BatchSize.day:
        return timestamp + relativedelta(days=offset)
    elif batch_size == BatchSize.month:
        return timestamp + relativedelta(months=offset)
    elif batch_size == BatchSize.year:
        return timestamp + relativedelta(years=offset)
    else:
        raise DbtRuntimeError(f"Unhandled batch_size '{batch_size}'")
