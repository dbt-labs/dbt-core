from datetime import datetime


# This converts a datetime to a json format datetime string which
# is used in constructing protobuf message timestamps.
def datetime_to_json_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# preformatted time stamp
def get_json_string_utcnow() -> str:
    ts = datetime.utcnow()
    ts_rfc3339 = datetime_to_json_string(ts)
    return ts_rfc3339
