from typing import Optional


def lowercase(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    else:
        return value.lower()
