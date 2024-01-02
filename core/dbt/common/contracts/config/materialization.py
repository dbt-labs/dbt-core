from dbt.common.dataclass_schema import StrEnum


class OnConfigurationChangeOption(StrEnum):
    Apply = "apply"
    Continue = "continue"
    Fail = "fail"

    @classmethod
    def default(cls) -> "OnConfigurationChangeOption":
        return cls.Apply
