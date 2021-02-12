from mashumaro.meta.helpers import type_name


class MissingField(LookupError):
    def __init__(self, field_name, field_type, holder_class):
        self.field_name = field_name
        self.field_type = field_type
        self.holder_class = holder_class

    @property
    def field_type_name(self):
        return type_name(self.field_type)

    @property
    def holder_class_name(self):
        return type_name(self.holder_class)

    def __str__(self):
        return (
            f'Field "{self.field_name}" of type {self.field_type_name}'
            f" is missing in {self.holder_class_name} instance"
        )


class UnserializableDataError(TypeError):
    pass


class UnserializableField(UnserializableDataError):
    def __init__(self, field_name, field_type, holder_class, msg=None):
        self.field_name = field_name
        self.field_type = field_type
        self.holder_class = holder_class
        self.msg = msg

    @property
    def field_type_name(self):
        return type_name(self.field_type)

    @property
    def holder_class_name(self):
        return type_name(self.holder_class)

    def __str__(self):
        s = (
            f'Field "{self.field_name}" of type {self.field_type_name} '
            f"in {self.holder_class_name} is not serializable"
        )
        if self.msg:
            s += f": {self.msg}"
        return s


class InvalidFieldValue(ValueError):
    def __init__(
        self, field_name, field_type, field_value, holder_class, msg=None
    ):
        self.field_name = field_name
        self.field_type = field_type
        self.field_value = field_value
        self.holder_class = holder_class
        self.msg = msg

    @property
    def field_type_name(self):
        return type_name(self.field_type)

    @property
    def holder_class_name(self):
        return type_name(self.holder_class)

    def __str__(self):
        s = (
            f'Field "{self.field_name}" of type {self.field_type_name} '
            f"in {self.holder_class_name} has invalid value "
            f"{repr(self.field_value)}"
        )
        if self.msg:
            s += f": {self.msg}"
        return s


class BadHookSignature(TypeError):
    pass
