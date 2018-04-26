import copy
from jsonschema import Draft4Validator

from dbt.exceptions import ValidationException
from dbt.utils import deep_merge


class APIObject(dict):
    """
    A serializable / deserializable object intended for
    use in a future dbt API.

    To create a new object, you'll want to extend this
    class, and then implement the SCHEMA property (a
    valid JSON schema), the DEFAULTS property (default
    settings for this object), and a static method that
    calls this constructor.
    """

    SCHEMA = {
        'type': 'object',
        'properties': {}
    }

    DEFAULTS = {}

    def __init__(self, *args, **kwargs):
        """
        Create and validate an instance. Note that it's
        not a good idea to override this.
        """
        defaults = copy.deepcopy(self.DEFAULTS)
        settings = copy.deepcopy(kwargs)

        d = deep_merge(defaults, settings)
        super(APIObject, self).__init__(*args, **d)
        self.__dict__ = self
        self.validate()

    def incorporate(self, **kwargs):
        """
        Given a list of kwargs, incorporate these arguments
        into a new copy of this instance, and return the new
        instance after validating.
        """
        existing = copy.deepcopy(dict(self))
        updates = copy.deepcopy(kwargs)
        return type(self)(**deep_merge(existing, updates))

    def serialize(self):
        """
        Return a dict representation of this object.
        """
        return dict(self)

    @classmethod
    def deserialize(cls, settings):
        """
        Convert a dict representation of this object into
        an actual object for internal use.
        """
        return cls(**settings)

    def validate(self):
        """
        Using the SCHEMA property, validate the attributes
        of this instance. If any attributes are missing or
        invalid, raise a ValidationException.
        """
        validator = Draft4Validator(self.SCHEMA)

        errors = []

        for error in validator.iter_errors(self.serialize()):
            errors.append('property "{}", {}'.format(
                ".".join(error.path), error.message))

        if errors:
            raise ValidationException(
                'Invalid arguments passed to "{}" instance: {}'
                .format(type(self).__name__,
                        ", ".join(errors)))
