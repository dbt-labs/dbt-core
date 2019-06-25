import copy
from collections import Mapping
from jsonschema import Draft7Validator

from dbt.exceptions import JSONValidationException
from dbt.utils import deep_merge
from dbt.clients.system import write_json


class APIObject(Mapping):
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

    def __init__(self, **kwargs):
        """
        Create and validate an instance. Note that if you override this, you
        will want to do so by modifying kwargs and only then calling
        super().__init__(**kwargs).
        """
        super().__init__()
        # note: deep_merge does a deep copy on its arguments.
        self._contents = deep_merge(self.DEFAULTS, kwargs)
        self.validate()

    def __str__(self):
        return '{}(**{})'.format(self.__class__.__name__, self._contents)

    def __repr__(self):
        return '{}(**{})'.format(self.__class__.__name__, self._contents)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.serialize() == other.serialize()

    def incorporate(self, **kwargs):
        """
        Given a list of kwargs, incorporate these arguments
        into a new copy of this instance, and return the new
        instance after validating.
        """
        return type(self)(**deep_merge(self._contents, kwargs))

    def serialize(self):
        """
        Return a dict representation of this object.
        """
        return copy.deepcopy(self._contents)

    def write(self, path):
        write_json(path, self.serialize())

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
        validator = Draft7Validator(self.SCHEMA)

        errors = set()  # make errors a set to avoid duplicates

        for error in validator.iter_errors(self.serialize()):
            errors.add('.'.join(
                list(map(str, error.path)) + [error.message]
            ))

        if errors:
            raise JSONValidationException(type(self).__name__, errors)

    # implement the Mapping protocol:
    # https://docs.python.org/3/library/collections.abc.html
    def __getitem__(self, key):
        return self._contents[key]

    def __iter__(self):
        return self._contents.__iter__()

    def __len__(self):
        return self._contents.__len__()

    # implement this because everyone always expects it.
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key, value):
        self._contents[key] = value

    # most users of APIObject also expect the attributes to be available via
    # dot-notation because the previous implementation assigned to __dict__.
    # we should consider removing this if we fix all uses to have properties.
    def __getattr__(self, name):
        if name != '_contents' and name in self._contents:
            return self._contents[name]
        elif hasattr(self.__class__, name):
            return getattr(self.__class__, name)
        raise AttributeError((
            "'{}' object has no attribute '{}'"
        ).format(type(self).__name__, name))
