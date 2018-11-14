import abc


def available(func):
    """A decorator to indicate that a method on the adapter will be exposed to
    the database wrapper, and the model name will be injected into the
    arguments.
    """
    func._is_available_ = True
    func._model_name_ = True
    return func


def available_raw(func):
    """A decorator to indicate that a method on the adapter will be exposed to
    the database wrapper, and the model name will be injected into the
    arguments.
    """
    func._is_available_ = True
    func._model_name_ = False
    return func


class AdapterMeta(abc.ABCMeta):
    def __new__(mcls, name, bases, namespace, **kwargs):
        cls = super(AdapterMeta, mcls).__new__(mcls, name, bases, namespace,
                                               **kwargs)

        # this is very much inspired by ABCMeta's own implementation

        # dict mapping the method name to whether the model name should be
        # injected into the arguments. All methods in here are exposed to the
        # context.
        available_model = set()
        available_raw = set()

        # collect base class data first
        for base in bases:
            available_model.update(getattr(base, '_available_model_', set()))
            available_raw.update(getattr(base, '_available_raw_', set()))

        # override with local data if it exists
        for name, value in namespace.items():
            if getattr(value, '_is_available_', False):
                if getattr(value, '_model_name_', False):
                    available_raw.discard(name)
                    available_model.add(name)
                else:
                    available_model.discard(name)
                    available_raw.add(name)

        cls._available_model_ = frozenset(available_model)
        cls._available_raw_ = frozenset(available_raw)
        return cls
