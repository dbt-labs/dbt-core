import inspect
from abc import abstractmethod
from typing import List, Optional, Type, TypeVar, Generic

from dbt.contracts.rpc import RPCParameters, RemoteResult
from dbt.exceptions import NotImplementedException, InternalException


Parameters = TypeVar('Parameters', bound=RPCParameters)
Result = TypeVar('Result', bound=RemoteResult)


# If you call recursive_subclasses on a subclass of BaseRemoteMethod, it should
# only return subtypes of the given subclass.
T = TypeVar('T', bound='RemoteMethod')


class RemoteMethod(Generic[Parameters, Result]):
    METHOD_NAME: Optional[str] = None

    def __init__(self, args, config):
        self.args = args
        self.config = config

    @classmethod
    def get_parameters(cls) -> Type[Parameters]:
        argspec = inspect.getfullargspec(cls.set_args)
        annotations = argspec.annotations
        if 'params' not in annotations:
            raise InternalException(
                'set_args must have parameter named params with a valid '
                'RPCParameters type definition (no params annotation found)'
            )
        params_type = annotations['params']
        if not issubclass(params_type, RPCParameters):
            raise InternalException(
                'set_args must have parameter named params with a valid '
                'RPCParameters type definition (got {}, expected '
                'RPCParameters subclass)'.format(params_type)
            )
        if params_type is RPCParameters:
            raise InternalException(
                'set_args must have parameter named params with a valid '
                'RPCParameters type definition (got RPCParameters itself!)'
            )
        return params_type

    @classmethod
    def recursive_subclasses(
        cls: Type[T], named_only: bool = True
    ) -> List[Type[T]]:
        classes = []
        current = [cls]
        while current:
            klass = current.pop()
            scls = klass.__subclasses__()
            classes.extend(scls)
            current.extend(scls)
        if named_only:
            classes = [c for c in classes if c.METHOD_NAME is not None]
        return classes

    @abstractmethod
    def set_args(self, params: Parameters):
        raise NotImplementedException(
            'set_args not implemented'
        )

    @abstractmethod
    def handle_request(self) -> Result:
        raise NotImplementedException(
            'handle_request not implemented'
        )


class RemoteManifestMethod(RemoteMethod[Parameters, Result]):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest
