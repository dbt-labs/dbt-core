import inspect
from abc import abstractmethod
from typing import List, Optional, Type, TypeVar, Generic
from typing import Any  # noqa

from dbt.contracts.rpc import RPCParameters, RemoteResult, RemoteMethodFlags
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

    def get_flags(self) -> RemoteMethodFlags:
        return RemoteMethodFlags.Empty

    @classmethod
    def recursive_subclasses(
        cls: Type[T],
        named_only: bool = True,
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
        """set_args executes in the parent process for an RPC call"""
        raise NotImplementedException('set_args not implemented')

    @abstractmethod
    def handle_request(self) -> Result:
        """handle_request executes inside the child process for an RPC call"""
        raise NotImplementedException('handle_request not implemented')

    def cleanup(self, result: Optional[Result]):
        """cleanup is an optional method that executes inside the parent
        process for an RPC call.

        This will always be executed if set_args was.

        It's optional, and by default it does nothing.
        """

    def set_config(self, config):
        self.config = config


class RemoteManifestMethod(RemoteMethod[Parameters, Result]):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self.manifest = manifest


class TaskList(List[Type[RemoteMethod]]):
    def __init__(
        self,
        tasks: Optional[List[Type[RemoteMethod]]] = None
    ):
        task_list: List[Type[RemoteMethod]]
        if tasks is None:
            task_list = RemoteMethod.recursive_subclasses(named_only=True)
        else:
            task_list = tasks
        return super().__init__(task_list)

    def manifest(self) -> List[Type[RemoteManifestMethod]]:
        return [
            t for t in self if issubclass(t, RemoteManifestMethod)
        ]

    def non_manifest(self) -> List[Type[RemoteMethod]]:
        return [
            t for t in self if not issubclass(t, RemoteManifestMethod)
        ]
