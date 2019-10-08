import base64
import inspect
from abc import ABCMeta, abstractmethod
from typing import Union, List, Optional, Type

from hologram import JsonSchemaMixin

from dbt.exceptions import NotImplementedException
from dbt.rpc.logger import RemoteCallableResult, RemoteExecutionResult
from dbt.rpc.error import invalid_params
from dbt.task.compile import CompileTask


class RemoteCallable(metaclass=ABCMeta):
    METHOD_NAME: Optional[str] = None
    is_async = False

    @classmethod
    def get_parameters(cls) -> Type[JsonSchemaMixin]:
        argspec = inspect.getfullargspec(cls.set_args)
        annotations = argspec.annotations
        if 'params' not in annotations:
            raise TypeError(
                'set_args must have parameter named params with a valid '
                'JsonSchemaMixin type definition (no params annotation found)'
            )
        params_type = annotations['params']
        if not issubclass(params_type, JsonSchemaMixin):
            raise TypeError(
                'set_args must have parameter named params with a valid '
                'JsonSchemaMixin type definition (got {}, expected '
                'JsonSchemaMixin subclass)'.format(params_type)
            )
        if params_type is JsonSchemaMixin:
            raise TypeError(
                'set_args must have parameter named params with a valid '
                'JsonSchemaMixin type definition (got JsonSchemaMixin itself!)'
            )
        return params_type

    @abstractmethod
    def set_args(self, params: JsonSchemaMixin):
        raise NotImplementedException(
            'set_args not implemented'
        )

    @abstractmethod
    def handle_request(self) -> RemoteCallableResult:
        raise NotImplementedException(
            'handle_request not implemented'
        )

    @staticmethod
    def _listify(
        value: Optional[Union[str, List[str]]]
    ) -> Optional[List[str]]:
        if value is None:
            return None
        elif isinstance(value, str):
            return [value]
        else:
            return value

    def decode_sql(self, sql: str) -> str:
        """Base64 decode a string. This should only be used for sql in calls.

        :param str sql: The base64 encoded form of the original utf-8 string
        :return str: The decoded utf-8 string
        """
        # JSON is defined as using "unicode", we'll go a step further and
        # mandate utf-8 (though for the base64 part, it doesn't really matter!)
        base64_sql_bytes = str(sql).encode('utf-8')

        try:
            sql_bytes = base64.b64decode(base64_sql_bytes, validate=True)
        except ValueError:
            self.raise_invalid_base64(sql)

        return sql_bytes.decode('utf-8')

    @staticmethod
    def raise_invalid_base64(sql):
        raise invalid_params(
            data={
                'message': 'invalid base64-encoded sql input',
                'sql': str(sql),
            }
        )


class RPCTask(CompileTask, RemoteCallable):
    def __init__(self, args, config, manifest):
        super().__init__(args, config)
        self._base_manifest = manifest.deepcopy(config=config)

    @classmethod
    def recursive_subclasses(
        cls, named_only: bool = True
    ) -> List[Type['RPCTask']]:
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

    def get_result(self, results, elapsed_time, generated_at):
        return RemoteExecutionResult(
            results=results,
            elapsed_time=elapsed_time,
            generated_at=generated_at,
            logs=[],
        )
