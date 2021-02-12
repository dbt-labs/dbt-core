# noinspection PyUnresolvedReferences
import builtins  # noqa
import collections
import collections.abc
import datetime
import enum
import ipaddress
import os
import pathlib
import typing
import uuid

# noinspection PyUnresolvedReferences
from base64 import decodebytes, encodebytes  # noqa
from contextlib import contextmanager, suppress

# noinspection PyProtectedMember
from dataclasses import _FIELDS, MISSING, Field, is_dataclass
from decimal import Decimal
from fractions import Fraction

# noinspection PyUnresolvedReferences
from mashumaro.exceptions import (  # noqa
    BadHookSignature,
    InvalidFieldValue,
    MissingField,
    UnserializableDataError,
    UnserializableField,
)
from mashumaro.meta.helpers import (
    get_class_that_define_method,
    get_imported_module_names,
    get_type_origin,
    is_class_var,
    is_generic,
    is_init_var,
    is_special_typing_primitive,
    is_type_var,
    is_union,
    type_name,
)
from mashumaro.meta.patch import patch_fromisoformat
from mashumaro.serializer.base.helpers import *  # noqa
from mashumaro.types import SerializableType, SerializationStrategy

patch_fromisoformat()


NoneType = type(None)
INITIAL_MODULES = get_imported_module_names()


__PRE_SERIALIZE__ = "__pre_serialize__"
__PRE_DESERIALIZE__ = "__pre_deserialize__"
__POST_SERIALIZE__ = "__post_serialize__"
__POST_DESERIALIZE__ = "__post_deserialize__"

DataClassDictMixinPath = "mashumaro.serializer.base.dict.DataClassDictMixin"


class CodeBuilder:
    def __init__(self, cls):
        self.cls = cls
        self.lines: typing.List[str] = []
        self.modules: typing.Set[str] = set()
        self.globals: typing.Set[str] = set()
        self._current_indent: str = ""

    def reset(self) -> None:
        self.lines = []
        self.modules = INITIAL_MODULES.copy()
        self.globals = set()
        self._current_indent = ""

    @property
    def namespace(self) -> typing.Dict[typing.Any, typing.Any]:
        return self.cls.__dict__

    @property
    def annotations(self) -> typing.Dict[str, typing.Any]:
        return self.namespace.get("__annotations__", {})

    def __get_field_types(
        self, recursive=True
    ) -> typing.Dict[str, typing.Any]:
        fields = {}
        for fname, ftype in typing.get_type_hints(self.cls).items():
            if is_class_var(ftype) or is_init_var(ftype):
                continue
            if recursive or fname in self.annotations:
                fields[fname] = ftype
        return fields

    @property
    def field_types(self) -> typing.Dict[str, typing.Any]:
        return self.__get_field_types()

    @property
    def defaults(self) -> typing.Dict[str, typing.Any]:
        d = {}
        for ancestor in self.cls.__mro__[-1:0:-1]:
            if is_dataclass(ancestor):
                for field in getattr(ancestor, _FIELDS).values():
                    if field.default is not MISSING:
                        d[field.name] = field.default
                    else:
                        d[field.name] = field.default_factory
        for name in self.__get_field_types(recursive=False):
            field = self.namespace.get(name, MISSING)
            if isinstance(field, Field):
                if field.default is not MISSING:
                    d[name] = field.default
                else:
                    # https://github.com/python/mypy/issues/6910
                    d[name] = field.default_factory  # type: ignore
            else:
                d[name] = field
        return d

    @property
    def metadatas(self) -> typing.Dict[str, typing.Mapping[str, typing.Any]]:
        d = {}
        for ancestor in self.cls.__mro__[-1:0:-1]:
            if is_dataclass(ancestor):
                for field in getattr(ancestor, _FIELDS).values():
                    d[field.name] = field.metadata
        for name in self.__get_field_types(recursive=False):
            field = self.namespace.get(name, MISSING)
            if isinstance(field, Field):
                d[name] = field.metadata
        return d

    def _add_type_modules(self, *types_) -> None:
        for t in types_:
            module = getattr(t, "__module__", None)
            if not module:
                return
            self.ensure_module_imported(module)
            args = getattr(t, "__args__", ())
            if args:
                self._add_type_modules(*args)
            constraints = getattr(t, "__constraints__", ())
            if constraints:
                self._add_type_modules(*constraints)

    def ensure_module_imported(self, module: str) -> None:
        if module not in self.modules:
            self.modules.add(module)
            self.add_line(f"if '{module}' not in globals():")
            with self.indent():
                self.add_line(f"import {module}")
            root_module = module.split(".")[0]
            if root_module not in self.globals:
                self.globals.add(root_module)
                self.add_line("else:")
                with self.indent():
                    self.add_line(f"global {root_module}")

    def add_line(self, line) -> None:
        self.lines.append(f"{self._current_indent}{line}")

    @contextmanager
    def indent(self) -> typing.Generator[None, None, None]:
        self._current_indent += " " * 4
        try:
            yield
        finally:
            self._current_indent = self._current_indent[:-4]

    def compile(self) -> None:
        exec("\n".join(self.lines), globals(), self.__dict__)

    def get_declared_hook(self, method_name: str):
        if not hasattr(self.cls, method_name):
            return
        cls = get_class_that_define_method(method_name, self.cls)
        if type_name(cls) != DataClassDictMixinPath:
            return cls.__dict__[method_name]

    def add_from_dict(self) -> None:

        self.reset()
        self.add_line("@classmethod")
        self.add_line(
            "def from_dict(cls, d, use_bytes=False, use_enum=False, "
            "use_datetime=False, options=None):"
        )
        with self.indent():
            pre_deserialize = self.get_declared_hook(__PRE_DESERIALIZE__)
            if pre_deserialize:
                if not isinstance(pre_deserialize, classmethod):
                    raise BadHookSignature(
                        f"`{__PRE_DESERIALIZE__}` must be a class method with "
                        f"Callable[[Dict[Any, Any]], Dict[Any, Any]] signature"
                    )
                else:
                    self.add_line(f"d = cls.{__PRE_DESERIALIZE__}(d, options=options)")
            self.add_line("try:")
            with self.indent():
                self.add_line("kwargs = {}")
                for fname, ftype in self.field_types.items():
                    metadata = self.metadatas.get(fname)
                    self._add_type_modules(ftype)
                    self.add_line(f"value = d.get('{fname}', MISSING)")
                    self.add_line("if value is None:")
                    with self.indent():
                        self.add_line(f"kwargs['{fname}'] = None")
                    self.add_line("else:")
                    with self.indent():
                        if self.defaults[fname] is MISSING:
                            self.add_line("if value is MISSING:")
                            with self.indent():
                                if isinstance(ftype, SerializationStrategy):
                                    self.add_line(
                                        f"raise MissingField('{fname}',"
                                        f"{type_name(ftype.__class__)},cls)"
                                    )
                                else:
                                    self.add_line(
                                        f"raise MissingField('{fname}',"
                                        f"{type_name(ftype)},cls)"
                                    )
                            self.add_line("else:")
                            with self.indent():
                                unpacked_value = self._unpack_field_value(
                                    fname=fname,
                                    ftype=ftype,
                                    parent=self.cls,
                                    metadata=metadata,
                                )
                                self.add_line("try:")
                                with self.indent():
                                    self.add_line(
                                        f"kwargs['{fname}'] = {unpacked_value}"
                                    )
                                self.add_line("except Exception as e:")
                                with self.indent():
                                    if isinstance(
                                        ftype, SerializationStrategy
                                    ):
                                        field_type = type_name(ftype.__class__)
                                    else:
                                        field_type = type_name(ftype)
                                    self.add_line(
                                        f"raise InvalidFieldValue('{fname}',"
                                        f"{field_type},value,cls)"
                                    )
                        else:
                            self.add_line("if value is not MISSING:")
                            with self.indent():
                                unpacked_value = self._unpack_field_value(
                                    fname=fname,
                                    ftype=ftype,
                                    parent=self.cls,
                                    metadata=metadata,
                                )
                                self.add_line("try:")
                                with self.indent():
                                    self.add_line(
                                        f"kwargs['{fname}'] = {unpacked_value}"
                                    )
                                self.add_line("except Exception as e:")
                                with self.indent():
                                    if isinstance(
                                        ftype, SerializationStrategy
                                    ):
                                        field_type = type_name(ftype.__class__)
                                    else:
                                        field_type = type_name(ftype)
                                    self.add_line(
                                        f"raise InvalidFieldValue('{fname}',"
                                        f"{field_type},value,cls)"
                                    )
            self.add_line("except AttributeError:")
            with self.indent():
                self.add_line("if not isinstance(d, dict):")
                with self.indent():
                    self.add_line(
                        f"raise ValueError('Argument for "
                        f"{type_name(self.cls)}.from_dict method "
                        f"should be a dict instance') from None"
                    )
                self.add_line("else:")
                with self.indent():
                    self.add_line("raise")
            post_deserialize = self.get_declared_hook(__POST_DESERIALIZE__)
            if post_deserialize:
                if not isinstance(post_deserialize, classmethod):
                    raise BadHookSignature(
                        f"`{__POST_DESERIALIZE__}` must be a class method "
                        f"with Callable[[{type_name(self.cls)}], "
                        f"{type_name(self.cls)}] signature"
                    )
                else:
                    self.add_line(
                        f"return cls.{__POST_DESERIALIZE__}(cls(**kwargs), options=options)"
                    )
            else:
                self.add_line("return cls(**kwargs)")
        self.add_line("setattr(cls, 'from_dict', from_dict)")
        self.compile()

    def add_to_dict(self) -> None:

        self.reset()
        self.add_line(
            "def to_dict(self, use_bytes=False, use_enum=False, "
            "use_datetime=False, options=None):"
        )
        with self.indent():
            pre_serialize = self.get_declared_hook(__PRE_SERIALIZE__)
            if pre_serialize:
                self.add_line(f"self = self.{__PRE_SERIALIZE__}(options=options)")
            self.add_line("kwargs = {}")
            for fname, ftype in self.field_types.items():
                metadata = self.metadatas.get(fname)
                self.add_line(f"value = getattr(self, '{fname}')")
                self.add_line("if value is None:")
                with self.indent():
                    self.add_line(f"kwargs['{fname}'] = None")
                self.add_line("else:")
                with self.indent():
                    packed_value = self._pack_value(
                        fname=fname,
                        ftype=ftype,
                        parent=self.cls,
                        metadata=metadata,
                    )
                    self.add_line(f"kwargs['{fname}'] = {packed_value}")
            post_serialize = self.get_declared_hook(__POST_SERIALIZE__)
            if post_serialize:
                self.add_line(f"return self.{__POST_SERIALIZE__}(kwargs, options=options)")
            else:
                self.add_line("return kwargs")
        self.add_line("setattr(cls, 'to_dict', to_dict)")
        self.compile()

    def add_pack_union(self, fname, ftype, parent, variant_types, value_name, metadata):
        self._add_type_modules(*variant_types)
        self.add_line(f'def resolve_union(value):')
        with self.indent():
            for variant in variant_types:
                if is_generic(variant):
                    variant_name = get_type_origin(variant).__name__
                else:
                    variant_name = type_name(variant)
                self.add_line(f'if isinstance(value, {variant_name}):')
                with self.indent():
                    self.add_line('try:')
                    with self.indent():
                        packed = self._pack_value(fname, variant, parent, value_name, metadata=metadata)
                        self.add_line(f'return {packed}')
                    self.add_line('except (TypeError, AttributeError, ValueError, LookupError) as e:')
                    with self.indent():
                        self.add_line('pass')
            else:
                variant_type_names = ", ".join([type_name(v) for v in variant_types])
                self.add_line(f"raise ValueError('Union value could not be "
                              f"encoded using types ({variant_type_names})')")
            return 'resolve_union(value)'

    def _pack_value(
        self, fname, ftype, parent, value_name="value", metadata=None
    ):

        overridden: typing.Optional[str] = None
        if metadata is not None:
            serialize_option = metadata.get("serialize")
            if callable(serialize_option):
                setattr(
                    self.cls,
                    f"__{fname}_serialize",
                    staticmethod(serialize_option),
                )
                return f"self.__{fname}_serialize(self.{fname})"

        if is_dataclass(ftype):
            return f"{value_name}.to_dict(use_bytes, use_enum, use_datetime, options)"

        with suppress(TypeError):
            if issubclass(ftype, SerializableType):
                return f"{value_name}._serialize()"
        if isinstance(ftype, SerializationStrategy):
            return (
                f"self.__dataclass_fields__['{fname}'].type"
                f"._serialize({value_name})"
            )
        if type_name(ftype) in self.cls._serializable_encoders:
            return f"self._serializable_encoders['{type_name(ftype)}']._serialize({value_name})"

        origin_type = get_type_origin(ftype)
        if is_special_typing_primitive(origin_type):
            if origin_type is typing.Any:
                return overridden or value_name
            elif is_union(ftype):
                args = getattr(ftype, "__args__", ())
                if len(args) == 2 and args[1] == NoneType:  # it is Optional
                    return self._pack_value(fname, args[0], parent, metadata=metadata)
                else:
                    return self.add_pack_union(fname, ftype, parent, args, value_name, metadata)
            elif origin_type is typing.AnyStr:
                raise UnserializableDataError(
                    "AnyStr is not supported by mashumaro"
                )
            elif is_type_var(ftype):
                raise UnserializableDataError(
                    "TypeVars are not supported by mashumaro"
                )
            else:
                raise UnserializableDataError(
                    f"{ftype} as a field type is not supported by mashumaro"
                )
        elif origin_type in (bool, NoneType):
            return overridden or value_name
        elif issubclass(origin_type, typing.Collection):
            args = getattr(ftype, "__args__", ())

            def inner_expr(arg_num=0, v_name="value"):
                return self._pack_value(fname, args[arg_num], parent, v_name, metadata=metadata)

            if issubclass(
                origin_type,
                (typing.List, typing.Deque, typing.Tuple, typing.AbstractSet),
            ):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"[{inner_expr()} for value in {value_name}]"
                    )
                elif ftype is list:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.List[T] instead"
                    )
                elif ftype is collections.deque:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Deque[T] instead"
                    )
                elif ftype is tuple:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Tuple[T] instead"
                    )
                elif ftype is set:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Set[T] instead"
                    )
                elif ftype is frozenset:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.FrozenSet[T] instead"
                    )
            elif issubclass(origin_type, typing.ChainMap):
                if ftype is collections.ChainMap:
                    raise UnserializableField(
                        fname,
                        ftype,
                        parent,
                        "Use typing.ChainMap[KT,VT] instead",
                    )
                elif is_generic(ftype):
                    if is_dataclass(args[0]):
                        raise UnserializableDataError(
                            "ChainMaps with dataclasses as keys "
                            "are not supported by mashumaro"
                        )
                    else:
                        return (
                            overridden
                            or f'[{{{inner_expr(0,"key")}:{inner_expr(1)} '
                            f"for key,value in m.items()}} "
                            f"for m in value.maps]"
                        )
            elif issubclass(origin_type, str):
                return overridden or value_name
            elif issubclass(origin_type, typing.Mapping):
                if ftype is dict:
                    raise UnserializableField(
                        fname,
                        ftype,
                        parent,
                        "Use typing.Dict[KT,VT] or Mapping[KT,VT] instead",
                    )
                elif is_generic(ftype):
                    if is_dataclass(args[0]):
                        raise UnserializableDataError(
                            "Mappings with dataclasses as keys "
                            "are not supported by mashumaro"
                        )
                    else:
                        return (
                            overridden
                            or f'{{{inner_expr(0,"key")}: {inner_expr(1)} '
                            f"for key, value in {value_name}.items()}}"
                        )
            elif issubclass(origin_type, typing.ByteString):
                specific = f"encodebytes({value_name}).decode()"
                return (
                    f"{value_name} if use_bytes else {overridden or specific}"
                )
            elif issubclass(origin_type, typing.Sequence):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"[{inner_expr()} for value in {value_name}]"
                    )
        elif issubclass(origin_type, os.PathLike):
            return overridden or f"{value_name}.__fspath__()"
        elif issubclass(origin_type, enum.Enum):
            specific = f"{value_name}.value"
            return f"{value_name} if use_enum else {overridden or specific}"
        elif origin_type is int:
            return overridden or f"int({value_name})"
        elif origin_type is float:
            return overridden or f"float({value_name})"
        elif origin_type in (datetime.datetime, datetime.date, datetime.time):
            if overridden:
                return f"{value_name} if use_datetime else {overridden}"
            return (
                f"{value_name} if use_datetime else {value_name}.isoformat()"
            )
        elif origin_type is datetime.timedelta:
            return overridden or f"{value_name}.total_seconds()"
        elif origin_type is datetime.timezone:
            return overridden or f"{value_name}.tzname(None)"
        elif origin_type is uuid.UUID:
            return overridden or f"str({value_name})"
        elif origin_type in [
            ipaddress.IPv4Address,
            ipaddress.IPv6Address,
            ipaddress.IPv4Network,
            ipaddress.IPv6Network,
            ipaddress.IPv4Interface,
            ipaddress.IPv6Interface,
        ]:
            return overridden or f"str({value_name})"
        elif origin_type is Decimal:
            return overridden or f"str({value_name})"
        elif origin_type is Fraction:
            return overridden or f"str({value_name})"

        raise UnserializableDataError(f"_unpack_value: fname: {fname}, ftype: {ftype}, parent: {parent}, origin_type: {origin_type}")

    def add_unpack_union(self, fname, ftype, parent, variant_types, value_name, metadata):
        self.add_line(f'def resolve_union(value):')
        with self.indent():
            for variant in variant_types:
                if is_generic(variant):
                    variant_name = get_type_origin(variant).__name__
                else:
                    variant_name = type_name(variant)
                self.add_line('try:')
                with self.indent():
                    packed = self._unpack_field_value(fname, variant, parent, value_name)
                    self.add_line(f'packed = {packed}')
                    self.add_line(f'if isinstance(packed, {variant_name}):')
                    with self.indent():
                        self.add_line(f'return packed')
                self.add_line('except (TypeError, AttributeError, ValueError, LookupError) as e:')
                with self.indent():
                    self.add_line('pass')
            else:
                variant_type_names = ", ".join([type_name(v) for v in variant_types])
                self.add_line(f"raise ValueError('Union value could not be "
                              f"decoded using types ({variant_type_names})')")
            return 'resolve_union(value)'

    def _unpack_field_value(
        self, fname, ftype, parent, value_name="value", metadata=None
    ):

        deserialize_option: typing.Optional[typing.Any] = None
        overridden: typing.Optional[str] = None
        if metadata is not None:
            deserialize_option = metadata.get("deserialize")
            if callable(deserialize_option):
                setattr(self.cls, f"__{fname}_deserialize", deserialize_option)
                return f"cls.__{fname}_deserialize({value_name})"

        if is_dataclass(ftype):
            return (
                f"{type_name(ftype)}.from_dict({value_name}, "
                f"use_bytes, use_enum, use_datetime, options)"
            )

        with suppress(TypeError):
            if issubclass(ftype, SerializableType):
                return f"{type_name(ftype)}._deserialize({value_name})"
        if isinstance(ftype, SerializationStrategy):
            return (
                f"cls.__dataclass_fields__['{fname}'].type"
                f"._deserialize({value_name})"
            )
        if type_name(ftype) in self.cls._serializable_encoders:
            return f"cls._serializable_encoders['{type_name(ftype)}']._deserialize({value_name})"

        origin_type = get_type_origin(ftype)
        if is_special_typing_primitive(origin_type):
            if origin_type is typing.Any:
                return overridden or value_name
            elif is_union(ftype):
                args = getattr(ftype, "__args__", ())
                if len(args) == 2 and args[1] == NoneType:  # it is Optional
                    return self._unpack_field_value(
                        fname, args[0], parent, metadata=metadata
                    )
                else:
                    return self.add_unpack_union(fname, ftype, parent, args, value_name, metadata)
            elif origin_type is typing.AnyStr:
                raise UnserializableDataError(
                    "AnyStr is not supported by mashumaro"
                )
            elif is_type_var(ftype):
                raise UnserializableDataError(
                    "TypeVars are not supported by mashumaro"
                )
            else:
                raise UnserializableDataError(
                    f"{ftype} as a field type is not supported by mashumaro"
                )
        elif origin_type in (bool, NoneType):
            return overridden or value_name
        elif issubclass(origin_type, typing.Collection):
            args = getattr(ftype, "__args__", ())

            def inner_expr(arg_num=0, v_name="value"):
                return self._unpack_field_value(
                    fname, args[arg_num], parent, v_name
                )

            if issubclass(origin_type, typing.List):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"[{inner_expr()} for value in {value_name}]"
                    )
                elif ftype is list:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.List[T] instead"
                    )
            elif issubclass(origin_type, typing.Deque):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"collections.deque([{inner_expr()} "
                        f"for value in {value_name}])"
                    )
                elif ftype is collections.deque:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Deque[T] instead"
                    )
            elif issubclass(origin_type, typing.Tuple):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"tuple([{inner_expr()} for value in {value_name}])"
                    )
                elif ftype is tuple:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Tuple[T] instead"
                    )
            elif issubclass(origin_type, typing.FrozenSet):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"frozenset([{inner_expr()} "
                        f"for value in {value_name}])"
                    )
                elif ftype is frozenset:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.FrozenSet[T] instead"
                    )
            elif issubclass(origin_type, typing.AbstractSet):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"set([{inner_expr()} for value in {value_name}])"
                    )
                elif ftype is set:
                    raise UnserializableField(
                        fname, ftype, parent, "Use typing.Set[T] instead"
                    )
            elif issubclass(origin_type, typing.ChainMap):
                if ftype is collections.ChainMap:
                    raise UnserializableField(
                        fname,
                        ftype,
                        parent,
                        "Use typing.ChainMap[KT,VT] instead",
                    )
                elif is_generic(ftype):
                    if is_dataclass(args[0]):
                        raise UnserializableDataError(
                            "ChainMaps with dataclasses as keys "
                            "are not supported by mashumaro"
                        )
                    else:
                        return (
                            overridden
                            or f"collections.ChainMap("
                            f'*[{{{inner_expr(0,"key")}:{inner_expr(1)} '
                            f"for key, value in m.items()}} "
                            f"for m in {value_name}])"
                        )
            elif issubclass(origin_type, str):
                return overridden or value_name
            elif issubclass(origin_type, typing.Mapping):
                if ftype is dict:
                    raise UnserializableField(
                        fname,
                        ftype,
                        parent,
                        "Use typing.Dict[KT,VT] or Mapping[KT,VT] instead",
                    )
                elif is_generic(ftype):
                    if is_dataclass(args[0]):
                        raise UnserializableDataError(
                            "Mappings with dataclasses as keys "
                            "are not supported by mashumaro"
                        )
                    else:
                        return (
                            overridden
                            or f'{{{inner_expr(0,"key")}: {inner_expr(1)} '
                            f"for key, value in {value_name}.items()}}"
                        )
            elif issubclass(origin_type, typing.ByteString):
                if origin_type is bytes:
                    specific = f"decodebytes({value_name}.encode())"
                    return (
                        f"{value_name} if use_bytes else "
                        f"{overridden or specific}"
                    )
                elif origin_type is bytearray:
                    if overridden:
                        overridden = (
                            f"bytearray({value_name}) if use_bytes else "
                            f"{overridden}"
                        )
                    specific = (
                        f"bytearray({value_name} if use_bytes else "
                        f"decodebytes({value_name}.encode()))"
                    )
                    return overridden or specific
            elif issubclass(origin_type, typing.Sequence):
                if is_generic(ftype):
                    return (
                        overridden
                        or f"[{inner_expr()} for value in {value_name}]"
                    )
        elif issubclass(origin_type, os.PathLike):
            if overridden:
                return overridden
            elif issubclass(origin_type, pathlib.PosixPath):
                return f"pathlib.PosixPath({value_name})"
            elif issubclass(origin_type, pathlib.WindowsPath):
                return f"pathlib.WindowsPath({value_name})"
            elif issubclass(origin_type, pathlib.Path):
                return f"pathlib.Path({value_name})"
            elif issubclass(origin_type, pathlib.PurePosixPath):
                return f"pathlib.PurePosixPath({value_name})"
            elif issubclass(origin_type, pathlib.PureWindowsPath):
                return f"pathlib.PureWindowsPath({value_name})"
            elif issubclass(origin_type, pathlib.PurePath):
                return f"pathlib.PurePath({value_name})"
            elif origin_type is os.PathLike:
                return f"pathlib.PurePath({value_name})"
            else:
                return f"{type_name(origin_type)}({value_name})"
        elif issubclass(origin_type, enum.Enum):
            specific = f"{type_name(origin_type)}({value_name})"
            return f"{value_name} if use_enum else {overridden or specific}"
        elif origin_type is int:
            return overridden or f"int({value_name})"
        elif origin_type is float:
            return overridden or f"float({value_name})"
        elif origin_type in (datetime.datetime, datetime.date, datetime.time):
            if overridden:
                return f"{value_name} if use_datetime else {overridden}"
            elif deserialize_option is not None:
                if deserialize_option == "ciso8601":
                    self.ensure_module_imported("ciso8601")
                    datetime_parser = "ciso8601.parse_datetime"
                elif deserialize_option == "pendulum":
                    self.ensure_module_imported("pendulum")
                    datetime_parser = "pendulum.parse"
                else:
                    raise UnserializableField(
                        fname,
                        ftype,
                        parent,
                        f"Unsupported deserialization engine "
                        f'"{deserialize_option}"',
                    )
                suffix = ""
                if origin_type is datetime.date:
                    suffix = ".date()"
                elif origin_type is datetime.time:
                    suffix = ".time()"
                return (
                    f"{value_name} if use_datetime else "
                    f"{datetime_parser}({value_name}){suffix}"
                )
            return (
                f"{value_name} if use_datetime else "
                f"datetime.{origin_type.__name__}."
                f"fromisoformat({value_name})"
            )
        elif origin_type is datetime.timedelta:
            return overridden or f"datetime.timedelta(seconds={value_name})"
        elif origin_type is datetime.timezone:
            return overridden or f"parse_timezone({value_name})"
        elif origin_type is uuid.UUID:
            return overridden or f"uuid.UUID({value_name})"
        elif origin_type is ipaddress.IPv4Address:
            return overridden or f"ipaddress.IPv4Address({value_name})"
        elif origin_type is ipaddress.IPv6Address:
            return overridden or f"ipaddress.IPv6Address({value_name})"
        elif origin_type is ipaddress.IPv4Network:
            return overridden or f"ipaddress.IPv4Network({value_name})"
        elif origin_type is ipaddress.IPv6Network:
            return overridden or f"ipaddress.IPv6Network({value_name})"
        elif origin_type is ipaddress.IPv4Interface:
            return overridden or f"ipaddress.IPv4Interface({value_name})"
        elif origin_type is ipaddress.IPv6Interface:
            return overridden or f"ipaddress.IPv6Interface({value_name})"
        elif origin_type is Decimal:
            return overridden or f"Decimal({value_name})"
        elif origin_type is Fraction:
            return overridden or f"Fraction({value_name})"

        raise UnserializableField(fname, ftype, parent)
