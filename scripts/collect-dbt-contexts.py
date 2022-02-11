#!/usr/bin/env python

import inspect
import json
from dataclasses import dataclass
from typing import List, Optional, Iterable, Union, Dict, Any
from dbt.dataclass_schema import dbtClassMixin


from dbt.context.base import BaseContext
from dbt.context.target import TargetContext
from dbt.context.providers import ModelContext, MacroContext


CONTEXTS_MAP = {
    "base": BaseContext,
    "target": TargetContext,
    "model": ModelContext,
    "macro": MacroContext,
}


@dataclass
class ContextValue(dbtClassMixin):
    name: str
    value: str  # a type description
    doc: Optional[str]


@dataclass
class MethodArgument(dbtClassMixin):
    name: str
    value: str  # a type description


@dataclass
class ContextMethod(dbtClassMixin):
    name: str
    args: List[MethodArgument]
    result: str  # a type description
    doc: Optional[str]


@dataclass
class Unknown(dbtClassMixin):
    name: str
    value: str
    doc: Optional[str]


ContextMember = Union[ContextValue, ContextMethod, Unknown]


def _get_args(func: inspect.Signature) -> Iterable[MethodArgument]:
    found_first = False
    for argname, arg in func.parameters.items():
        if found_first is False and argname in {"self", "cls"}:
            continue
        if found_first is False:
            found_first = True

        yield MethodArgument(
            name=argname,
            value=inspect.formatannotation(arg.annotation),
        )


def collect(cls):
    values = []
    for name, v in cls._context_members_.items():
        attrname = cls._context_attrs_[name]
        attrdef = getattr(cls, attrname)
        doc = getattr(attrdef, "__doc__")
        if inspect.isfunction(attrdef):
            sig = inspect.signature(attrdef)
            result = inspect.formatannotation(sig.return_annotation)
            sig_good_part = ContextMethod(
                name=name,
                args=list(_get_args(sig)),
                result=result,
                doc=doc,
            )
        elif isinstance(attrdef, property):
            sig = inspect.signature(attrdef.fget)
            sig_txt = inspect.formatannotation(sig.return_annotation)
            sig_good_part = ContextValue(name=name, value=sig_txt, doc=doc)
        else:
            sig_good_part = Unknown(name=name, value=repr(attrdef), doc=doc)
        values.append(sig_good_part)

    return values


@dataclass
class ContextCatalog(dbtClassMixin):
    base: List[ContextMember]
    target: List[ContextMember]
    model: List[ContextMember]
    macro: List[ContextMember]
    schema: Dict[str, Any]


def main():
    catalog = ContextCatalog(
        base=collect(BaseContext),
        target=collect(TargetContext),
        model=collect(ModelContext),
        macro=collect(MacroContext),
        schema=ContextCatalog.json_schema(),
    )
    print(json.dumps(catalog.to_dict()))


if __name__ == "__main__":
    main()
