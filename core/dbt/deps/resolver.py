from dataclasses import dataclass, field
from typing import Dict, List, NoReturn, Union, Type, Iterator, Set

from dbt.exceptions import raise_dependency_error, InternalException

from dbt.config import Project, RuntimeConfig
from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.deps.base import BasePackage, PinnedPackage, UnpinnedPackage
from dbt.deps.local import LocalUnpinnedPackage
from dbt.deps.git import GitUnpinnedPackage
from dbt.deps.registry import RegistryUnpinnedPackage

from dbt.contracts.project import (
    LocalPackage,
    GitPackage,
    RegistryPackage,
)

PackageContract = Union[LocalPackage, GitPackage, RegistryPackage]


@dataclass
class PackageListing:
    packages: Dict[str, UnpinnedPackage] = field(default_factory=dict)

    def __len__(self):
        return len(self.packages)

    def __bool__(self):
        return bool(self.packages)

    def _pick_key(self, key: BasePackage) -> str:
        for name in key.all_names():
            if name in self.packages:
                return name
        return key.name

    def __contains__(self, key: BasePackage):
        for name in key.all_names():
            if name in self.packages:
                return True

    def __getitem__(self, key: BasePackage):
        key_str: str = self._pick_key(key)
        return self.packages[key_str]

    def __setitem__(self, key: BasePackage, value):
        key_str: str = self._pick_key(key)
        self.packages[key_str] = value

    def _mismatched_types(self, old: UnpinnedPackage, new: UnpinnedPackage) -> NoReturn:
        raise_dependency_error(
            f"Cannot incorporate {new} ({new.__class__.__name__}) in {old} "
            f"({old.__class__.__name__}): mismatched types"
        )

    def incorporate(self, package: UnpinnedPackage):
        key: str = self._pick_key(package)
        if key in self.packages:
            existing: UnpinnedPackage = self.packages[key]
            if not isinstance(existing, type(package)):
                self._mismatched_types(existing, package)
            self.packages[key] = existing.incorporate(package)
        else:
            self.packages[key] = package

    def update_from(self, src: List[PackageContract]) -> None:
        pkg: UnpinnedPackage
        for contract in src:
            if isinstance(contract, LocalPackage):
                pkg = LocalUnpinnedPackage.from_contract(contract)
            elif isinstance(contract, GitPackage):
                pkg = GitUnpinnedPackage.from_contract(contract)
            elif isinstance(contract, RegistryPackage):
                pkg = RegistryUnpinnedPackage.from_contract(contract)
            else:
                raise InternalException("Invalid package type {}".format(type(contract)))
            self.incorporate(pkg)

    @classmethod
    def from_contracts(
        cls: Type["PackageListing"], src: List[PackageContract]
    ) -> "PackageListing":
        self = cls({})
        self.update_from(src)
        return self

    def resolved(self) -> List[PinnedPackage]:
        return [p.resolved() for p in self.packages.values()]

    def __iter__(self) -> Iterator[UnpinnedPackage]:
        return iter(self.packages.values())


def _check_for_duplicate_project_names(
    final_deps: List[PinnedPackage],
    config: Project,
    renderer: DbtProjectYamlRenderer,
):
    seen: Set[str] = set()
    for package in final_deps:
        project_name = package.get_project_name(config, renderer)
        if project_name in seen:
            raise_dependency_error(
                f'Found duplicate project "{project_name}". This occurs when '
                "a dependency has the same project name as some other "
                "dependency."
            )
        elif project_name == config.project_name:
            raise_dependency_error(
                "Found a dependency with the same name as the root project "
                f'"{project_name}". Package names must be unique in a project.'
                " Please rename one of these packages."
            )
        seen.add(project_name)


def resolve_packages(
    packages: List[PackageContract], config: RuntimeConfig
) -> List[PinnedPackage]:
    pending = PackageListing.from_contracts(packages)
    final = PackageListing()

    renderer = DbtProjectYamlRenderer(config, config.cli_vars)

    while pending:
        next_pending = PackageListing()
        # resolve the dependency in question
        for package in pending:
            final.incorporate(package)
            target = final[package].resolved().fetch_metadata(config, renderer)
            next_pending.update_from(target.packages)
        pending = next_pending

    resolved = final.resolved()
    _check_for_duplicate_project_names(resolved, config, renderer)
    return resolved
