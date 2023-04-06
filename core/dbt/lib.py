import os
from dbt.config.project import Project
from dataclasses import dataclass
from dbt.cli.resolvers import default_profiles_dir
from dbt.config.runtime import load_profile, load_project
from dbt.flags import set_from_args


@dataclass
class RuntimeArgs:
    project_dir: str
    profiles_dir: str
    single_threaded: bool
    profile: str
    target: str


def get_dbt_config(project_dir, args=None, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig
    import dbt.adapters.factory
    import dbt.events.functions

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = default_profiles_dir()

    profile_name = getattr(args, "profile", None)

    runtime_args = RuntimeArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        single_threaded=single_threaded,
        profile=profile_name,
        target=getattr(args, "target", None),
    )

    # set global flags from arguments
    set_from_args(runtime_args, None)
    profile = load_profile(project_dir, {}, profile_name)
    project = load_project(project_dir, False, profile, {})
    assert type(project) is Project

    config = RuntimeConfig.from_parts(project, profile, runtime_args)

    # the only thing this set_from_args does differently than
    # the one above is that it pass runtime config over, I don't think
    # we need that. but leaving this for now for future reference

    # flags.set_from_args(runtime_args, config)

    # This is idempotent, so we can call it repeatedly
    dbt.adapters.factory.register_adapter(config)

    # Make sure we have a valid invocation_id
    dbt.events.functions.set_invocation_id()
    dbt.events.functions.reset_metadata_vars()

    return config


def parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def deserialize_manifest(manifest_msgpack):
    from dbt.contracts.graph.manifest import Manifest

    return Manifest.from_msgpack(manifest_msgpack)


def serialize_manifest(manifest):
    return manifest.to_msgpack()
