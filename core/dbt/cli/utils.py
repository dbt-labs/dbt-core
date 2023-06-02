from dbt.cli.flags import Flags
from dbt.config import RuntimeConfig
from dbt.config.runtime import Profile, Project, load_project, load_profile
from dbt.flags import get_flags


def get_profile(flags: Flags) -> Profile:
    # TODO: Generalize safe access to flags.THREADS:
    # https://github.com/dbt-labs/dbt-core/issues/6259
    threads = get_flags().THREADS
    return load_profile(
        get_flags().PROJECT_DIR, get_flags().VARS, get_flags().PROFILE, get_flags().TARGET, threads
    )


def get_project(flags: Flags, profile: Profile) -> Project:
    return load_project(
        get_flags().PROJECT_DIR,
        get_flags().VERSION_CHECK,
        profile,
        get_flags().VARS,
    )


def get_runtime_config(flags: Flags) -> RuntimeConfig:
    profile = get_profile(flags)
    project = get_project(flags, profile)
    return RuntimeConfig.from_parts(
        args=flags,
        profile=profile,
        project=project,
    )
