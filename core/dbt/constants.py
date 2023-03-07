import os

SECRET_ENV_PREFIX = "DBT_ENV_SECRET_"
DEFAULT_ENV_PLACEHOLDER = "DBT_DEFAULT_PLACEHOLDER"
METADATA_ENV_PREFIX = "DBT_ENV_CUSTOM_ENV_"

def get_max_seed_size():
    mx = os.getenv('DBT_MAXIMUM_SEED_SIZE', '1')
    return int(mx)

DEFAULT_MAXIMUM_SEED_SIZE = 1 * 1024 * 1024
MAXIMUM_SEED_SIZE = get_max_seed_size() * DEFAULT_MAXIMUM_SEED_SIZE
MAXIMUM_SEED_SIZE_NAME = str(get_max_seed_size()) + "MiB"

PIN_PACKAGE_URL = (
    "https://docs.getdbt.com/docs/package-management#section-specifying-package-versions"
)
