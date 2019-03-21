import os
from dbt.include.postgres import PACKAGE_PATH as POSTGRES_PACKAGE_PATH
PACKAGE_PATH = os.path.dirname(__file__)
